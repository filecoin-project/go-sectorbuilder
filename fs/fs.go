package fs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("sectorbuilder")

var ErrNotFound = errors.New("sector not found")
var ErrExists = errors.New("sector already exists")

type DataType string

const (
	DataCache    DataType = "cache"
	DataStaging  DataType = "staging"
	DataSealed   DataType = "sealed"
	DataUnsealed DataType = "unsealed"
)

var types = []DataType{DataCache, DataStaging, DataSealed, DataUnsealed}

var overheadMul = map[DataType]uint64{ // * sectorSize
	DataCache:    11,
	DataStaging:  1,
	DataSealed:   1,
	DataUnsealed: 1,
}

// StoragePath is a path to storage folder (.lotusstorage)
type StoragePath string

// SectorPath is a path to sector data (.lotusstorage/sealed/s-t0101-42))
type SectorPath string

func (p SectorPath) storage() StoragePath {
	return StoragePath(filepath.Dir(filepath.Dir(string(p))))
}

func (p SectorPath) typ() DataType {
	return DataType(filepath.Base(filepath.Dir(string(p))))
}

func (p SectorPath) id() (uint64, error) {
	b := filepath.Base(string(p))
	i := strings.LastIndexByte(b, '-')
	if i < 0 {
		return 0, xerrors.Errorf("malformed sector file name: '%s', expected to be in form 's-[miner]-[id]'", b)
	}
	id, err := strconv.ParseUint(b[i+1:], 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("parsing sector id (name: '%s'): %w", b, err)
	}

	return id, nil
}

func (p SectorPath) miner() (address.Address, error) {
	b := filepath.Base(string(p))
	fi := strings.IndexByte(b, '-')
	li := strings.LastIndexByte(b, '-')
	if li < 0 || fi < 0 {
		return address.Undef, xerrors.Errorf("malformed sector file name: '%s', expected to be in form 's-[miner]-[id]'", b)
	}

	return address.NewFromString(b[fi+1 : li])
}

func SectorName(miner address.Address, sectorID uint64) string {
	return fmt.Sprintf("s-%s-%d", miner, sectorID)
}

func (p StoragePath) sector(typ DataType, miner address.Address, id uint64) SectorPath {
	return SectorPath(filepath.Join(string(p), string(typ), SectorName(miner, id)))
}

type pathInfo struct {
	cache  bool // TODO: better name?
	weight int
}

type FS struct {
	paths map[StoragePath]*pathInfo

	// in progress actions

	// path -> datatype
	reserved map[StoragePath]map[DataType]uint64

	locks map[SectorPath]chan struct{}

	lk sync.Mutex
}

type PathConfig struct {
	Path string

	Cache  bool
	Weight int
}

func OpenFs(cfg []PathConfig) *FS {
	paths := map[StoragePath]*pathInfo{}
	for _, c := range cfg {
		paths[StoragePath(c.Path)] = &pathInfo{
			cache:  c.Cache,
			weight: c.Weight,
		}
	}
	return &FS{
		paths:    paths,
		reserved: map[StoragePath]map[DataType]uint64{},
		locks:    map[SectorPath]chan struct{}{},
	}
}

func (f *FS) Init() error {
	for path := range f.paths {
		for _, dir := range []string{string(path),
			filepath.Join(string(path), string(DataCache)),
			filepath.Join(string(path), string(DataStaging)),
			filepath.Join(string(path), string(DataSealed)),
			filepath.Join(string(path), string(DataUnsealed))} {
			if err := os.Mkdir(dir, 0755); err != nil {
				if os.IsExist(err) {
					continue
				}
				return err
			}
		}
	}

	return nil
}

// TODO: fix the locking situation

func (f *FS) FindSector(typ DataType, miner address.Address, id uint64) (out SectorPath, err error) {
	// TODO: consider keeping some sort of index at some point

	for path := range f.paths {
		p := path.sector(typ, miner, id)

		_, err := os.Stat(string(p))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.Errorf("error scanning path %s for sector %s (%s): %+v", p, SectorName(miner, id), string(typ))
			continue
		}
		if out != "" {
			if !f.paths[p.storage()].cache {
				log.Errorf("%s also found in cache at %s", p, out)
				return p, nil
			}
		}
		out = p
	}

	if out == "" {
		return "", ErrNotFound
	}

	return out, nil
}

func (f *FS) findBestPath(size uint64, cache bool) StoragePath {
	var best StoragePath
	bestw := big.NewInt(0)
	bestc := !cache

	for path, info := range f.paths {
		if info.cache != cache && bestc != info.cache {
			continue
		}

		avail, _, err := f.availableBytes(path)
		if err != nil {
			log.Errorf("%+v", err)
			continue
		}

		if uint64(avail) < size {
			continue
		}

		w := big.NewInt(avail)
		w.Mul(w, big.NewInt(int64(info.weight)))
		if w.Cmp(bestw) > 0 {
			best = path
			bestw = w
			bestc = info.cache
		}
	}

	return best
}

func (f *FS) ForceAllocSector(typ DataType, miner address.Address, ssize uint64, cache bool, id uint64) (SectorPath, error) {
	for {
		spath, err := f.FindSector(typ, miner, id)
		if err == ErrNotFound {
			break
		}
		if err != nil {
			return "", xerrors.Errorf("looking for existing sector data: %w", err)
		}
		log.Warn("found existing sector data in %s, cleaning up", spath)

		if err := os.RemoveAll(string(spath)); err != nil {
			return "", xerrors.Errorf("cleaning up sector data: %w", err)
		}
	}

	return f.AllocSector(typ, miner, ssize, cache, id)
}

// AllocSector finds the best path for this sector to use
func (f *FS) AllocSector(typ DataType, miner address.Address, ssize uint64, cache bool, id uint64) (SectorPath, error) {
	{
		spath, err := f.FindSector(typ, miner, id)
		if err == nil {
			return spath, xerrors.Errorf("allocating sector %s: %m", spath, ErrExists)
		}
		if err != ErrNotFound {
			return "", err
		}
	}

	need := overheadMul[typ] * ssize

	p := f.findBestPath(need, cache)
	if p == "" {
		return "", xerrors.New("no suitable path for sector fond")
	}

	sp := p.sector(typ, miner, id)

	return sp, f.reserve(typ, sp, need)
}

// reserve reserves storage for the sector. `path` is the path of the directory containing sectors
func (f *FS) reserve(typ DataType, path SectorPath, size uint64) error {
	f.lk.Lock()
	defer f.lk.Unlock()

	avail, fsavail, err := f.availableBytes(path.storage())
	if err != nil {
		return err
	}

	if int64(size) > avail {
		return xerrors.Errorf("not enough space in '%s', need %dB, available %dB (fs: %dB, reserved: %dB)",
			f.paths,
			size,
			avail,
			fsavail,
			f.reservedBytes(path.storage()))
	}

	if _, ok := f.reserved[path.storage()]; !ok {
		f.reserved[path.storage()] = map[DataType]uint64{}
	}
	f.reserved[path.storage()][typ] += size

	return nil
}

func (f *FS) Release(typ DataType, path SectorPath, sectorSize uint64) {
	f.lk.Lock()
	defer f.lk.Unlock()

	f.reserved[path.storage()][typ] -= overheadMul[typ] * sectorSize
}

func (f *FS) List(path StoragePath, typ DataType) ([]SectorPath, error) {
	tp := filepath.Join(string(path), string(typ))

	ents, err := ioutil.ReadDir(tp)
	if err != nil {
		return nil, err
	}

	out := make([]SectorPath, len(ents))
	for i, ent := range ents {
		out[i] = SectorPath(filepath.Join(tp, ent.Name()))
	}

	return out, nil
}

func (f *FS) reservedBytes(path StoragePath) int64 {
	var out int64
	rsvs, ok := f.reserved[path]
	if !ok {
		return 0
	}
	for _, r := range rsvs {
		out += int64(r)
	}
	return out
}

func (f *FS) availableBytes(path StoragePath) (int64, int64, error) {
	var fsstat syscall.Statfs_t

	if err := syscall.Statfs(string(path), &fsstat); err != nil {
		return 0, 0, err
	}

	fsavail := int64(fsstat.Bavail) * int64(fsstat.Bsize)

	avail := fsavail - f.reservedBytes(path)

	return avail, fsavail, nil
}
