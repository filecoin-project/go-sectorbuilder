package fs

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("sectorbuilder")

var ErrNotFound = errors.New("sector not found")
var ErrExists = errors.New("sector already exists")
var ErrNoSuitablePath = errors.New("no suitable path for sector found")

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

func (p SectorPath) num() (abi.SectorNumber, error) {
	b := filepath.Base(string(p))
	i := strings.LastIndexByte(b, '-')
	if i < 0 {
		return 0, xerrors.Errorf("malformed sector file name: '%s', expected to be in form 's-[miner]-[num]'", b)
	}
	num, err := strconv.ParseUint(b[i+1:], 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("parsing sector num (name: '%s'): %w", b, err)
	}

	return abi.SectorNumber(num), nil
}

func (p SectorPath) miner() (address.Address, error) {
	b := filepath.Base(string(p))
	fi := strings.IndexByte(b, '-')
	li := strings.LastIndexByte(b, '-')
	if li < 0 || fi < 0 {
		return address.Undef, xerrors.Errorf("malformed sector file name: '%s', expected to be in form 's-[miner]-[num]'", b)
	}

	return address.NewFromString(b[fi+1 : li])
}

func SectorName(miner address.Address, sectorNum abi.SectorNumber) string {
	return fmt.Sprintf("s-%s-%d", miner, uint64(sectorNum))
}

func (p StoragePath) Sector(typ DataType, miner address.Address, num abi.SectorNumber) SectorPath {
	return SectorPath(filepath.Join(string(p), string(typ), SectorName(miner, num)))
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

func (f *FS) FindSector(typ DataType, miner address.Address, num abi.SectorNumber) (out SectorPath, err error) {
	// TODO: consider keeping some sort of index at some point

	for path := range f.paths {
		p := path.Sector(typ, miner, num)

		_, err := os.Stat(string(p))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.Errorf("error scanning path %s for sector %s (%s): %+v", p, SectorName(miner, num), string(typ))
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

func (f *FS) findBestPath(qtyBytesNeeded uint64, cache bool, strict bool) (StoragePath, error) {
	var best StoragePath
	bestw := big.NewInt(0)

	// If we need cache, only return cache. If we need storage, prefer storage, fall back to cache
	bestc := true

	for path, info := range f.paths {
		if info.cache != cache && (bestc != info.cache || strict) {
			continue
		}

		avail, _, err := f.availableBytes(path)
		if err != nil {
			log.Errorf("%+v", err)
			continue
		}

		if uint64(avail) < qtyBytesNeeded {
			continue
		}

		w := big.NewInt(avail)
		w.Mul(w, big.NewInt(int64(info.weight)))
		if w.Cmp(bestw) > 0 {
			if info.cache == cache {
				bestw = w
			}

			best = path
			bestc = info.cache
		}
	}

	if best == "" {
		if cache {
			return best, xerrors.Errorf("no available cache: %w", ErrNoSuitablePath)
		}

		return best, xerrors.Errorf("no available storage: %w", ErrNoSuitablePath)
	}

	return best, nil
}

func (f *FS) ForceAllocSector(typ DataType, miner address.Address, ssize abi.SectorSize, cache bool, num abi.SectorNumber) (SectorPath, error) {
	for {
		spath, err := f.FindSector(typ, miner, num)
		if err == ErrNotFound {
			break
		}
		if err != nil {
			return "", xerrors.Errorf("looking for existing sector data: %w", err)
		}
		log.Warnf("found existing sector data in %s, cleaning up", spath)

		if err := os.RemoveAll(string(spath)); err != nil {
			return "", xerrors.Errorf("cleaning up sector data: %w", err)
		}
	}

	return f.AllocSector(typ, miner, ssize, cache, num)
}

// AllocSector finds the best path for this sector to use
func (f *FS) AllocSector(typ DataType, miner address.Address, ssize abi.SectorSize, cache bool, num abi.SectorNumber) (SectorPath, error) {
	{
		spath, err := f.FindSector(typ, miner, num)
		if err == nil {
			return spath, xerrors.Errorf("allocating sector %s: %m", spath, ErrExists)
		}
		if err != ErrNotFound {
			return "", err
		}
	}

	need := overheadMul[typ] * uint64(ssize)

	p, err := f.findBestPath(need, cache, false)
	if err != nil {
		return "", err
	}

	sp := p.Sector(typ, miner, num)

	return sp, f.reserve(typ, sp.storage(), need)
}

func (f *FS) PrepareCacheMove(sector SectorPath, ssize abi.SectorSize, tocache bool) (SectorPath, error) {
	need := uint64(ssize)

	p, err := f.findBestPath(need, tocache, true)
	if err != nil {
		return "", err
	}

	m, err := sector.miner()
	if err != nil {
		return "", err
	}
	id, err := sector.num()
	if err != nil {
		return "", err
	}

	return p.Sector(sector.typ(), m, id), f.reserve(sector.typ(), p, need)
}

func (f *FS) MoveSector(from, to SectorPath) error {
	if from == to {
		return nil
	}

	inf, err := os.Stat(string(from))
	if err != nil {
		return xerrors.Errorf("stat %s: %w", from, err)
	}

	if inf.IsDir() {
		err = migrateDir(string(from), string(to), false)
	} else {
		err = migrateFile(string(from), string(to), false)
	}
	if err != nil {
		return xerrors.Errorf("migrate sector %s -> %s: %w", from, to, err)
	}

	// TODO: run some quick checks

	if err := os.RemoveAll(string(from)); err != nil {
		return xerrors.Errorf("cleanup %s: %w", from, err)
	}

	return nil
}
