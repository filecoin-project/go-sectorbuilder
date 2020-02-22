package sectorbuilder

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

func (sb *SectorBuilder) SectorName(sectorNum abi.SectorNumber) string {
	return fs.SectorName(sb.Miner, sectorNum)
}

func (sb *SectorBuilder) SectorPath(typ fs.DataType, sectorNum abi.SectorNumber) (fs.SectorPath, error) {
	return sb.filesystem.FindSector(typ, sb.Miner, sectorNum)
}

func (sb *SectorBuilder) AllocSectorPath(typ fs.DataType, sectorNum abi.SectorNumber, cache bool) (fs.SectorPath, error) {
	return sb.filesystem.AllocSector(typ, sb.Miner, sb.ssize, cache, sectorNum)
}

func (sb *SectorBuilder) ReleaseSector(typ fs.DataType, path fs.SectorPath) {
	sb.filesystem.Release(path, sb.ssize)
}

func (sb *SectorBuilder) TrimCache(ctx context.Context, sectorNum abi.SectorNumber) error {
	dir, err := sb.filesystem.FindSector(fs.DataCache, sb.Miner, sectorNum)
	if err != nil {
		return xerrors.Errorf("getting cache dir: %w", err)
	}
	if err := sb.filesystem.Lock(ctx, dir); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}
	defer sb.filesystem.Unlock(dir)

	files, err := ioutil.ReadDir(string(dir))
	if err != nil {
		return xerrors.Errorf("readdir: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".dat") { // _aux probably
			continue
		}
		if strings.HasSuffix(file.Name(), "-data-tree-r-last.dat") { // Want to keep
			continue
		}
		if strings.HasSuffix(file.Name(), "-data-tree-d.dat") { // Want to keep
			continue
		}

		if err := os.Remove(filepath.Join(string(dir), file.Name())); err != nil {
			return xerrors.Errorf("rm %s: %w", file.Name(), err)
		}
	}

	return nil
}

func (sb *SectorBuilder) CanCommit(sectorNum abi.SectorNumber) (bool, error) {
	dir, err := sb.SectorPath(fs.DataCache, sectorNum)
	if err != nil {
		return false, xerrors.Errorf("getting cache dir: %w", err)
	}

	ents, err := ioutil.ReadDir(string(dir))
	if err != nil {
		return false, err
	}

	// TODO: slightly more sophisticated check
	return len(ents) == 10, nil
}

func toReadableFile(r io.Reader, n int64) (*os.File, func() error, error) {
	f, ok := r.(*os.File)
	if ok {
		return f, func() error { return nil }, nil
	}

	var w *os.File

	f, w, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}

	var wait sync.Mutex
	var werr error

	wait.Lock()
	go func() {
		defer wait.Unlock()

		var copied int64
		copied, werr = io.CopyN(w, r, n)
		if werr != nil {
			log.Warnf("toReadableFile: copy error: %+v", werr)
		}

		err := w.Close()
		if werr == nil && err != nil {
			werr = err
			log.Warnf("toReadableFile: close error: %+v", err)
			return
		}
		if copied != n {
			log.Warnf("copied different amount than expected: %d != %d", copied, n)
			werr = xerrors.Errorf("copied different amount than expected: %d != %d", copied, n)
		}
	}()

	return f, func() error {
		wait.Lock()
		return werr
	}, nil
}
