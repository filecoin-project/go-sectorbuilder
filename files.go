package sectorbuilder

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
)

func (sb *SectorBuilder) FinalizeSector(ctx context.Context, sectorNum abi.SectorNumber) error {
	paths, done , err := sb.sectors.AcquireSector(sectorNum, FTCache, 0, false)
	if err != nil {
		return xerrors.Errorf("acquiring sector cache path: %w", err)
	}
	defer done()

	return ffi.ClearCache(paths.Cache)
}

func (sb *SectorBuilder) CanCommit(sectorNum abi.SectorNumber) (bool, error) {
	/*dir, err := sb.SectorPath(fs.DataCache, sectorNum)
	if err != nil {
		return false, xerrors.Errorf("getting cache dir: %w", err)
	}

	ents, err := ioutil.ReadDir(string(dir))
	if err != nil {
		return false, err
	}

	// TODO: slightly more sophisticated check
	return len(ents) == 10, nil
	*/

	log.Warnf("stub CanCommit")
	 return true, nil
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
