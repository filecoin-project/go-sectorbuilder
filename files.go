package sectorbuilder

import (
	"io"
	"os"
	"sync"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"golang.org/x/xerrors"
)

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
