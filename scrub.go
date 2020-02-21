package sectorbuilder

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

type Fault struct {
	SectorNum abi.SectorNumber

	Err error
}

func (sb *SectorBuilder) Scrub(sectorSet []abi.SectorNumber) []*Fault {
	var faults []*Fault

	for _, sectorNum := range sectorSet {
		err := sb.checkSector(sectorNum)
		if err != nil {
			faults = append(faults, &Fault{SectorNum: sectorNum, Err: err})
		}
	}

	return faults
}

func (sb *SectorBuilder) checkSector(sectorNum abi.SectorNumber) error {
	scache, err := sb.SectorPath(fs.DataCache, sectorNum)
	if err != nil {
		return xerrors.Errorf("getting sector cache dir: %w", err)
	}
	cache := string(scache)

	if err := assertFile(filepath.Join(cache, "p_aux"), 96, 96); err != nil {
		return err
	}

	ssizeRaw := uint64(sb.ssize)
	if err := assertFile(filepath.Join(cache, "sc-01-data-tree-r-last.dat"), (2*ssizeRaw)-32, (2*ssizeRaw)-32); err != nil {
		return err
	}

	// TODO: better validate this
	if err := assertFile(filepath.Join(cache, "t_aux"), 100, 32000); err != nil { // TODO: what should this actually be?
		return err
	}

	dent, err := ioutil.ReadDir(cache)
	if err != nil {
		return xerrors.Errorf("reading cache dir %s", cache)
	}
	if len(dent) != 3 {
		return xerrors.Errorf("found %d files in %s, expected 3", len(dent), cache)
	}

	sealed, err := sb.SectorPath(fs.DataSealed, sectorNum)
	if err != nil {
		return xerrors.Errorf("getting sealed sector paths: %w", err)
	}

	if err := assertFile(filepath.Join(string(sealed)), ssizeRaw, ssizeRaw); err != nil {
		return err
	}

	return nil
}

func assertFile(path string, minSz uint64, maxSz uint64) error {
	st, err := os.Stat(path)
	if err != nil {
		return xerrors.Errorf("stat %s: %w", path, err)
	}

	if st.IsDir() {
		return xerrors.Errorf("expected %s to be a regular file", path)
	}

	if uint64(st.Size()) < minSz || uint64(st.Size()) > maxSz {
		return xerrors.Errorf("%s wasn't within size bounds, expected %d < f < %d, got %d", minSz, maxSz, st.Size())
	}

	return nil
}
