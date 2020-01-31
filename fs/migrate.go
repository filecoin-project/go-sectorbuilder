package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"

	dcopy "github.com/otiai10/copy"
	"golang.org/x/xerrors"
)

func (f *FS) MigrateTo(to *FS, ssize uint64, symlink bool) error {
	for path := range f.paths {
		for _, dataType := range types {
			sectors, err := f.List(path, dataType)
			if err != nil {
				return err
			}

			for _, sector := range sectors {
				if err := f.migrateSector(to, ssize, sector, symlink); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f *FS) migrateSector(to *FS, ssize uint64, sector SectorPath, symlink bool) error {
	id, err := sector.id()
	if err != nil {
		return err
	}

	m, err := sector.miner()
	if err != nil {
		return err
	}

	newp, err := to.AllocSector(sector.typ(), m, ssize, false, id)
	if err != nil {
		return err
	}

	inf, err := os.Stat(string(sector))
	if err != nil {
		return err
	}

	if inf.IsDir() {
		return migrateDir(string(sector), string(newp), symlink)
	}
	return migrateFile(string(sector), string(newp), symlink)
}

func migrateDir(from, to string, symlink bool) error {
	tost, err := os.Stat(to)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err := os.MkdirAll(to, 0755); err != nil {
			return err
		}
	} else if !tost.IsDir() {
		return xerrors.Errorf("target %q already exists and is a file (expected directory)")
	}

	dirents, err := ioutil.ReadDir(from)
	if err != nil {
		return err
	}

	for _, inf := range dirents {
		n := inf.Name()
		if inf.IsDir() {
			if err := migrateDir(filepath.Join(from, n), filepath.Join(to, n), symlink); err != nil {
				return err
			}
		} else {
			if err := migrateFile(filepath.Join(from, n), filepath.Join(to, n), symlink); err != nil {
				return err
			}
		}
	}

	return nil
}

func migrateFile(from, to string, symlink bool) error {
	if symlink {
		return os.Symlink(from, to)
	}

	log.Infof("copy %s -> %s", from, to)

	return dcopy.Copy(from, to)
}
