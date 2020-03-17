package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/go-sectorbuilder"
)

type sectorFile struct {
	abi.SectorID
	sectorbuilder.SectorFileType
}

type Basic struct {
	Root  string

	lk sync.Mutex
	waitSector map[sectorFile]chan struct{}
}

func (b *Basic) AcquireSector(ctx context.Context, id abi.SectorID, existing sectorbuilder.SectorFileType, allocate sectorbuilder.SectorFileType, sealing bool) (sectorbuilder.SectorPaths, func(), error) {
	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTUnsealed.String()), 0755)
	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTSealed.String()), 0755)
	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTCache.String()), 0755)

	done := func(){}

	for i := 0; i < 3; i++ {
		if (existing | allocate) & (1 << i) == 0 {
			continue
		}

		b.lk.Lock()
		ch, found := b.waitSector[sectorFile{id, 1 << i}]
		if !found {
			ch = make(chan struct{}, 1)
			b.waitSector[sectorFile{id, 1 << i}] = ch
		}
		b.lk.Unlock()

		select {
		case ch <- struct{}{}:
		case <-ctx.Done():
			done()
			return sectorbuilder.SectorPaths{}, nil, ctx.Err()
		}

		prevDone := done
		done = func() {
			prevDone()
			<-ch
		}
	}

	return sectorbuilder.SectorPaths{
		Id: id,
		Unsealed: filepath.Join(b.Root, sectorbuilder.FTUnsealed.String(), fmt.Sprintf("s-t0%d-%d", id.Miner, id)),
		Sealed:   filepath.Join(b.Root, sectorbuilder.FTSealed.String(), fmt.Sprintf("s-t0%d-%d", id.Miner, id)),
		Cache:    filepath.Join(b.Root, sectorbuilder.FTCache.String(), fmt.Sprintf("s-t0%d-%d", id.Miner, id)),
	}, done, nil
}

var _ sectorbuilder.SectorProvider = &Basic{}
