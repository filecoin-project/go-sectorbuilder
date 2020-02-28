package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/go-sectorbuilder"
)

type Basic struct {
	Miner address.Address
	NextID abi.SectorNumber
	Root   string
}

func (b *Basic) AcquireSectorNumber() (abi.SectorNumber, error) {
	b.NextID++
	return b.NextID, nil
}

func (b *Basic) FinalizeSector(abi.SectorNumber) error {
	return nil
}

func (b *Basic) AcquireSector(id abi.SectorNumber, existing sectorbuilder.SectorFileType, allocate sectorbuilder.SectorFileType, sealing bool) (sectorbuilder.SectorPaths, func(), error) {
	mid, err := address.IDFromAddress(b.Miner)
	if err != nil {
		return sectorbuilder.SectorPaths{}, nil, err
	}

	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTUnsealed.String()), 0755)
	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTSealed.String()), 0755)
	os.Mkdir(filepath.Join(b.Root, sectorbuilder.FTCache.String()), 0755)

	return sectorbuilder.SectorPaths{
		Id:       abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: id,
		},
		Unsealed: filepath.Join(b.Root, sectorbuilder.FTUnsealed.String(), fmt.Sprintf("s-%s-%d", b.Miner.String(), id)),
		Sealed:   filepath.Join(b.Root, sectorbuilder.FTSealed.String(), fmt.Sprintf("s-%s-%d", b.Miner.String(), id)),
		Cache:    filepath.Join(b.Root, sectorbuilder.FTCache.String(), fmt.Sprintf("s-%s-%d", b.Miner.String(), id)),
	}, func() {}, nil
}

var _ sectorbuilder.SectorProvider = &Basic{}
