package sectorbuilder

import (
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-datastore"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

func TempSectorbuilderDir(paths []fs.PathConfig, sectorSize abi.SectorSize, ds datastore.Batching) (*SectorBuilder, error) {
	addr, err := address.NewFromString("t0123")
	if err != nil {
		return nil, err
	}

	var sealProofType abi.RegisteredProof
	var postProofType abi.RegisteredProof

	switch n := uint64(sectorSize); n {
	case 1024:
		sealProofType = abi.RegisteredProof_StackedDRG1KiBSeal
		postProofType = abi.RegisteredProof_StackedDRG1KiBPoSt
	case 1048576:
		sealProofType = abi.RegisteredProof_StackedDRG16MiBSeal
		postProofType = abi.RegisteredProof_StackedDRG16MiBPoSt
	default:
		panic(fmt.Sprintf("mock sector builder does not support sector size: %+v", sectorSize))
	}

	sb, err := New(&Config{
		SealProofType: sealProofType,
		PoStProofType: postProofType,

		Paths: paths,

		WorkerThreads: 2,
		Miner:         addr,
	}, ds)
	if err != nil {
		return nil, err
	}

	return sb, nil
}

func SimplePath(dir string) []fs.PathConfig {
	return []fs.PathConfig{{
		Path:   dir,
		Cache:  true,
		Weight: 1,
	}}
}
