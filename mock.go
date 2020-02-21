package sectorbuilder

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-datastore"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

func TempSectorbuilderDir(paths []fs.PathConfig, sealProofType, postProofType abi.RegisteredProof, ds datastore.Batching) (*SectorBuilder, error) {
	addr, err := address.NewFromString("t0123")
	if err != nil {
		return nil, err
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
