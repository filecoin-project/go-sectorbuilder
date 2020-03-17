package sectorbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/ipfs/go-cid"
)

type SectorFileType int

const (
	FTUnsealed SectorFileType = 1 << iota
	FTSealed
	FTCache
)

func (t SectorFileType) String() string {
	switch t {
	case FTUnsealed:
		return "unsealed"
	case FTSealed:
		return "sealed"
	case FTCache:
		return "cache"
	default:
		return fmt.Sprintf("<unknown %d>", t)
	}
}

type SectorPaths struct {
	Id abi.SectorID

	Unsealed string
	Sealed   string
	Cache    string
}

type Validator interface {
	CanCommit(sector SectorPaths) (bool, error)
	CanProve(sector SectorPaths) (bool, error)
}

type Sealer interface {
	storage.Sealer
	storage.Storage
}

type Basic interface {
	storage.Prover
	Sealer

	ReadPieceFromSealedSector(context.Context, abi.SectorID, UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (io.ReadCloser, error)
}

type UnpaddedByteIndex uint64

type Verifier interface {
	VerifySeal(abi.SealVerifyInfo) (bool, error)
	VerifyElectionPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error)
	VerifyFallbackPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error)
}

// Interfaces consumed by this package

var ErrSectorNotFound = errors.New("sector not found")

type SectorProvider interface {
	// * returns ErrSectorNotFound if a requested existing sector doesn't exist
	// * returns an error when allocate is set, and existing isn't, and the sector exists
	AcquireSector(ctx context.Context, id abi.SectorID, existing SectorFileType, allocate SectorFileType, sealing bool) (SectorPaths, func(), error)
}
