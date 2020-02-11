package sectorbuilder

import (
	"context"
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

type Interface interface {
	RateLimit() func()

	AddPiece(context.Context, abi.UnpaddedPieceSize, abi.SectorNumber, io.Reader, []abi.UnpaddedPieceSize) (PublicPieceInfo, error)
	SectorSize() abi.SectorSize
	AcquireSectorNumber() (abi.SectorNumber, error)
	Scrub(SortedPublicSectorInfo) []*Fault

	GenerateEPostCandidates(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []abi.SectorNumber) ([]EPostCandidate, error)
	GenerateFallbackPoSt(SortedPublicSectorInfo, [CommLen]byte, []abi.SectorNumber) ([]EPostCandidate, []byte, error)
	ComputeElectionPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed []byte, winners []EPostCandidate) ([]byte, error)

	SealPreCommit(context.Context, abi.SectorNumber, SealTicket, []PublicPieceInfo) (RawSealPreCommitOutput, error)
	SealCommit(context.Context, abi.SectorNumber, SealTicket, SealSeed, []PublicPieceInfo, RawSealPreCommitOutput) ([]byte, error)
	// FinalizeSector cleans up cache, and moves it to storage filesystem
	FinalizeSector(context.Context, abi.SectorNumber) error
	DropStaged(context.Context, abi.SectorNumber) error

	ReadPieceFromSealedSector(ctx context.Context, sectorNum abi.SectorNumber, offset UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket []byte, commD []byte) (io.ReadCloser, error)

	SectorPath(typ fs.DataType, sectorNum abi.SectorNumber) (fs.SectorPath, error)
	AllocSectorPath(typ fs.DataType, sectorNum abi.SectorNumber, cache bool) (fs.SectorPath, error)
	ReleaseSector(fs.DataType, fs.SectorPath)
	CanCommit(sectorNum abi.SectorNumber) (bool, error)
	WorkerStats() WorkerStats
	AddWorker(context.Context, WorkerCfg) (<-chan WorkerTask, error)
	TaskDone(context.Context, uint64, SealRes) error
}

type UnpaddedByteIndex uint64

type Verifier interface {
	VerifyElectionPost(ctx context.Context, sectorSize abi.SectorSize, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address) (bool, error)
	VerifyFallbackPost(ctx context.Context, sectorSize abi.SectorSize, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address, faults uint64) (bool, error)
	VerifySeal(sectorSize abi.SectorSize, commR, commD []byte, proverID address.Address, ticket []byte, seed []byte, sectorNum abi.SectorNumber, proof []byte) (bool, error)
}
