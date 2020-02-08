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
	AcquireSectorId() (abi.SectorNumber, error)
	Scrub(SortedPublicSectorInfo) []*Fault

	GenerateEPostCandidates(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []abi.SectorNumber) ([]EPostCandidate, error)
	GenerateFallbackPoSt(SortedPublicSectorInfo, [CommLen]byte, []abi.SectorNumber) ([]EPostCandidate, []byte, error)
	ComputeElectionPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed []byte, winners []EPostCandidate) ([]byte, error)

	SealPreCommit(context.Context, abi.SectorNumber, SealTicket, []PublicPieceInfo) (RawSealPreCommitOutput, error)
	SealCommit(context.Context, abi.SectorNumber, SealTicket, SealSeed, []PublicPieceInfo, RawSealPreCommitOutput) ([]byte, error)
	// FinalizeSector cleans up cache, and moves it to storage filesystem
	FinalizeSector(context.Context, abi.SectorNumber) error
	DropStaged(context.Context, abi.SectorNumber) error

	ReadPieceFromSealedSector(ctx context.Context, sectorID abi.SectorNumber, offset uint64, size uint64, ticket []byte, commD []byte) (io.ReadCloser, error)

	SectorPath(typ fs.DataType, sectorID abi.SectorNumber) (fs.SectorPath, error)
	AllocSectorPath(typ fs.DataType, sectorID abi.SectorNumber, cache bool) (fs.SectorPath, error)
	ReleaseSector(fs.DataType, fs.SectorPath)
	CanCommit(sectorID abi.SectorNumber) (bool, error)
	WorkerStats() WorkerStats
	AddWorker(context.Context, WorkerCfg) (<-chan WorkerTask, error)
	TaskDone(context.Context, uint64, SealRes) error
}

type Verifier interface {
	VerifyElectionPost(ctx context.Context, sectorSize abi.SectorSize, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address) (bool, error)
	VerifyFallbackPost(ctx context.Context, sectorSize abi.SectorSize, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address, faults uint64) (bool, error)
	VerifySeal(sectorSize abi.SectorSize, commR, commD []byte, proverID address.Address, ticket []byte, seed []byte, sectorID abi.SectorNumber, proof []byte) (bool, error)
}
