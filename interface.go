package sectorbuilder

import (
	"context"
	"io"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

type Interface interface {
	RateLimit() func()

	AddPiece(context.Context, abi.UnpaddedPieceSize, abi.SectorNumber, io.Reader, []abi.UnpaddedPieceSize) (abi.PieceInfo, error)
	SectorSize() abi.SectorSize
	SealProofType() abi.RegisteredProof
	PoStProofType() abi.RegisteredProof
	AcquireSectorNumber() (abi.SectorNumber, error)
	Scrub([]abi.SectorNumber) []*Fault

	GenerateEPostCandidates(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]ffi.PoStCandidateWithTicket, error)
	GenerateFallbackPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]ffi.PoStCandidateWithTicket, []abi.PoStProof, error)
	ComputeElectionPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, winners []abi.PoStCandidate) ([]abi.PoStProof, error)

	SealPreCommit(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, pieces []abi.PieceInfo) (sealedCID cid.Cid, unsealedCID cid.Cid, err error)
	SealCommit(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, sealedCID cid.Cid, unsealedCID cid.Cid) (proof []byte, err error)
	// FinalizeSector cleans up cache, and moves it to storage filesystem
	FinalizeSector(context.Context, abi.SectorNumber) error
	DropStaged(context.Context, abi.SectorNumber) error

	ReadPieceFromSealedSector(context.Context, abi.SectorNumber, UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (io.ReadCloser, error)

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
	VerifySeal(abi.SealVerifyInfo) (bool, error)
	VerifyElectionPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error)
	VerifyFallbackPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error)
}
