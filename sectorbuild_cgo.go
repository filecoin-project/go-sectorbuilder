//+build cgo

package sectorbuilder

import (
	"context"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"

	ffi "github.com/filecoin-project/filecoin-ffi"
)

var _ Basic = &SectorBuilder{}

func (sb *SectorBuilder) NewSector(ctx context.Context, sector abi.SectorID) error {
	// TODO: Allocate the sector here instead of in addpiece

	return nil
}

func (sb *SectorBuilder) AddPiece(ctx context.Context, sector abi.SectorID, existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize, file storage.Data) (abi.PieceInfo, error) {
	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return abi.PieceInfo{}, err
	}

	var done func()
	var stagedFile *os.File

	defer func() {
		if done != nil {
			done()
		}

		if stagedFile != nil {
			if err := stagedFile.Close(); err != nil {
				log.Errorf("closing staged file: %+v", err)
			}
		}
	}()

	var stagedPath SectorPaths
	if len(existingPieceSizes) == 0 {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, 0, FTUnsealed, true)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = os.Create(stagedPath.Unsealed)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	} else {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, FTUnsealed, 0, true)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = os.OpenFile(stagedPath.Unsealed, os.O_RDWR, 0644)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}

		if _, err := stagedFile.Seek(0, io.SeekEnd); err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("seek end: %w", err)
		}
	}

	_, _, pieceCID, err := ffi.WriteWithAlignment(sb.sealProofType, f, pieceSize, stagedFile, existingPieceSizes)
	if err != nil {
		return abi.PieceInfo{}, err
	}

	if err := f.Close(); err != nil {
		return abi.PieceInfo{}, err
	}

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: pieceCID,
	}, werr()
}

func (sb *SectorBuilder) ReadPieceFromSealedSector(ctx context.Context, sector abi.SectorID, offset UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealedCID cid.Cid) (io.ReadCloser, error) {
	path, doneUnsealed, err := sb.sectors.AcquireSector(ctx, sector, FTUnsealed, FTUnsealed, false)
	if err != nil {
		return nil, xerrors.Errorf("acquire unsealed sector path: %w", err)
	}
	defer doneUnsealed()
	f, err := os.OpenFile(path.Unsealed, os.O_RDONLY, 0644)
	if err == nil {
		if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
			return nil, xerrors.Errorf("seek: %w", err)
		}

		lr := io.LimitReader(f, int64(size))

		return &struct {
			io.Reader
			io.Closer
		}{
			Reader: lr,
			Closer: f,
		}, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	sealed, doneSealed, err := sb.sectors.AcquireSector(ctx, sector, FTUnsealed|FTCache, 0, false)
	if err != nil {
		return nil, xerrors.Errorf("acquire sealed/cache sector path: %w", err)
	}
	defer doneSealed()

	// TODO: GC for those
	//  (Probably configurable count of sectors to be kept unsealed, and just
	//   remove last used one (or use whatever other cache policy makes sense))
	err = ffi.Unseal(
		sb.sealProofType,
		sealed.Cache,
		sealed.Sealed,
		path.Unsealed,
		sector.Number,
		sector.Miner,
		ticket,
		unsealedCID,
	)
	if err != nil {
		return nil, xerrors.Errorf("unseal failed: %w", err)
	}

	f, err = os.OpenFile(string(path.Unsealed), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seek: %w", err)
	}

	lr := io.LimitReader(f, int64(size))

	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: lr,
		Closer: f,
	}, nil
}

func (sb *SectorBuilder) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, FTUnsealed, FTSealed|FTCache, true)
	if err != nil {
		return nil, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer done()

	e, err := os.OpenFile(paths.Sealed, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, xerrors.Errorf("ensuring sealed file exists: %w", err)
	}
	if err := e.Close(); err != nil {
		return nil, err
	}

	if err := os.Mkdir(paths.Cache, 0755); err != nil {
		if os.IsExist(err) {
			log.Warnf("existing cache in %s; removing", paths.Cache)

			if err := os.RemoveAll(paths.Cache); err != nil {
				return nil, xerrors.Errorf("remove existing sector cache from %s (sector %d): %w", paths.Cache, sector, err)
			}

			if err := os.Mkdir(paths.Cache, 0755); err != nil {
				return nil, xerrors.Errorf("mkdir cache path after cleanup: %w", err)
			}
		} else {
			return nil, err
		}
	}

	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ussize := abi.PaddedPieceSize(sb.ssize).Unpadded()
	if sum != ussize {
		return nil, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	// TODO: context cancellation respect
	p1o, err := ffi.SealPreCommitPhase1(
		sb.sealProofType,
		paths.Cache,
		paths.Unsealed,
		paths.Sealed,
		sector.Number,
		sector.Miner,
		ticket,
		pieces,
	)
	if err != nil {
		return nil, xerrors.Errorf("presealing sector %d (%s): %w", sector.Number, paths.Unsealed, err)
	}
	return p1o, nil
}

func (sb *SectorBuilder) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (storage.SectorCids, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, FTSealed|FTCache, 0, true)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer done()

	sealedCID, unsealedCID, err := ffi.SealPreCommitPhase2(phase1Out, paths.Cache, paths.Sealed)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("presealing sector %d (%s): %w", sector.Number, paths.Unsealed, err)
	}

	return storage.SectorCids{
		Unsealed: unsealedCID,
		Sealed:   sealedCID,
	}, nil
}

func (sb *SectorBuilder) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storage.Commit1Out, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, FTSealed|FTCache, 0, true)
	if err != nil {
		return nil, xerrors.Errorf("acquire sector paths: %w", err)
	}
	defer done()
	output, err := ffi.SealCommitPhase1(
		sb.sealProofType,
		cids.Sealed,
		cids.Unsealed,
		paths.Cache,
		paths.Sealed,
		sector.Number,
		sector.Miner,
		ticket,
		seed,
		pieces,
	)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("num:%d tkt:%v seed:%v, pi:%v sealedCID:%v, unsealedCID:%v", sector.Number, ticket, seed, pieces, cids.Sealed, cids.Unsealed)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}
	return output, nil
}

func (sb *SectorBuilder) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (storage.Proof, error) {
	return ffi.SealCommitPhase2(phase1Out, sector.Number, sector.Miner)
}

func (sb *SectorBuilder) ComputeElectionPoSt(ctx context.Context, miner abi.ActorID, sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, winners []abi.PoStCandidate) ([]abi.PoStProof, error) {
	challengeSeed[31] = 0

	privsects, err := sb.pubSectorToPriv(ctx, miner, sectorInfo, nil) // TODO: faults
	if err != nil {
		return nil, err
	}

	return ffi.GeneratePoSt(miner, privsects, challengeSeed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(ctx context.Context, miner abi.ActorID, sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]storage.PoStCandidateWithTicket, error) {
	privsectors, err := sb.pubSectorToPriv(ctx, miner, sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeSeed[31] = 0

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))
	pc, err := ffi.GenerateCandidates(miner, challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, err
	}

	return ffiToStorageCandidates(pc), nil
}

func (sb *SectorBuilder) GenerateFallbackPoSt(ctx context.Context, miner abi.ActorID, sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) (storage.FallbackPostOut, error) {
	privsectors, err := sb.pubSectorToPriv(ctx, miner, sectorInfo, faults)
	if err != nil {
		return storage.FallbackPostOut{}, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))
	challengeSeed[31] = 0

	candidates, err := ffi.GenerateCandidates(miner, challengeSeed, challengeCount, privsectors)
	if err != nil {
		return storage.FallbackPostOut{}, err
	}

	winners := make([]abi.PoStCandidate, len(candidates))
	for idx := range winners {
		winners[idx] = candidates[idx].Candidate
	}

	proof, err := ffi.GeneratePoSt(miner, privsectors, challengeSeed, winners)
	return storage.FallbackPostOut{
		PoStInputs: ffiToStorageCandidates(candidates),
		Proof:      proof,
	}, err
}

func (sb *SectorBuilder) FinalizeSector(ctx context.Context, sector abi.SectorID) error {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, FTCache, 0, false)
	if err != nil {
		return xerrors.Errorf("acquiring sector cache path: %w", err)
	}
	defer done()

	return ffi.ClearCache(paths.Cache)
}

func ffiToStorageCandidates(pc []ffi.PoStCandidateWithTicket) []storage.PoStCandidateWithTicket {
	out := make([]storage.PoStCandidateWithTicket, len(pc))
	for i := range out {
		out[i] = storage.PoStCandidateWithTicket{
			Candidate: pc[i].Candidate,
			Ticket:    pc[i].Ticket,
		}
	}

	return out
}
