//+build cgo

package sectorbuilder

import (
	"context"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"

	ffi "github.com/filecoin-project/filecoin-ffi"
)

var _ Basic = &SectorBuilder{}

func (sb *SectorBuilder) AddPiece(ctx context.Context, sectorNum abi.SectorNumber, existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize, file storage.Data) (abi.PieceInfo, error) {
	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return abi.PieceInfo{}, err
	}

	var done func()
	var stagedFile *os.File
	var stagedPath SectorPaths
	if len(existingPieceSizes) == 0 {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sectorNum, 0, FTUnsealed, true)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = os.Create(stagedPath.Unsealed)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	} else {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sectorNum, FTUnsealed, 0, true)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = os.OpenFile(stagedPath.Unsealed, os.O_RDWR, 0644)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	}
	defer done()

	_, _, pieceCID, err := ffi.WriteWithAlignment(sb.sealProofType, f, pieceSize, stagedFile, existingPieceSizes)
	if err != nil {
		return abi.PieceInfo{}, err
	}

	if err := stagedFile.Close(); err != nil {
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

func (sb *SectorBuilder) ReadPieceFromSealedSector(ctx context.Context, sectorNum abi.SectorNumber, offset UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealedCID cid.Cid) (io.ReadCloser, error) {
	path, doneUnsealed, err := sb.sectors.AcquireSector(ctx, sectorNum, FTUnsealed, FTUnsealed, false)
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

	sealed, doneSealed, err := sb.sectors.AcquireSector(ctx, sectorNum, FTUnsealed|FTCache, 0, false)
	if err != nil {
		return nil, xerrors.Errorf("acquire sealed/cache sector path: %w", err)
	}
	defer doneSealed()

	// TODO: GC for those
	//  (Probably configurable count of sectors to be kept unsealed, and just
	//   remove last used one (or use whatever other cache policy makes sense))
	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	err = ffi.Unseal(
		sb.sealProofType,
		sealed.Cache,
		sealed.Sealed,
		path.Unsealed,
		sectorNum,
		abi.ActorID(mid),
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

func (sb *SectorBuilder) SealPreCommit1(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sectorNum, FTUnsealed, FTSealed|FTCache, true)
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
				return nil, xerrors.Errorf("remove existing sector cache from %s (sector %d): %w", paths.Cache, sectorNum, err)
			}

			if err := os.Mkdir(paths.Cache, 0755); err != nil {
				return nil, xerrors.Errorf("mkdir cache path after cleanup: %w", err)
			}
		}
		return nil, err
	}

	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ussize := abi.PaddedPieceSize(sb.ssize).Unpadded()
	if sum != ussize {
		return nil, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	// TODO: context cancellation respect
	p1o, err := ffi.SealPreCommitPhase1(
		sb.sealProofType,
		paths.Cache,
		paths.Unsealed,
		paths.Sealed,
		sectorNum,
		abi.ActorID(mid),
		ticket,
		pieces,
	)
	if err != nil {
		return nil, xerrors.Errorf("presealing sector %d (%s): %w", sectorNum, paths.Unsealed, err)
	}
	return p1o, nil
}

func (sb *SectorBuilder) SealPreCommit2(ctx context.Context, sectorNum abi.SectorNumber, phase1Out storage.PreCommit1Out) (cid.Cid, cid.Cid, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sectorNum, FTSealed|FTCache, 0, true)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer done()

	sealedCID, unsealedCID, err := ffi.SealPreCommitPhase2(phase1Out, paths.Cache, paths.Sealed)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("presealing sector %d (%s): %w", sectorNum, paths.Unsealed, err)
	}

	return sealedCID, unsealedCID, nil
}

func (sb *SectorBuilder) SealCommit1(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, sealedCID cid.Cid, unsealedCID cid.Cid) (storage.Commit1Out, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sectorNum, FTSealed|FTCache, 0, true)
	if err != nil {
		return nil, xerrors.Errorf("acquire sector paths: %w", err)
	}
	defer done()

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	output, err := ffi.SealCommitPhase1(
		sb.sealProofType,
		sealedCID,
		unsealedCID,
		paths.Cache,
		paths.Sealed,
		sectorNum,
		abi.ActorID(mid),
		ticket,
		seed,
		pieces,
	)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("num:%d tkt:%v seed:%v, pi:%v sealedCID:%v, unsealedCID:%v", sectorNum, ticket, seed, pieces, sealedCID, unsealedCID)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}
	return output, nil
}

func (sb *SectorBuilder) SealCommit2(ctx context.Context, sectorNum abi.SectorNumber, phase1Out storage.Commit1Out) (storage.Proof, error) {
	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	return ffi.SealCommitPhase2(phase1Out, sectorNum, abi.ActorID(mid))
}

func (sb *SectorBuilder) ComputeElectionPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, winners []abi.PoStCandidate) ([]abi.PoStProof, error) {
	minerActorID, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	challengeSeed[31] = 0

	privsects, err := sb.pubSectorToPriv(context.TODO(), sectorInfo, nil) // TODO: faults
	if err != nil {
		return nil, err
	}

	return ffi.GeneratePoSt(abi.ActorID(minerActorID), privsects, challengeSeed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]storage.PoStCandidateWithTicket, error) {
	minerActorID, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	privsectors, err := sb.pubSectorToPriv(context.TODO(), sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeSeed[31] = 0

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))
	pc, err := ffi.GenerateCandidates(abi.ActorID(minerActorID), challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, err
	}

	return ffiToStorageCandidates(pc), nil
}

func (sb *SectorBuilder) GenerateFallbackPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]storage.PoStCandidateWithTicket, []abi.PoStProof, error) {
	privsectors, err := sb.pubSectorToPriv(context.TODO(), sectorInfo, faults)
	if err != nil {
		return nil, nil, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, nil, err
	}

	challengeSeed[31] = 0

	candidates, err := ffi.GenerateCandidates(abi.ActorID(mid), challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, nil, err
	}

	winners := make([]abi.PoStCandidate, len(candidates))
	for idx := range winners {
		winners[idx] = candidates[idx].Candidate
	}

	proof, err := ffi.GeneratePoSt(abi.ActorID(mid), privsectors, challengeSeed, winners)
	return ffiToStorageCandidates(candidates), proof, err
}

func (sb *SectorBuilder) FinalizeSector(ctx context.Context, sectorNum abi.SectorNumber) error {
	paths, done, err := sb.sectors.AcquireSector(ctx, sectorNum, FTCache, 0, false)
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
