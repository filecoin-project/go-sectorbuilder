//+build cgo

package sectorbuilder

import (
	"context"
	"io"
	"os"
	"sync/atomic"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

var _ Basic = &SectorBuilder{}

func (sb *SectorBuilder) AddPiece(ctx context.Context, pieceSize abi.UnpaddedPieceSize, sectorNum abi.SectorNumber, file io.Reader, existingPieceSizes []abi.UnpaddedPieceSize) (abi.PieceInfo, error) {
	atomic.AddInt32(&sb.addPieceWait, 1)
	ret := sb.RateLimit()
	atomic.AddInt32(&sb.addPieceWait, -1)
	defer ret()

	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return abi.PieceInfo{}, err
	}

	var stagedFile *os.File
	var stagedPath fs.SectorPath
	if len(existingPieceSizes) == 0 {
		stagedPath, err = sb.AllocSectorPath(fs.DataStaging, sectorNum, true)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("allocating sector: %w", err)
		}

		stagedFile, err = os.Create(string(stagedPath))
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}

		defer sb.filesystem.Release(stagedPath, sb.ssize)
	} else {
		stagedPath, err = sb.SectorPath(fs.DataStaging, sectorNum)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("getting sector path: %w", err)
		}

		stagedFile, err = os.OpenFile(string(stagedPath), os.O_RDWR, 0644)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	}

	if err := sb.filesystem.Lock(ctx, stagedPath); err != nil {
		return abi.PieceInfo{}, err
	}
	defer sb.filesystem.Unlock(stagedPath)

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
	sfs := sb.filesystem

	// TODO: this needs to get smarter when we start supporting partial unseals
	unsealedPath, err := sfs.AllocSector(fs.DataUnsealed, sb.Miner, sb.ssize, true, sectorNum)
	if err != nil {
		if !xerrors.Is(err, fs.ErrExists) {
			return nil, xerrors.Errorf("AllocSector: %w", err)
		}
	}
	defer sfs.Release(unsealedPath, sb.ssize)

	if err := sfs.Lock(ctx, unsealedPath); err != nil {
		return nil, err
	}
	defer sfs.Unlock(unsealedPath)

	atomic.AddInt32(&sb.unsealWait, 1)
	// TODO: Don't wait if cached
	ret := sb.RateLimit() // TODO: check perf, consider remote unseal worker
	defer ret()
	atomic.AddInt32(&sb.unsealWait, -1)

	sb.unsealLk.Lock() // TODO: allow unsealing unrelated sectors in parallel
	defer sb.unsealLk.Unlock()

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorNum)
	if err != nil {
		return nil, err
	}

	sealedPath, err := sb.SectorPath(fs.DataSealed, sectorNum)
	if err != nil {
		return nil, err
	}

	// TODO: GC for those
	//  (Probably configurable count of sectors to be kept unsealed, and just
	//   remove last used one (or use whatever other cache policy makes sense))
	f, err := os.OpenFile(string(unsealedPath), os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		mid, err := address.IDFromAddress(sb.Miner)
		if err != nil {
			return nil, err
		}

		err = ffi.Unseal(
			sb.sealProofType,
			string(cacheDir),
			string(sealedPath),
			string(unsealedPath),
			sectorNum,
			abi.ActorID(mid),
			ticket,
			unsealedCID,
		)
		if err != nil {
			return nil, xerrors.Errorf("unseal failed: %w", err)
		}

		f, err = os.OpenFile(string(unsealedPath), os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
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

func (sb *SectorBuilder) SealPreCommit(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, pieces []abi.PieceInfo) (cid.Cid, cid.Cid, error) {
	sfs := sb.filesystem

	cacheDir, err := sfs.ForceAllocSector(fs.DataCache, sb.Miner, sb.ssize, true, sectorNum)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("getting cache dir: %w", err)
	}

	sealedPath, err := sfs.ForceAllocSector(fs.DataSealed, sb.Miner, sb.ssize, true, sectorNum)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("getting sealed sector paths: %w", err)
	}

	defer sfs.Release(cacheDir, sb.ssize)
	defer sfs.Release(sealedPath, sb.ssize)

	if err := sfs.Lock(ctx, cacheDir); err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("lock cache: %w", err)
	}
	if err := sfs.Lock(ctx, sealedPath); err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("lock sealed: %w", err)
	}
	defer sfs.Unlock(cacheDir)
	defer sfs.Unlock(sealedPath)

	call := workerCall{
		task: WorkerTask{
			Type:       WorkerPreCommit,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorNum:  sectorNum,
			SealTicket: ticket,
			Pieces:     pieces,
		},
		ret: make(chan SealRes),
	}

	atomic.AddInt32(&sb.preCommitWait, 1)

	select { // prefer remote
	case sb.precommitTasks <- call:
		return sb.sealPreCommitRemote(call)
	default:
	}

	sb.checkRateLimit()

	rl := sb.rateLimit
	if sb.noPreCommit {
		rl = make(chan struct{})
	}

	select { // use whichever is available
	case sb.precommitTasks <- call:
		return sb.sealPreCommitRemote(call)
	case rl <- struct{}{}:
	case <-ctx.Done():
		return cid.Undef, cid.Undef, ctx.Err()
	}

	atomic.AddInt32(&sb.preCommitWait, -1)

	// local

	defer func() {
		<-sb.rateLimit
	}()

	e, err := os.OpenFile(string(sealedPath), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("ensuring sealed file exists: %w", err)
	}
	if err := e.Close(); err != nil {
		return cid.Undef, cid.Undef, err
	}

	if err := os.Mkdir(string(cacheDir), 0755); err != nil {
		return cid.Undef, cid.Undef, err
	}

	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ussize := abi.PaddedPieceSize(sb.ssize).Unpadded()
	if sum != ussize {
		return cid.Undef, cid.Undef, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	stagedPath, err := sb.SectorPath(fs.DataStaging, sectorNum)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("get staged: %w", err)
	}

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return cid.Undef, cid.Undef, err
	}

	// TODO: context cancellation respect
	p1o, err := ffi.SealPreCommitPhase1(
		sb.sealProofType,
		string(cacheDir),
		string(stagedPath),
		string(sealedPath),
		sectorNum,
		abi.ActorID(mid),
		ticket,
		pieces,
	)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("presealing sector %d (%s): %w", sectorNum, stagedPath, err)
	}

	sealedCID, unsealedCID, err := ffi.SealPreCommitPhase2(p1o, string(cacheDir), string(sealedPath))
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("presealing sector %d (%s): %w", sectorNum, stagedPath, err)
	}

	return sealedCID, unsealedCID, nil
}

func (sb *SectorBuilder) sealCommitLocal(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, sealedCID cid.Cid, unsealedCID cid.Cid) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	defer func() {
		<-sb.rateLimit
	}()

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorNum)
	if err != nil {
		return nil, err
	}
	if err := sb.filesystem.Lock(ctx, cacheDir); err != nil {
		return nil, err
	}
	defer sb.filesystem.Unlock(cacheDir)

	sealedPath, err := sb.SectorPath(fs.DataSealed, sectorNum)
	if err != nil {
		return nil, xerrors.Errorf("get sealed: %w", err)
	}
	if err := sb.filesystem.Lock(ctx, sealedPath); err != nil {
		return nil, err
	}
	defer sb.filesystem.Unlock(sealedPath)

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	output, err := ffi.SealCommitPhase1(
		sb.sealProofType,
		sealedCID,
		unsealedCID,
		string(cacheDir),
		string(sealedPath),
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

	return ffi.SealCommitPhase2(output, sectorNum, abi.ActorID(mid))
}

func (sb *SectorBuilder) SealCommit(ctx context.Context, sectorNum abi.SectorNumber, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, sealedCID cid.Cid, unsealedCID cid.Cid) (proof []byte, err error) {
	call := workerCall{
		task: WorkerTask{
			Pieces:      pieces,
			SealedCID:   sealedCID,
			SealSeed:    seed,
			SealTicket:  ticket,
			SectorNum:   sectorNum,
			TaskID:      atomic.AddUint64(&sb.taskCtr, 1),
			Type:        WorkerCommit,
			UnsealedCID: unsealedCID,
		},
		ret: make(chan SealRes),
	}

	atomic.AddInt32(&sb.commitWait, 1)

	select { // prefer remote
	case sb.commitTasks <- call:
		proof, err = sb.sealCommitRemote(call)
	default:
		sb.checkRateLimit()

		rl := sb.rateLimit
		if sb.noCommit {
			rl = make(chan struct{})
		}

		select { // use whichever is available
		case sb.commitTasks <- call:
			proof, err = sb.sealCommitRemote(call)
		case rl <- struct{}{}:
			proof, err = sb.sealCommitLocal(ctx, sectorNum, ticket, seed, pieces, sealedCID, unsealedCID)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, xerrors.Errorf("commit: %w", err)
	}

	return proof, nil
}

func (sb *SectorBuilder) ComputeElectionPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, winners []abi.PoStCandidate) ([]abi.PoStProof, error) {
	minerActorID, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	privsects, err := sb.pubSectorToPriv(sectorInfo, nil) // TODO: faults
	if err != nil {
		return nil, err
	}

	return ffi.GeneratePoSt(abi.ActorID(minerActorID), privsects, challengeSeed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]ffi.PoStCandidateWithTicket, error) {
	minerActorID, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, err
	}

	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeSeed[31] = 0

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))

	return ffi.GenerateCandidates(abi.ActorID(minerActorID), challengeSeed, challengeCount, privsectors)
}

func (sb *SectorBuilder) GenerateFallbackPoSt(sectorInfo []abi.SectorInfo, challengeSeed abi.PoStRandomness, faults []abi.SectorNumber) ([]ffi.PoStCandidateWithTicket, []abi.PoStProof, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, nil, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo)), uint64(len(faults)))

	mid, err := address.IDFromAddress(sb.Miner)
	if err != nil {
		return nil, nil, err
	}

	candidates, err := ffi.GenerateCandidates(abi.ActorID(mid), challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, nil, err
	}

	winners := make([]abi.PoStCandidate, len(candidates))
	for idx := range winners {
		winners[idx] = candidates[idx].Candidate
	}

	proof, err := ffi.GeneratePoSt(abi.ActorID(mid), privsectors, challengeSeed, winners)
	return candidates, proof, err
}
