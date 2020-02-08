//+build cgo

package sectorbuilder

import (
	"context"
	"io"
	"os"
	"sync/atomic"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	ffi "github.com/filecoin-project/filecoin-ffi"
	fs "github.com/filecoin-project/go-sectorbuilder/fs"
)

var _ Interface = &SectorBuilder{}

func (sb *SectorBuilder) AddPiece(ctx context.Context, pieceSize abi.UnpaddedPieceSize, sectorId abi.SectorNumber, file io.Reader, existingPieceSizes []abi.UnpaddedPieceSize) (PublicPieceInfo, error) {
	atomic.AddInt32(&sb.addPieceWait, 1)
	ret := sb.RateLimit()
	atomic.AddInt32(&sb.addPieceWait, -1)
	defer ret()

	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return PublicPieceInfo{}, err
	}

	var stagedFile *os.File
	var stagedPath fs.SectorPath
	if len(existingPieceSizes) == 0 {
		stagedPath, err = sb.AllocSectorPath(fs.DataStaging, sectorId, true)
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("allocating sector: %w", err)
		}

		stagedFile, err = os.Create(string(stagedPath))
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}

		defer sb.filesystem.Release(stagedPath, sb.ssize)
	} else {
		stagedPath, err = sb.SectorPath(fs.DataStaging, sectorId)
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("getting sector path: %w", err)
		}

		stagedFile, err = os.OpenFile(string(stagedPath), os.O_RDWR, 0644)
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	}

	if err := sb.filesystem.Lock(ctx, stagedPath); err != nil {
		return PublicPieceInfo{}, err
	}
	defer sb.filesystem.Unlock(stagedPath)

	sizes := make([]uint64, len(existingPieceSizes))
	for i, size := range existingPieceSizes {
		sizes[i] = uint64(size)
	}

	_, _, commP, err := ffi.WriteWithAlignment(f, uint64(pieceSize), stagedFile, sizes)
	if err != nil {
		return PublicPieceInfo{}, err
	}

	if err := stagedFile.Close(); err != nil {
		return PublicPieceInfo{}, err
	}

	if err := f.Close(); err != nil {
		return PublicPieceInfo{}, err
	}

	return PublicPieceInfo{
		Size:  uint64(pieceSize),
		CommP: commP,
	}, werr()
}

func (sb *SectorBuilder) ReadPieceFromSealedSector(ctx context.Context, sectorID abi.SectorNumber, offset uint64, size uint64, ticket []byte, commD []byte) (io.ReadCloser, error) {
	sfs := sb.filesystem

	// TODO: this needs to get smarter when we start supporting partial unseals
	unsealedPath, err := sfs.AllocSector(fs.DataUnsealed, sb.Miner, sb.ssize, true, sectorID)
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

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorID)
	if err != nil {
		return nil, err
	}

	sealedPath, err := sb.SectorPath(fs.DataSealed, sectorID)
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

		var commd [CommLen]byte
		copy(commd[:], commD)

		var tkt [CommLen]byte
		copy(tkt[:], ticket)

		err = ffi.Unseal(uint64(sb.ssize),
			PoRepProofPartitions,
			string(cacheDir),
			string(sealedPath),
			string(unsealedPath),
			uint64(sectorID),
			addressToProverID(sb.Miner),
			tkt,
			commd)
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

func (sb *SectorBuilder) SealPreCommit(ctx context.Context, sectorID abi.SectorNumber, ticket SealTicket, pieces []PublicPieceInfo) (RawSealPreCommitOutput, error) {
	sfs := sb.filesystem

	cacheDir, err := sfs.ForceAllocSector(fs.DataCache, sb.Miner, sb.ssize, true, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting cache dir: %w", err)
	}

	sealedPath, err := sfs.ForceAllocSector(fs.DataSealed, sb.Miner, sb.ssize, true, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting sealed sector paths: %w", err)
	}

	defer sfs.Release(cacheDir, sb.ssize)
	defer sfs.Release(sealedPath, sb.ssize)

	if err := sfs.Lock(ctx, cacheDir); err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("lock cache: %w", err)
	}
	if err := sfs.Lock(ctx, sealedPath); err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("lock sealed: %w", err)
	}
	defer sfs.Unlock(cacheDir)
	defer sfs.Unlock(sealedPath)

	call := workerCall{
		task: WorkerTask{
			Type:       WorkerPreCommit,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
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
		return RawSealPreCommitOutput{}, ctx.Err()
	}

	atomic.AddInt32(&sb.preCommitWait, -1)

	// local

	defer func() {
		<-sb.rateLimit
	}()

	e, err := os.OpenFile(string(sealedPath), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("ensuring sealed file exists: %w", err)
	}
	if err := e.Close(); err != nil {
		return RawSealPreCommitOutput{}, err
	}

	if err := os.Mkdir(string(cacheDir), 0755); err != nil {
		return RawSealPreCommitOutput{}, err
	}

	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += abi.UnpaddedPieceSize(piece.Size)
	}
	ussize := abi.PaddedPieceSize(sb.ssize).Unpadded()
	if sum != ussize {
		return RawSealPreCommitOutput{}, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	stagedPath, err := sb.SectorPath(fs.DataStaging, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("get staged: %w", err)
	}
	// TODO: context cancellation respect
	rspco, err := ffi.SealPreCommit(
		uint64(sb.ssize),
		PoRepProofPartitions,
		string(cacheDir),
		string(stagedPath),
		string(sealedPath),
		uint64(sectorID),
		addressToProverID(sb.Miner),
		ticket.TicketBytes,
		pieces,
	)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("presealing sector %d (%s): %w", sectorID, stagedPath, err)
	}

	return RawSealPreCommitOutput(rspco), nil
}

func (sb *SectorBuilder) sealCommitLocal(ctx context.Context, sectorID abi.SectorNumber, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	defer func() {
		<-sb.rateLimit
	}()

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorID)
	if err != nil {
		return nil, err
	}
	if err := sb.filesystem.Lock(ctx, cacheDir); err != nil {
		return nil, err
	}
	defer sb.filesystem.Unlock(cacheDir)

	proof, err = ffi.SealCommit(
		uint64(sb.ssize),
		PoRepProofPartitions,
		string(cacheDir),
		uint64(sectorID),
		addressToProverID(sb.Miner),
		ticket.TicketBytes,
		seed.TicketBytes,
		pieces,
		ffi.RawSealPreCommitOutput(rspco),
	)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("sid:%d tkt:%v seed:%v, ppi:%v rspco:%v", sectorID, ticket, seed, pieces, rspco)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}

	return proof, nil
}

func (sb *SectorBuilder) SealCommit(ctx context.Context, sectorID abi.SectorNumber, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput) (proof []byte, err error) {
	call := workerCall{
		task: WorkerTask{
			Type:       WorkerCommit,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
			SealTicket: ticket,
			Pieces:     pieces,

			SealSeed: seed,
			Rspco:    rspco,
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
			proof, err = sb.sealCommitLocal(ctx, sectorID, ticket, seed, pieces, rspco)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, xerrors.Errorf("commit: %w", err)
	}

	return proof, nil
}

func (sb *SectorBuilder) ComputeElectionPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed []byte, winners []EPostCandidate) ([]byte, error) {
	if len(challengeSeed) != CommLen {
		return nil, xerrors.Errorf("given challenge seed was the wrong length: %d != %d", len(challengeSeed), CommLen)
	}
	var cseed [CommLen]byte
	copy(cseed[:], challengeSeed)

	privsects, err := sb.pubSectorToPriv(sectorInfo, nil) // TODO: faults
	if err != nil {
		return nil, err
	}

	proverID := addressToProverID(sb.Miner)

	return ffi.GeneratePoSt(uint64(sb.ssize), proverID, privsects, cseed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []abi.SectorNumber) ([]EPostCandidate, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	return ffi.GenerateCandidates(uint64(sb.ssize), proverID, challengeSeed, challengeCount, privsectors)
}

func (sb *SectorBuilder) GenerateFallbackPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []abi.SectorNumber) ([]EPostCandidate, []byte, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, nil, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	candidates, err := ffi.GenerateCandidates(uint64(sb.ssize), proverID, challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, nil, err
	}

	proof, err := ffi.GeneratePoSt(uint64(sb.ssize), proverID, privsectors, challengeSeed, candidates)
	return candidates, proof, err
}
