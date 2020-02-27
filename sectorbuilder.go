package sectorbuilder

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

const PoStReservedWorkers = 1

var lastSectorNumKey = datastore.NewKey("/last")

var log = logging.Logger("sectorbuilder")

func NewJsonEncodablePreCommitOutput(sealedCID cid.Cid, unsealedCID cid.Cid) (JsonEncodablePreCommitOutput, error) {
	return JsonEncodablePreCommitOutput{
		CommD: unsealedCID,
		CommR: sealedCID,
	}, nil
}

func (r *JsonEncodablePreCommitOutput) ToTuple() (sealedCID cid.Cid, unsealedCID cid.Cid) {
	return r.CommR, r.CommD
}

type Config struct {
	SealProofType abi.RegisteredProof
	PoStProofType abi.RegisteredProof
	Miner         address.Address

	WorkerThreads   uint8
	FallbackLastNum abi.SectorNumber
	NoCommit        bool
	NoPreCommit     bool

	Paths []fs.PathConfig
	_     struct{} // guard against nameless init
}

func New(cfg *Config, ds datastore.Batching) (*SectorBuilder, error) {
	if cfg.WorkerThreads < PoStReservedWorkers {
		return nil, xerrors.Errorf("minimum worker threads is %d, specified %d", PoStReservedWorkers, cfg.WorkerThreads)
	}

	sectorSize, err := sizeFromConfig(*cfg)
	if err != nil {
		return nil, err
	}

	var lastUsedNum abi.SectorNumber
	b, err := ds.Get(lastSectorNumKey)
	switch err {
	case nil:
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return nil, err
		}
		lastUsedNum = abi.SectorNumber(i)
	case datastore.ErrNotFound:
		lastUsedNum = cfg.FallbackLastNum
	default:
		return nil, err
	}

	rlimit := cfg.WorkerThreads - PoStReservedWorkers

	sealLocal := rlimit > 0

	if rlimit == 0 {
		rlimit = 1
	}

	sb := &SectorBuilder{
		ds: ds,

		lastNum: lastUsedNum,

		sealProofType: cfg.SealProofType,
		postProofType: cfg.PoStProofType,
		ssize:         sectorSize,

		filesystem: fs.OpenFs(cfg.Paths),

		Miner: cfg.Miner,

		noPreCommit: cfg.NoPreCommit || !sealLocal,
		noCommit:    cfg.NoCommit || !sealLocal,
		rateLimit:   make(chan struct{}, rlimit),

		taskCtr:        1,
		precommitTasks: make(chan workerCall),
		commitTasks:    make(chan workerCall),
		remoteResults:  map[uint64]chan<- SealRes{},
		remotes:        map[int]*remote{},

		stopping: make(chan struct{}),
	}

	if err := sb.filesystem.Init(); err != nil {
		return nil, xerrors.Errorf("initializing sectorbuilder filesystem: %w", err)
	}

	return sb, nil
}

func NewStandalone(cfg *Config) (*SectorBuilder, error) {
	sectorSize, err := sizeFromConfig(*cfg)
	if err != nil {
		return nil, err
	}

	sb := &SectorBuilder{
		ds: nil,

		sealProofType: cfg.SealProofType,
		postProofType: cfg.PoStProofType,
		ssize:         sectorSize,

		Miner:      cfg.Miner,
		filesystem: fs.OpenFs(cfg.Paths),

		taskCtr:   1,
		remotes:   map[int]*remote{},
		rateLimit: make(chan struct{}, cfg.WorkerThreads),
		stopping:  make(chan struct{}),
	}

	if err := sb.filesystem.Init(); err != nil {
		return nil, xerrors.Errorf("initializing sectorbuilder filesystem: %w", err)
	}

	return sb, nil
}

func (sb *SectorBuilder) checkRateLimit() {
	if cap(sb.rateLimit) == len(sb.rateLimit) {
		log.Warn("rate-limiting local sectorbuilder call")
	}
}

func (sb *SectorBuilder) RateLimit() func() {
	sb.checkRateLimit()

	sb.rateLimit <- struct{}{}

	return func() {
		<-sb.rateLimit
	}
}

type WorkerStats struct {
	LocalFree     int
	LocalReserved int
	LocalTotal    int
	// todo: post in progress
	RemotesTotal int
	RemotesFree  int

	AddPieceWait  int
	PreCommitWait int
	CommitWait    int
	UnsealWait    int
}

func (sb *SectorBuilder) WorkerStats() WorkerStats {
	sb.remoteLk.Lock()
	defer sb.remoteLk.Unlock()

	remoteFree := len(sb.remotes)
	for _, r := range sb.remotes {
		if r.busy > 0 {
			remoteFree--
		}
	}

	return WorkerStats{
		LocalFree:     cap(sb.rateLimit) - len(sb.rateLimit),
		LocalReserved: PoStReservedWorkers,
		LocalTotal:    cap(sb.rateLimit) + PoStReservedWorkers,
		RemotesTotal:  len(sb.remotes),
		RemotesFree:   remoteFree,

		AddPieceWait:  int(atomic.LoadInt32(&sb.addPieceWait)),
		PreCommitWait: int(atomic.LoadInt32(&sb.preCommitWait)),
		CommitWait:    int(atomic.LoadInt32(&sb.commitWait)),
		UnsealWait:    int(atomic.LoadInt32(&sb.unsealWait)),
	}
}

func addressToProverID(a address.Address) [32]byte {
	var proverId [32]byte
	copy(proverId[:], a.Payload())
	return proverId
}

func (sb *SectorBuilder) AcquireSectorNumber() (abi.SectorNumber, error) {
	sb.numLk.Lock()
	defer sb.numLk.Unlock()

	sb.lastNum++
	num := sb.lastNum

	err := sb.ds.Put(lastSectorNumKey, []byte(fmt.Sprint(num)))
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (sb *SectorBuilder) sealPreCommitRemote(call workerCall) (sealedCID cid.Cid, unsealedCID cid.Cid, err error) {
	atomic.AddInt32(&sb.preCommitWait, -1)

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}

		return ret.Rspco.CommR, ret.Rspco.CommD, err
	case <-sb.stopping:
		return cid.Undef, cid.Undef, xerrors.New("sectorbuilder stopped")
	}
}

func (sb *SectorBuilder) sealCommitRemote(call workerCall) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	select {
	case ret := <-call.ret:
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.Proof, err
	case <-sb.stopping:
		return nil, xerrors.New("sectorbuilder stopped")
	}
}

func (sb *SectorBuilder) pubSectorToPriv(sectorInfo []abi.SectorInfo, faults []abi.SectorNumber) (ffi.SortedPrivateSectorInfo, error) {
	fmap := map[abi.SectorNumber]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var out []ffi.PrivateSectorInfo
	for _, s := range sectorInfo {
		if _, faulty := fmap[s.SectorNumber]; faulty {
			continue
		}

		cachePath, err := sb.SectorPath(fs.DataCache, s.SectorNumber) // TODO: LOCK!
		if err != nil {
			return ffi.SortedPrivateSectorInfo{}, xerrors.Errorf("getting cache paths for sector %d: %w", s.SectorNumber, err)
		}

		sealedPath, err := sb.SectorPath(fs.DataSealed, s.SectorNumber)
		if err != nil {
			return ffi.SortedPrivateSectorInfo{}, xerrors.Errorf("getting sealed paths for sector %d: %w", s.SectorNumber, err)
		}

		postProofType, err := s.RegisteredProof.RegisteredPoStProof()
		if err != nil {
			return ffi.SortedPrivateSectorInfo{}, xerrors.Errorf("acquiring registered PoSt proof from sector info %+v: %w", s, err)
		}

		out = append(out, ffi.PrivateSectorInfo{
			CacheDirPath:     string(cachePath),
			PoStProofType:    postProofType,
			SealedSectorPath: string(sealedPath),
			SectorInfo:       s,
		})
	}

	return ffi.NewSortedPrivateSectorInfo(out...), nil
}

func fallbackPostChallengeCount(sectors uint64, faults uint64) uint64 {
	challengeCount := ElectionPostChallengeCount(sectors, faults)
	if challengeCount > MaxFallbackPostChallengeCount {
		return MaxFallbackPostChallengeCount
	}
	return challengeCount
}

func (sb *SectorBuilder) FinalizeSector(ctx context.Context, num abi.SectorNumber) error {
	sealed, err := sb.filesystem.FindSector(fs.DataSealed, sb.Miner, num)
	if err != nil {
		return xerrors.Errorf("getting sealed sector: %w", err)
	}
	cache, err := sb.filesystem.FindSector(fs.DataCache, sb.Miner, num)
	if err != nil {
		return xerrors.Errorf("getting sector cache: %w", err)
	}

	// todo: flag to just remove
	staged, err := sb.filesystem.FindSector(fs.DataStaging, sb.Miner, num)
	if err != nil {
		return xerrors.Errorf("getting staged sector: %w", err)
	}

	{
		if err := sb.filesystem.Lock(ctx, sealed); err != nil {
			return err
		}
		defer sb.filesystem.Unlock(sealed)

		if err := sb.filesystem.Lock(ctx, cache); err != nil {
			return err
		}
		defer sb.filesystem.Unlock(cache)

		if err := sb.filesystem.Lock(ctx, staged); err != nil {
			return err
		}
		defer sb.filesystem.Unlock(staged)
	}

	sealedDest, err := sb.filesystem.PrepareCacheMove(sealed, sb.ssize, false)
	if err != nil {
		return xerrors.Errorf("prepare move sealed: %w", err)
	}
	defer sb.filesystem.Release(sealedDest, sb.ssize)

	cacheDest, err := sb.filesystem.PrepareCacheMove(cache, sb.ssize, false)
	if err != nil {
		return xerrors.Errorf("prepare move cache: %w", err)
	}
	defer sb.filesystem.Release(cacheDest, sb.ssize)

	stagedDest, err := sb.filesystem.PrepareCacheMove(staged, sb.ssize, false)
	if err != nil {
		return xerrors.Errorf("prepare move staged: %w", err)
	}
	defer sb.filesystem.Release(stagedDest, sb.ssize)

	if err := sb.filesystem.MoveSector(sealed, sealedDest); err != nil {
		return xerrors.Errorf("move sealed: %w", err)
	}
	if err := sb.filesystem.MoveSector(cache, cacheDest); err != nil {
		return xerrors.Errorf("move cache: %w", err)
	}
	if err := sb.filesystem.MoveSector(staged, stagedDest); err != nil {
		return xerrors.Errorf("move staged: %w", err)
	}

	return nil
}

func (sb *SectorBuilder) DropStaged(ctx context.Context, num abi.SectorNumber) error {
	sp, err := sb.SectorPath(fs.DataStaging, num)
	if err != nil {
		return xerrors.Errorf("finding staged sector: %w", err)
	}

	if err := sb.filesystem.Lock(ctx, sp); err != nil {
		return err
	}
	defer sb.filesystem.Unlock(sp)

	return os.RemoveAll(string(sp))
}

func (sb *SectorBuilder) ImportFrom(osb *SectorBuilder, symlink bool) error {
	if err := osb.filesystem.MigrateTo(sb.filesystem, sb.ssize, symlink); err != nil {
		return xerrors.Errorf("migrating sector data: %w", err)
	}

	val, err := osb.ds.Get(lastSectorNumKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Warnf("CAUTION: last sector ID not found in previous datastore")
			return nil
		}
		return err
	}

	if err := sb.ds.Put(lastSectorNumKey, val); err != nil {
		return err
	}

	sb.lastNum = osb.lastNum

	return nil
}

func (sb *SectorBuilder) SetLastSectorNum(num abi.SectorNumber) error {
	if err := sb.ds.Put(lastSectorNumKey, []byte(fmt.Sprint(num))); err != nil {
		return err
	}

	sb.lastNum = num
	return nil
}

func (sb *SectorBuilder) Stop() {
	close(sb.stopping)
}

func sizeFromConfig(cfg Config) (abi.SectorSize, error) {
	if cfg.SealProofType == abi.RegisteredProof(0) {
		return abi.SectorSize(0), xerrors.New("must specify a seal proof type from abi.RegisteredProof")
	}

	if cfg.PoStProofType == abi.RegisteredProof(0) {
		return abi.SectorSize(0), xerrors.New("must specify a PoSt proof type from abi.RegisteredProof")
	}

	s1, err := sizeFromProofType(cfg.SealProofType)
	if err != nil {
		return abi.SectorSize(0), err
	}

	s2, err := sizeFromProofType(cfg.PoStProofType)
	if err != nil {
		return abi.SectorSize(0), err
	}

	if s1 != s2 {
		return abi.SectorSize(0), xerrors.Errorf("seal sector size %d does not equal PoSt sector size %d", s1, s2)
	}

	return s1, nil
}

func sizeFromProofType(p abi.RegisteredProof) (abi.SectorSize, error) {
	x, err := p.RegisteredPoStProof()
	if err != nil {
		return 0, err
	}

	// values taken from https://github.com/filecoin-project/rust-fil-proofs/blob/master/filecoin-proofs/src/constants.rs#L11

	switch x {
	case abi.RegisteredProof_StackedDRG32GiBPoSt:
		return 1 << 35, nil
	case abi.RegisteredProof_StackedDRG2KiBPoSt:
		return 2048, nil
	case abi.RegisteredProof_StackedDRG8MiBPoSt:
		return 1 << 23, nil
	case abi.RegisteredProof_StackedDRG512MiBPoSt:
		return 1 << 29, nil
	default:
		return abi.SectorSize(0), errors.Errorf("unsupported proof type: %+v", p)
	}
}
