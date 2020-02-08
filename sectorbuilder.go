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
	datastore "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

const PoStReservedWorkers = 1
const PoRepProofPartitions = 10

var lastSectorIdKey = datastore.NewKey("/last")

var log = logging.Logger("sectorbuilder")

func (rspco *RawSealPreCommitOutput) ToJson() JsonRSPCO {
	return JsonRSPCO{
		CommD: rspco.CommD[:],
		CommR: rspco.CommR[:],
	}
}

func (rspco *JsonRSPCO) rspco() RawSealPreCommitOutput {
	var out RawSealPreCommitOutput
	copy(out.CommD[:], rspco.CommD)
	copy(out.CommR[:], rspco.CommR)
	return out
}

type Config struct {
	SectorSize abi.SectorSize
	Miner      address.Address

	WorkerThreads  uint8
	FallbackLastID uint64
	NoCommit       bool
	NoPreCommit    bool

	Paths []fs.PathConfig
	_     struct{} // guard against nameless init
}

func New(cfg *Config, ds datastore.Batching) (*SectorBuilder, error) {
	if cfg.WorkerThreads < PoStReservedWorkers {
		return nil, xerrors.Errorf("minimum worker threads is %d, specified %d", PoStReservedWorkers, cfg.WorkerThreads)
	}

	var lastUsedID uint64
	b, err := ds.Get(lastSectorIdKey)
	switch err {
	case nil:
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return nil, err
		}
		lastUsedID = uint64(i)
	case datastore.ErrNotFound:
		lastUsedID = cfg.FallbackLastID
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

		ssize:  cfg.SectorSize,
		lastID: lastUsedID,

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
	sb := &SectorBuilder{
		ds: nil,

		ssize: cfg.SectorSize,

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

func (sb *SectorBuilder) AcquireSectorId() (abi.SectorNumber, error) {
	sb.idLk.Lock()
	defer sb.idLk.Unlock()

	sb.lastID++
	id := sb.lastID

	err := sb.ds.Put(lastSectorIdKey, []byte(fmt.Sprint(id)))
	if err != nil {
		return 0, err
	}
	return abi.SectorNumber(id), nil
}

func (sb *SectorBuilder) sealPreCommitRemote(call workerCall) (RawSealPreCommitOutput, error) {
	atomic.AddInt32(&sb.preCommitWait, -1)

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.Rspco.rspco(), err
	case <-sb.stopping:
		return RawSealPreCommitOutput{}, xerrors.New("sectorbuilder stopped")
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

func (sb *SectorBuilder) pubSectorToPriv(sectorInfo SortedPublicSectorInfo, faults []abi.SectorNumber) (SortedPrivateSectorInfo, error) {
	fmap := map[abi.SectorNumber]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var out []ffi.PrivateSectorInfo
	for _, s := range sectorInfo.Values() {
		if _, faulty := fmap[abi.SectorNumber(s.SectorID)]; faulty {
			continue
		}

		cachePath, err := sb.SectorPath(fs.DataCache, abi.SectorNumber(s.SectorID)) // TODO: LOCK!
		if err != nil {
			return SortedPrivateSectorInfo{}, xerrors.Errorf("getting cache paths for sector %d: %w", s.SectorID, err)
		}

		sealedPath, err := sb.SectorPath(fs.DataSealed, abi.SectorNumber(s.SectorID))
		if err != nil {
			return SortedPrivateSectorInfo{}, xerrors.Errorf("getting sealed paths for sector %d: %w", s.SectorID, err)
		}

		out = append(out, ffi.PrivateSectorInfo{
			SectorID:         s.SectorID,
			CommR:            s.CommR,
			CacheDirPath:     string(cachePath),
			SealedSectorPath: string(sealedPath),
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

func (sb *SectorBuilder) FinalizeSector(ctx context.Context, id abi.SectorNumber) error {
	sealed, err := sb.filesystem.FindSector(fs.DataSealed, sb.Miner, id)
	if err != nil {
		return xerrors.Errorf("getting sealed sector: %w", err)
	}
	cache, err := sb.filesystem.FindSector(fs.DataCache, sb.Miner, id)
	if err != nil {
		return xerrors.Errorf("getting sector cache: %w", err)
	}

	// todo: flag to just remove
	staged, err := sb.filesystem.FindSector(fs.DataStaging, sb.Miner, id)
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

func (sb *SectorBuilder) DropStaged(ctx context.Context, id abi.SectorNumber) error {
	sp, err := sb.SectorPath(fs.DataStaging, id)
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

	val, err := osb.ds.Get(lastSectorIdKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Warnf("CAUTION: last sector ID not found in previous datastore")
			return nil
		}
		return err
	}

	if err := sb.ds.Put(lastSectorIdKey, val); err != nil {
		return err
	}

	sb.lastID = osb.lastID

	return nil
}

func (sb *SectorBuilder) SetLastSectorID(id abi.SectorNumber) error {
	if err := sb.ds.Put(lastSectorIdKey, []byte(fmt.Sprint(id))); err != nil {
		return err
	}

	sb.lastID = uint64(id)
	return nil
}

func (sb *SectorBuilder) Stop() {
	close(sb.stopping)
}
