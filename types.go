package sectorbuilder

import (
	"sync"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-datastore"
)

type SortedPublicSectorInfo = ffi.SortedPublicSectorInfo
type SortedPrivateSectorInfo = ffi.SortedPrivateSectorInfo

type SealTicket = ffi.SealTicket

type SealSeed = ffi.SealSeed

type SealPreCommitOutput = ffi.SealPreCommitOutput

type SealCommitOutput = ffi.SealCommitOutput

type PublicPieceInfo = ffi.PublicPieceInfo

type RawSealPreCommitOutput ffi.RawSealPreCommitOutput

type EPostCandidate = ffi.Candidate

const CommLen = ffi.CommitmentBytesLen

type WorkerCfg struct {
	NoPreCommit bool
	NoCommit    bool

	// TODO: 'cost' info, probably in terms of sealing + transfer speed
}

type SectorBuilder struct {
	ds   datastore.Batching
	idLk sync.Mutex

	ssize  uint64
	lastID uint64

	Miner address.Address

	unsealLk sync.Mutex

	noCommit    bool
	noPreCommit bool
	rateLimit   chan struct{}

	precommitTasks chan workerCall
	commitTasks    chan workerCall

	taskCtr       uint64
	remoteLk      sync.Mutex
	remoteCtr     int
	remotes       map[int]*remote
	remoteResults map[uint64]chan<- SealRes

	addPieceWait  int32
	preCommitWait int32
	commitWait    int32
	unsealWait    int32

	fsLk       sync.Mutex //nolint: struckcheck
	filesystem *fs        // TODO: multi-fs support

	stopping chan struct{}
}

type remote struct {
	lk sync.Mutex

	sealTasks chan<- WorkerTask
	busy      uint64 // only for metrics
}

type JsonRSPCO struct {
	CommD []byte
	CommR []byte
}

type SealRes struct {
	Err   string
	GoErr error `json:"-"`

	Proof []byte
	Rspco JsonRSPCO
}
