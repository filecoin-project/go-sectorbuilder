package sectorbuilder

import (
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"

	"github.com/filecoin-project/go-sectorbuilder/fs"
)

type WorkerCfg struct {
	NoPreCommit bool
	NoCommit    bool

	// TODO: 'cost' info, probably in terms of sealing + transfer speed
}

type SectorBuilder struct {
	ds    datastore.Batching
	numLk sync.Mutex

	lastNum abi.SectorNumber

	sealProofType abi.RegisteredProof
	postProofType abi.RegisteredProof
	ssize         abi.SectorSize // a function of sealProofType and postProofType

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

	fsLk       sync.Mutex //nolint: structcheck, unused
	filesystem *fs.FS

	stopping chan struct{}
}

type remote struct {
	lk sync.Mutex

	sealTasks chan<- WorkerTask
	busy      uint64 // only for metrics
}

type JsonEncodablePreCommitOutput struct {
	CommD cid.Cid
	CommR cid.Cid
}

type SealRes struct {
	Err   string
	GoErr error `json:"-"`

	Proof []byte
	Rspco JsonEncodablePreCommitOutput
}
