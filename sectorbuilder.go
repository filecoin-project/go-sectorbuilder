package sectorbuilder

import (
	"context"

	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"

	ffi "github.com/filecoin-project/filecoin-ffi"
)

var log = logging.Logger("sectorbuilder")

type Config struct {
	SealProofType abi.RegisteredProof
	PoStProofType abi.RegisteredProof
	Miner         address.Address

	_ struct{} // guard against nameless init
}

type SectorBuilder struct {
	sealProofType abi.RegisteredProof
	postProofType abi.RegisteredProof
	ssize         abi.SectorSize // a function of sealProofType and postProofType

	Miner address.Address

	sectors  SectorProvider
	stopping chan struct{}
}

func New(sectors SectorProvider, cfg *Config) (*SectorBuilder, error) {
	sectorSize, err := sizeFromConfig(*cfg)
	if err != nil {
		return nil, err
	}

	sb := &SectorBuilder{
		sealProofType: cfg.SealProofType,
		postProofType: cfg.PoStProofType,
		ssize:         sectorSize,

		Miner: cfg.Miner,

		sectors: sectors,

		stopping: make(chan struct{}),
	}

	return sb, nil
}

func (sb *SectorBuilder) pubSectorToPriv(ctx context.Context, sectorInfo []abi.SectorInfo, faults []abi.SectorNumber) (ffi.SortedPrivateSectorInfo, error) {
	fmap := map[abi.SectorNumber]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var out []ffi.PrivateSectorInfo
	for _, s := range sectorInfo {
		if _, faulty := fmap[s.SectorNumber]; faulty {
			continue
		}

		paths, done, err := sb.sectors.AcquireSector(ctx, s.SectorNumber, FTCache|FTSealed, 0, false)
		if err != nil {
			return ffi.SortedPrivateSectorInfo{}, xerrors.Errorf("acquire sector paths: %w", err)
		}
		done() // TODO: This is a tiny bit suboptimal

		postProofType, err := s.RegisteredProof.RegisteredPoStProof()
		if err != nil {
			return ffi.SortedPrivateSectorInfo{}, xerrors.Errorf("acquiring registered PoSt proof from sector info %+v: %w", s, err)
		}

		out = append(out, ffi.PrivateSectorInfo{
			CacheDirPath:     paths.Cache,
			PoStProofType:    postProofType,
			SealedSectorPath: paths.Sealed,
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
