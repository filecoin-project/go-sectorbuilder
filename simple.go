//+build cgo

package sectorbuilder

import (
	"context"
	"io"

	sectorbuilder "github.com/filecoin-project/filecoin-ffi"
	"go.opencensus.io/trace"

	"github.com/filecoin-project/go-address"
)

var _ Verifier = ProofVerifier

func (sb *SectorBuilder) SectorSize() uint64 {
	return sb.ssize
}

type proofVerifier struct{}

var ProofVerifier = proofVerifier{}

var UserBytesForSectorSize = sectorbuilder.GetMaxUserBytesPerStagedSector

func (proofVerifier) VerifySeal(sectorSize uint64, commR, commD []byte, proverID address.Address, ticket []byte, seed []byte, sectorID uint64, proof []byte) (bool, error) {
	var commRa, commDa, ticketa, seeda [32]byte
	copy(commRa[:], commR)
	copy(commDa[:], commD)
	copy(ticketa[:], ticket)
	copy(seeda[:], seed)
	proverIDa := addressToProverID(proverID)

	return sectorbuilder.VerifySeal(sectorSize, commRa, commDa, proverIDa, ticketa, seeda, sectorID, proof)
}

func (proofVerifier) VerifyElectionPost(ctx context.Context, sectorSize uint64, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address) (bool, error) {
	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo.Values())), 0)
	return verifyPost(ctx, sectorSize, sectorInfo, challengeCount, challengeSeed, proof, candidates, proverID)
}

func (proofVerifier) VerifyFallbackPost(ctx context.Context, sectorSize uint64, sectorInfo SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address, faults uint64) (bool, error) {
	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo.Values())), faults)
	return verifyPost(ctx, sectorSize, sectorInfo, challengeCount, challengeSeed, proof, candidates, proverID)
}

func verifyPost(ctx context.Context, sectorSize uint64, sectorInfo SortedPublicSectorInfo, challengeCount uint64, challengeSeed []byte, proof []byte, candidates []EPostCandidate, proverID address.Address) (bool, error) {
	var challengeSeeda [CommLen]byte
	copy(challengeSeeda[:], challengeSeed)

	_, span := trace.StartSpan(ctx, "VerifyPoSt")
	defer span.End()
	prover := addressToProverID(proverID)
	return sectorbuilder.VerifyPoSt(sectorSize, sectorInfo, challengeSeeda, challengeCount, proof, candidates, prover)
}

func newSortedPrivateSectorInfo(sectors []sectorbuilder.PrivateSectorInfo) SortedPrivateSectorInfo {
	return sectorbuilder.NewSortedPrivateSectorInfo(sectors...)
}

func NewSortedPublicSectorInfo(sectors []sectorbuilder.PublicSectorInfo) SortedPublicSectorInfo {
	return sectorbuilder.NewSortedPublicSectorInfo(sectors...)
}

func GeneratePieceCommitment(piece io.Reader, pieceSize uint64) (commP [CommLen]byte, err error) {
	f, werr, err := toReadableFile(piece, int64(pieceSize))
	if err != nil {
		return [32]byte{}, err
	}

	commP, err = sectorbuilder.GeneratePieceCommitmentFromFile(f, pieceSize)
	if err != nil {
		return [32]byte{}, err
	}

	return commP, werr()
}

func GenerateDataCommitment(ssize uint64, pieces []sectorbuilder.PublicPieceInfo) ([CommLen]byte, error) {
	return sectorbuilder.GenerateDataCommitment(ssize, pieces)
}
