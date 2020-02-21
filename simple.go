//+build cgo

package sectorbuilder

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"go.opencensus.io/trace"
)

var _ Verifier = ProofVerifier

func (sb *SectorBuilder) SectorSize() abi.SectorSize {
	return sb.ssize
}

type proofVerifier struct{}

var ProofVerifier = proofVerifier{}

func (proofVerifier) VerifySeal(proofType abi.RegisteredProof, sealedCID, unsealedCID cid.Cid, prover address.Address, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, sectorNum abi.SectorNumber, proof abi.SealProof) (bool, error) {
	return ffi.VerifySeal(proofType, sealedCID, unsealedCID, addressToProverID(prover), ticket, seed, sectorNum, proof)
}

func (proofVerifier) VerifyElectionPost(ctx context.Context, sectorInfo ffi.SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []abi.PoStCandidate, prover address.Address) (bool, error) {
	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo.Values())), 0)
	return verifyPost(ctx, sectorInfo, challengeCount, challengeSeed, proof, candidates, prover)
}

func (proofVerifier) VerifyFallbackPost(ctx context.Context, sectorInfo ffi.SortedPublicSectorInfo, challengeSeed []byte, proof []byte, candidates []abi.PoStCandidate, prover address.Address, faults uint64) (bool, error) {
	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo.Values())), faults)
	return verifyPost(ctx, sectorInfo, challengeCount, challengeSeed, proof, candidates, prover)
}

func verifyPost(ctx context.Context, sectorInfo ffi.SortedPublicSectorInfo, challengeCount uint64, challengeSeed abi.PoStRandomness, proof []byte, candidates []abi.PoStCandidate, prover address.Address) (bool, error) {
	_, span := trace.StartSpan(ctx, "VerifyPoSt")
	defer span.End()

	return ffi.VerifyPoSt(sectorInfo, challengeSeed, challengeCount, proof, candidates, addressToProverID(prover))
}

func GeneratePieceCIDFromFile(proofType abi.RegisteredProof, piece io.Reader, pieceSize abi.UnpaddedPieceSize) (cid.Cid, error) {
	f, werr, err := toReadableFile(piece, int64(pieceSize))
	if err != nil {
		return cid.Undef, err
	}

	pieceCID, err := ffi.GeneratePieceCIDFromFile(proofType, f, pieceSize)
	if err != nil {
		return cid.Undef, err
	}

	return pieceCID, werr()
}

func GenerateUnsealedCID(proofType abi.RegisteredProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	return ffi.GenerateUnsealedCID(proofType, pieces)
}
