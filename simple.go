//+build cgo

package sectorbuilder

import (
	"context"
	"io"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	"go.opencensus.io/trace"
)

var _ Verifier = ProofVerifier

func (sb *SectorBuilder) SectorSize() abi.SectorSize {
	return sb.ssize
}

type proofVerifier struct{}

var ProofVerifier = proofVerifier{}

func (proofVerifier) VerifySeal(info abi.SealVerifyInfo) (bool, error) {
	return ffi.VerifySeal(info)
}

func (proofVerifier) VerifyElectionPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error) {
	return verifyPost(ctx, info)
}

func (proofVerifier) VerifyFallbackPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error) {
	return verifyPost(ctx, info)
}

func verifyPost(ctx context.Context, info abi.PoStVerifyInfo) (bool, error) {
	_, span := trace.StartSpan(ctx, "VerifyPoSt")
	defer span.End()

	return ffi.VerifyPoSt(info)
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
