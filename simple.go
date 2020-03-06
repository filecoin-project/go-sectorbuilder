//+build cgo

package sectorbuilder

import (
	"context"
	"fmt"
	"io"
	"math/bits"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	"go.opencensus.io/trace"

	ffi "github.com/filecoin-project/filecoin-ffi"
)

var _ Verifier = ProofVerifier

func (sb *SectorBuilder) SectorSize() abi.SectorSize {
	return sb.ssize
}

func (sb *SectorBuilder) SealProofType() abi.RegisteredProof {
	return sb.sealProofType
}

func (sb *SectorBuilder) PoStProofType() abi.RegisteredProof {
	return sb.postProofType
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

	info.Randomness[31] = 0

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
	var sum abi.PaddedPieceSize
	for _, p := range pieces {
		sum += p.Size
	}

	ssize, err := SectorSizeForRegisteredProof(proofType)
	if err != nil {
		return cid.Undef, err
	}

	{
		// pad remaining space with 0 CommPs
		toFill := uint64(abi.PaddedPieceSize(ssize) - sum)
		n := bits.OnesCount64(toFill)
		for i := 0; i < n; i++ {
			next := bits.TrailingZeros64(toFill)
			psize := uint64(1) << uint(next)
			toFill ^= psize

			unpadded := abi.PaddedPieceSize(psize).Unpadded()
			pieces = append(pieces, abi.PieceInfo{
				Size:     unpadded.Padded(),
				PieceCID: ZeroPieceCommitment(unpadded),
			})
		}
	}

	return ffi.GenerateUnsealedCID(proofType, pieces)
}

// TODO: remove this method after implementing it along side the registered proofs and importing it from there.
func SectorSizeForRegisteredProof(p abi.RegisteredProof) (abi.SectorSize, error) {
	switch p {
	case abi.RegisteredProof_StackedDRG32GiBSeal, abi.RegisteredProof_StackedDRG32GiBPoSt:
		return 32 << 30, nil
	case abi.RegisteredProof_StackedDRG2KiBSeal, abi.RegisteredProof_StackedDRG2KiBPoSt:
		return 2 << 10, nil
	case abi.RegisteredProof_StackedDRG8MiBSeal, abi.RegisteredProof_StackedDRG8MiBPoSt:
		return 8 << 20, nil
	case abi.RegisteredProof_StackedDRG512MiBSeal, abi.RegisteredProof_StackedDRG512MiBPoSt:
		return 512 << 20, nil
	default:
		return 0, fmt.Errorf("unsupported registered proof %d", p)
	}
}
