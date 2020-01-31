package fs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathUtils(t *testing.T) {
	sp := SectorPath("/aoe/aaa-oeu/cache/s-t0999-84")

	i, err := sp.id()
	require.NoError(t, err)
	require.EqualValues(t, 84, i)

	a, err := sp.miner()
	require.NoError(t, err)
	require.Equal(t, "t0999", a.String())

	require.Equal(t, DataCache, sp.typ())
	require.Equal(t, "/aoe/aaa-oeu", string(sp.storage()))
}
