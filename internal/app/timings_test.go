package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_TimingsLifecycle(t *testing.T) {
	timing := NewTimings()

	t1 := timing.Get(MasterStuckAt, "host")
	require.Equal(t, t1, time.Time{})

	// some time
	fixTime := time.Date(2025, 01, 01, 23, 0, 0, 0, time.UTC)
	// some other time
	fixTime2 := time.Date(2025, 02, 02, 23, 0, 0, 0, time.UTC)

	timing.SetIfZero(MasterStuckAt, "host", fixTime)
	t1 = timing.Get(MasterStuckAt, "host")
	require.Equal(t, t1, fixTime)

	// Setting second time doesn't change value
	timing.SetIfZero(MasterStuckAt, "host", fixTime2)
	t1 = timing.Get(MasterStuckAt, "host")
	require.Equal(t, t1, fixTime)

	// host2 time is still zero
	t1 = timing.Get(MasterStuckAt, "host2")
	require.Equal(t, t1, time.Time{})

	// NodeFailedAt is still zero
	t1 = timing.Get(NodeFailedAt, "host")
	require.Equal(t, t1, time.Time{})

	timing.Clean(MasterStuckAt, "host")
	t1 = timing.Get(MasterStuckAt, "host")
	require.Equal(t, t1, time.Time{})
}
