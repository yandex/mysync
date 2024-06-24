package gtids

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGTIDDiff(t *testing.T) {
	sourceGTID := ParseGtidSet("00000000-0000-0000-0000-000000000000:1-100,11111111-1111-1111-1111-111111111111:1-100")

	// equal
	replicaGTID := ParseGtidSet("00000000-0000-0000-0000-000000000000:1-100,11111111-1111-1111-1111-111111111111:1-100")
	diff, err := GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "replica gtid equal source", diff)

	replicaGTID = ParseGtidSet("11111111-1111-1111-1111-111111111111:1-100,00000000-0000-0000-0000-000000000000:1-100")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "replica gtid equal source", diff)

	// source ahead
	replicaGTID = ParseGtidSet("00000000-0000-0000-0000-000000000000:1-90,11111111-1111-1111-1111-111111111111:1-100")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "source ahead on: 00000000-0000-0000-0000-000000000000:91-100", diff)

	replicaGTID = ParseGtidSet("00000000-0000-0000-0000-000000000000:1-90,11111111-1111-1111-1111-111111111111:1-90")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "source ahead on: 00000000-0000-0000-0000-000000000000:91-100,11111111-1111-1111-1111-111111111111:91-100", diff)

	// replica ahead
	replicaGTID = ParseGtidSet("00000000-0000-0000-0000-000000000000:1-110,11111111-1111-1111-1111-111111111111:1-100")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "replica ahead on: 00000000-0000-0000-0000-000000000000:101-110", diff)

	// split brain
	replicaGTID = ParseGtidSet("00000000-0000-0000-0000-000000000000:1-90,11111111-1111-1111-1111-111111111111:1-110")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "split brain! source ahead on: 00000000-0000-0000-0000-000000000000:91-100; replica ahead on: 11111111-1111-1111-1111-111111111111:101-110", diff)

	replicaGTID = ParseGtidSet("00000000-0000-0000-0000-000000000000:1-100,22222222-2222-2222-2222-222222222222:1-110")
	diff, err = GTIDDiff(replicaGTID, sourceGTID)
	require.NoError(t, err)
	require.Equal(t, "split brain! source ahead on: 11111111-1111-1111-1111-111111111111:1-100; replica ahead on: 22222222-2222-2222-2222-222222222222:1-110", diff)
}
