package app

import (
	"testing"

	"github.com/stretchr/testify/require"
	nodestate "github.com/yandex/mysync/internal/app/node_state"
)

func TestSemiSyncNeedsAdjustmentOnMaster(t *testing.T) {
	testCases := []struct {
		name           string
		semiSyncState  *nodestate.SemiSyncState
		waitSlaveCount int
		expected       bool
	}{
		{
			name:           "state is unavailable",
			waitSlaveCount: 0,
		},
		{
			name:           "semi-sync is disabled and not required",
			semiSyncState:  new(nodestate.SemiSyncState),
			waitSlaveCount: 0,
		},
		{
			name:           "source is enabled but not required",
			semiSyncState:  &nodestate.SemiSyncState{MasterEnabled: true, WaitSlaveCount: 1},
			waitSlaveCount: 0,
			expected:       true,
		},
		{
			name:           "promoted replica is enabled but semi-sync is not required",
			semiSyncState:  &nodestate.SemiSyncState{SlaveEnabled: true, WaitSlaveCount: 1},
			waitSlaveCount: 0,
			expected:       true,
		},
		{
			name:           "source role and wait count match",
			semiSyncState:  &nodestate.SemiSyncState{MasterEnabled: true, WaitSlaveCount: 1},
			waitSlaveCount: 1,
		},
		{
			name:           "replica role must be changed to source",
			semiSyncState:  &nodestate.SemiSyncState{SlaveEnabled: true, WaitSlaveCount: 1},
			waitSlaveCount: 1,
			expected:       true,
		},
		{
			name:           "source must not also have replica role enabled",
			semiSyncState:  &nodestate.SemiSyncState{MasterEnabled: true, SlaveEnabled: true, WaitSlaveCount: 1},
			waitSlaveCount: 1,
			expected:       true,
		},
		{
			name:           "wait count must be changed",
			semiSyncState:  &nodestate.SemiSyncState{MasterEnabled: true, WaitSlaveCount: 2},
			waitSlaveCount: 1,
			expected:       true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			state := &nodestate.NodeState{SemiSyncState: testCase.semiSyncState}
			require.Equal(t, testCase.expected, semiSyncNeedsAdjustmentOnMaster(state, testCase.waitSlaveCount))
		})
	}
}
