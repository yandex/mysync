package app

import (
	"testing"

	"github.com/stretchr/testify/require"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

type fakeExternalReplicationSourceStatus struct {
	statuses   map[string]mysql.ExternalSourceStatus
	resetCount int
}

func newFakeExternalReplicationSourceStatus() *fakeExternalReplicationSourceStatus {
	return &fakeExternalReplicationSourceStatus{
		statuses: make(map[string]mysql.ExternalSourceStatus),
	}
}

func (s *fakeExternalReplicationSourceStatus) GetExtSourcesStatus(host string) mysql.ExternalSourceStatus {
	return s.statuses[host]
}

func (s *fakeExternalReplicationSourceStatus) SetSourcesStatus(host string, status mysql.ExternalSourceStatus) {
	s.statuses[host] = status
}

func (s *fakeExternalReplicationSourceStatus) ResetSourcesStatus() {
	s.statuses = make(map[string]mysql.ExternalSourceStatus)
	s.resetCount++
}

func TestNextExternalReplicationSourceSequence(t *testing.T) {
	sources := []mysql.ReplicationSource{
		{SourceHost: "test_source", Priority: 100},
		{SourceHost: "test_source_2", Priority: 50},
		{SourceHost: "test_source_3", Priority: 10},
	}
	steps := []struct {
		wantSource     string
		wantIgnored    []string
		wantFound      bool
		wantResetCount int
	}{
		{wantSource: "test_source", wantIgnored: []string{}, wantFound: true},
		{wantSource: "test_source_3", wantIgnored: []string{"test_source", "test_source_2"}, wantFound: true},
		{wantIgnored: []string{"test_source", "test_source_2", "test_source_3"}, wantFound: false, wantResetCount: 1},
		{wantSource: "test_source", wantIgnored: []string{}, wantFound: true, wantResetCount: 1},
		{wantSource: "test_source_2", wantIgnored: []string{"test_source"}, wantFound: true, wantResetCount: 1},
		{wantIgnored: []string{"test_source", "test_source_2", "test_source_3"}, wantFound: false, wantResetCount: 2},
		{wantSource: "test_source", wantIgnored: []string{}, wantFound: true, wantResetCount: 2},
		{wantSource: "test_source_3", wantIgnored: []string{"test_source", "test_source_2"}, wantFound: true, wantResetCount: 2},
		{wantIgnored: []string{"test_source", "test_source_2", "test_source_3"}, wantFound: false, wantResetCount: 3},
		{wantSource: "test_source", wantIgnored: []string{}, wantFound: true, wantResetCount: 3},
	}

	status := newFakeExternalReplicationSourceStatus()
	currentSource := "test_source_2"
	for _, step := range steps {
		source, ignored, found := nextExternalReplicationSource(currentSource, sources, status)
		require.Equal(t, step.wantFound, found)
		require.Equal(t, step.wantSource, source.SourceHost)
		require.Equal(t, step.wantIgnored, ignored)
		require.Equal(t, step.wantResetCount, status.resetCount)
		if found {
			currentSource = source.SourceHost
		}
	}
}

func TestExternalReplicationRepairOrderWithSingleAttempt(t *testing.T) {
	app := &App{config: &config.Config{
		ExternalReplicationChannel:   "external",
		ReplicationRepairMaxAttempts: 1,
	}}
	state := &ReplicationRepairState{History: map[ReplicationRepairAlgorithmType]int{
		StartSlave:   0,
		ChangeSource: 0,
	}}

	algorithm, count, err := app.getSuitableAlgorithmType(state, "external")
	require.NoError(t, err)
	require.Equal(t, StartSlave, algorithm)
	require.Zero(t, count)

	state.History[StartSlave]++
	algorithm, count, err = app.getSuitableAlgorithmType(state, "external")
	require.NoError(t, err)
	require.Equal(t, ChangeSource, algorithm)
	require.Zero(t, count)
}
func TestReplicaConverged(t *testing.T) {
	lowMark := 5.0

	makeState := func(lag *float64) *nodestate.NodeState {
		return &nodestate.NodeState{
			SlaveState: &nodestate.SlaveState{ReplicationLag: lag},
		}
	}

	t.Run("target with high lag returns false — turbo must not be skipped", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(1.0)),   // low lag but NOT the target
			"replica2": makeState(util.Ptr(100.0)), // target — high lag
		}
		// anyReplicaConverged would incorrectly find replica1 if called for both.
		_, _, ok := anyReplicaConverged([]string{"replica1", "replica2"}, state, lowMark)
		require.True(t, ok, "sanity: anyReplicaConverged finds replica1")

		// replicaConverged checks only the designated target.
		_, ok = replicaConverged(state, "replica2", lowMark)
		require.False(t, ok, "target replica2 has high lag — turbo must NOT be skipped")
	})

	t.Run("target with low lag returns true — turbo should be skipped", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(100.0)),
			"replica2": makeState(util.Ptr(3.0)),
		}
		lag, ok := replicaConverged(state, "replica2", lowMark)
		require.True(t, ok, "target replica2 already has low lag — turbo should be skipped")
		require.InDelta(t, 3.0, lag, 1e-9)
	})

	t.Run("target absent from state returns false", func(t *testing.T) {
		_, ok := replicaConverged(map[string]*nodestate.NodeState{}, "replica1", lowMark)
		require.False(t, ok)
	})

	t.Run("target with nil lag returns false", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{"replica1": makeState(nil)}
		_, ok := replicaConverged(state, "replica1", lowMark)
		require.False(t, ok)
	})

	t.Run("lag equal to lowMark is not converged", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{"replica1": makeState(util.Ptr(5.0))}
		_, ok := replicaConverged(state, "replica1", lowMark)
		require.False(t, ok)
	})
}

func TestAnyReplicaConverged(t *testing.T) {
	lowMark := 5.0 // seconds

	makeState := func(lag *float64) *nodestate.NodeState {
		return &nodestate.NodeState{
			SlaveState: &nodestate.SlaveState{
				ReplicationLag: lag,
			},
		}
	}

	t.Run("returns false when replica list is empty", func(t *testing.T) {
		_, _, ok := anyReplicaConverged(nil, nil, lowMark)
		require.False(t, ok)
	})

	t.Run("returns false when all replicas have high lag", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(100.0)),
			"replica2": makeState(util.Ptr(50.0)),
		}
		_, _, ok := anyReplicaConverged([]string{"replica1", "replica2"}, state, lowMark)
		require.False(t, ok)
	})

	t.Run("returns false when replica has no lag info", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(nil),
		}
		_, _, ok := anyReplicaConverged([]string{"replica1"}, state, lowMark)
		require.False(t, ok)
	})

	t.Run("returns false when replica state is nil", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": nil,
		}
		_, _, ok := anyReplicaConverged([]string{"replica1"}, state, lowMark)
		require.False(t, ok)
	})

	t.Run("returns true for replica with lag below low mark", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(100.0)),
			"replica2": makeState(util.Ptr(3.0)),
		}
		host, lag, ok := anyReplicaConverged([]string{"replica1", "replica2"}, state, lowMark)
		require.True(t, ok)
		require.Equal(t, "replica2", host)
		require.InDelta(t, 3.0, lag, 1e-9)
	})

	t.Run("lag equal to low mark is not converged", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(5.0)),
		}
		_, _, ok := anyReplicaConverged([]string{"replica1"}, state, lowMark)
		require.False(t, ok)
	})

	t.Run("returns the first converged replica in order", func(t *testing.T) {
		state := map[string]*nodestate.NodeState{
			"replica1": makeState(util.Ptr(1.0)),
			"replica2": makeState(util.Ptr(2.0)),
		}
		host, _, ok := anyReplicaConverged([]string{"replica1", "replica2"}, state, lowMark)
		require.True(t, ok)
		require.Equal(t, "replica1", host)
	})
}
