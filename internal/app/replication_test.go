package app

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
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
