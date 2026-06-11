package app

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

// newTestApp builds a minimal *App suitable for unit tests.
// appDCS is injected so tests can pass a MockIAppDCS.
func newTestApp(t *testing.T, cfg *config.Config, appDCS IAppDCS) *App {
	t.Helper()
	logger, _, _, err := log.Open("/dev/null", "fatal", 100, 0)
	require.NoError(t, err)
	return &App{
		config:       cfg,
		logger:       logger,
		t:            NewTimings(),
		appDCS:       appDCS,
		switchHelper: mysql.NewSwitchHelper(cfg),
	}
}

// minConfig returns a config with sensible defaults for tests.
func minConfig() *config.Config {
	return &config.Config{
		Failover:         true,
		FailoverCooldown: 30 * time.Second,
		FailoverDelay:    0,
		SemiSync:         false,
	}
}

// aliveReplica returns a NodeState for a healthy HA replica.
// SlaveState must be non-nil for countAliveHASlavesWithinNodes to count it.
func aliveReplica() *nodestate.NodeState {
	return &nodestate.NodeState{
		PingOk:     true,
		IsMaster:   false,
		IsCascade:  false,
		SlaveState: &nodestate.SlaveState{},
	}
}

// clusterState builds a map[host]NodeState with the given master and HA replicas.
func clusterState(master string, replicas ...string) map[string]*nodestate.NodeState {
	m := make(map[string]*nodestate.NodeState)
	m[master] = &nodestate.NodeState{PingOk: true, IsMaster: true}
	for _, r := range replicas {
		m[r] = aliveReplica()
	}
	return m
}

// ─── approveFailover ────────────────────────────────────────────────────────

func TestApproveFailover_DisabledInConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.Failover = false
	app := newTestApp(t, cfg, NewMockIAppDCS(ctrl))

	cs := clusterState("master1", "replica1")
	err := app.approveFailover(cs, cs, []string{"replica1"}, "master1")
	require.ErrorContains(t, err, "auto_failover is disabled")
}

func TestApproveFailover_NoQuorum(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.SemiSync = true
	cfg.RplSemiSyncMasterWaitForSlaveCount = 1

	mockDCS := NewMockIAppDCS(ctrl)
	// GetLastSwitchover is called after quorum check — but quorum fails first, so no call expected
	app := newTestApp(t, cfg, mockDCS)

	cs := clusterState("master1") // no replicas → no quorum
	err := app.approveFailover(cs, cs, []string{}, "master1")
	require.ErrorContains(t, err, "no quorum")
}

func TestApproveFailover_CooldownNotElapsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.FailoverCooldown = 60 * time.Second

	mockDCS := NewMockIAppDCS(ctrl)
	// Last auto-failover finished 10 seconds ago — within cooldown
	lastSwitch := Switchover{
		Cause: CauseAuto,
		Result: &SwitchoverResult{
			Ok:         true,
			FinishedAt: time.Now().Add(-10 * time.Second),
		},
	}
	mockDCS.EXPECT().GetLastSwitchover().Return(lastSwitch)

	app := newTestApp(t, cfg, mockDCS)

	cs := clusterState("master1", "replica1")
	err := app.approveFailover(cs, cs, []string{"replica1"}, "master1")
	require.ErrorContains(t, err, "not enough time from last failover")
}

func TestApproveFailover_CooldownElapsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.FailoverCooldown = 10 * time.Second

	mockDCS := NewMockIAppDCS(ctrl)
	// Last auto-failover finished 60 seconds ago — cooldown elapsed
	lastSwitch := Switchover{
		Cause: CauseAuto,
		Result: &SwitchoverResult{
			Ok:         true,
			FinishedAt: time.Now().Add(-60 * time.Second),
		},
	}
	mockDCS.EXPECT().GetLastSwitchover().Return(lastSwitch)

	app := newTestApp(t, cfg, mockDCS)

	cs := clusterState("master1", "replica1")
	err := app.approveFailover(cs, cs, []string{"replica1"}, "master1")
	require.NoError(t, err)
}

func TestApproveFailover_ManualSwitchoverDoesNotTriggerCooldown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.FailoverCooldown = 60 * time.Second

	mockDCS := NewMockIAppDCS(ctrl)
	// Last switchover was manual (CauseManual) — cooldown only applies to CauseAuto
	lastSwitch := Switchover{
		Cause: CauseManual,
		Result: &SwitchoverResult{
			Ok:         true,
			FinishedAt: time.Now().Add(-5 * time.Second),
		},
	}
	mockDCS.EXPECT().GetLastSwitchover().Return(lastSwitch)

	app := newTestApp(t, cfg, mockDCS)

	cs := clusterState("master1", "replica1")
	err := app.approveFailover(cs, cs, []string{"replica1"}, "master1")
	require.NoError(t, err)
}

func TestApproveFailover_NoLastSwitchover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()

	mockDCS := NewMockIAppDCS(ctrl)
	// No previous switchover — zero-value Switchover has nil Result
	mockDCS.EXPECT().GetLastSwitchover().Return(Switchover{})

	app := newTestApp(t, cfg, mockDCS)

	cs := clusterState("master1", "replica1")
	err := app.approveFailover(cs, cs, []string{"replica1"}, "master1")
	require.NoError(t, err)
}

func TestApproveFailover_AfterCrashRecovery_SkipsDelayCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := minConfig()
	cfg.FailoverDelay = 60 * time.Second // would normally block
	cfg.ResetupCrashedHosts = true

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetLastSwitchover().Return(Switchover{})

	app := newTestApp(t, cfg, mockDCS)
	// NodeFailedAt is zero → failingTime is huge, but crash recovery skips the check
	cs := clusterState("master1", "replica1")
	dcsState := map[string]*nodestate.NodeState{
		"master1": {
			PingOk:   true,
			IsMaster: true,
			DaemonState: &nodestate.DaemonState{
				CrashRecovery: true,
			},
		},
		"replica1": {PingOk: true},
	}

	err := app.approveFailover(cs, dcsState, []string{"replica1"}, "master1")
	require.NoError(t, err)
}

// ─── getMasterHost ───────────────────────────────────────────────────────────

func TestGetMasterHost_OneMaster(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	cs := clusterState("master1", "replica1", "replica2")
	host, err := app.getMasterHost(cs)
	require.NoError(t, err)
	require.Equal(t, "master1", host)
}

func TestGetMasterHost_NoMaster(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	cs := map[string]*nodestate.NodeState{
		"node1": {PingOk: true, IsMaster: false},
		"node2": {PingOk: true, IsMaster: false},
	}
	host, err := app.getMasterHost(cs)
	require.NoError(t, err)
	require.Empty(t, host)
}

func TestGetMasterHost_ManyMasters(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	cs := map[string]*nodestate.NodeState{
		"node1": {PingOk: true, IsMaster: true},
		"node2": {PingOk: true, IsMaster: true},
	}
	_, err := app.getMasterHost(cs)
	require.ErrorIs(t, err, ErrManyMasters)
}

func TestGetMasterHost_DeadMasterNotCounted(t *testing.T) {
	// A node with PingOk=false and IsMaster=true should NOT be returned as master
	app := newTestApp(t, minConfig(), nil)
	cs := map[string]*nodestate.NodeState{
		"dead-master": {PingOk: false, IsMaster: true},
		"replica1":    {PingOk: true, IsMaster: false},
	}
	host, err := app.getMasterHost(cs)
	require.NoError(t, err)
	require.Empty(t, host)
}

// ─── getCurrentMaster ────────────────────────────────────────────────────────

func TestGetCurrentMaster_FromDCS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetMasterHostFromDcs().Return("master1", nil)

	app := newTestApp(t, minConfig(), mockDCS)
	cs := clusterState("master1", "replica1")
	host, err := app.getCurrentMaster(cs)
	require.NoError(t, err)
	require.Equal(t, "master1", host)
}

func TestGetCurrentMaster_FallbackToClusterState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	// DCS returns empty — fall back to scanning cluster state
	mockDCS.EXPECT().GetMasterHostFromDcs().Return("", nil)
	mockDCS.EXPECT().SetMasterHost("master1").Return("master1", nil)

	app := newTestApp(t, minConfig(), mockDCS)
	cs := clusterState("master1", "replica1")
	host, err := app.getCurrentMaster(cs)
	require.NoError(t, err)
	require.Equal(t, "master1", host)
}

func TestGetCurrentMaster_NoMasterAnywhere(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetMasterHostFromDcs().Return("", nil)

	app := newTestApp(t, minConfig(), mockDCS)
	cs := map[string]*nodestate.NodeState{
		"node1": {PingOk: true, IsMaster: false},
	}
	_, err := app.getCurrentMaster(cs)
	require.ErrorIs(t, err, ErrNoMaster)
}
