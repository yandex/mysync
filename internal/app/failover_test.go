package app

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

const failoverTestUUID = "908fa82e-1529-11ef-8446-a0362c3daca9"

func stalledFailoverCluster() (map[string]*nodestate.NodeState, map[string]*nodestate.NodeState) {
	cs := clusterState("master", "replica1", "replica2")
	cs["master"].PingOk = false
	for _, host := range []string{"replica1", "replica2"} {
		cs[host].SlaveState = &nodestate.SlaveState{
			MasterHost:       "master",
			ReplicationState: mysql.ReplicationRunning,
			ExecutedGtidSet:  failoverTestUUID + ":1-10",
			RetrievedGtidSet: failoverTestUUID + ":1-11",
		}
	}
	health := map[string]*nodestate.NodeState{
		"master": {CheckBy: "master", CheckAt: time.Now(), PingOk: false},
	}
	return cs, health
}

func TestCheckMasterVisibleWithoutHealthyMaster(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	cs, health := stalledFailoverCluster()
	// No database connection is needed when DCS has no healthy master to ping.
	visible, err := app.checkMasterVisible(cs, health)
	require.NoError(t, err)
	require.False(t, visible)
}

func TestNodeStateCheckAtAfterPing(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wantPings int
	}{
		{"initial ping", 1},
		{"retry after status query failure", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, nodes := newFailoverRetryApp(t, failoverTestUUID+":1-11", nil)
			var pingFinishedAt time.Time
			pings := 0
			nodes["C"].queryHook = func(query string) error {
				if query == mysql.DefaultQueries["ping"] {
					pings++
					pingFinishedAt = time.Now()
					if pings == tc.wantPings {
						return errors.New("ping failed")
					}
				} else if query == mysql.DefaultQueries["is_readonly"] {
					return errors.New("status query failed")
				}
				return nil
			}
			state := app.getNodeState("C")
			require.Equal(t, tc.wantPings, pings)
			require.False(t, state.PingOk)
			require.Equal(t, "ping failed", state.Error)
			require.False(t, state.CheckAt.Before(pingFinishedAt), "health timestamp must follow the completed ping")
		})
	}
}

func TestFailoverWaitsForGTIDInactivity(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "all replicas report running"
		if partial {
			name = "only some replicas report running"
		}
		t.Run(name, func(t *testing.T) {
			cfg := minConfig()
			cfg.FailoverDelay = 30 * time.Second
			app := newTestApp(t, cfg, nil)
			cs, health := stalledFailoverCluster()
			if partial {
				cs["replica2"].SlaveState.ReplicationState = mysql.ReplicationError
			}
			now := time.Now()
			check := func(seconds int, wait bool) {
				t.Helper()
				checkedAt := now.Add(time.Duration(seconds) * time.Second)
				health["master"].CheckAt = checkedAt
				var err error
				app.failoverProgress, err = app.checkFailoverReplication(cs, health, "master", app.failoverProgress, checkedAt)
				if wait {
					require.ErrorContains(t, err, "waiting for received GTIDs")
				} else {
					require.NoError(t, err)
				}
			}
			check(0, true)
			check(29, true)
			check(30, false)

			// Receiving transactions counts as progress even if SQL execution is stuck.
			cs["replica1"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-12"
			check(31, true)
			check(60, true)
			check(61, false)

			// Applying buffered transactions must not delay failover.
			cs["replica2"].SlaveState.ExecutedGtidSet = failoverTestUUID + ":1-11"
			check(62, false)

			// A reachable master clears progress before its next failure.
			cs["master"].PingOk = true
			var err error
			app.failoverProgress, err = app.checkFailoverReplication(cs, health, "master", app.failoverProgress, now.Add(63*time.Second))
			require.ErrorContains(t, err, "still reachable")
			require.Nil(t, app.failoverProgress)
			cs["master"].PingOk = false
			check(93, true)
			check(123, false)
			app.state = stateManager
			app.setState(stateCandidate)
			require.Nil(t, app.failoverProgress)
			check(124, true)
		})
	}
}

func TestFailoverKeepsZooKeeperProtection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(map[string]*nodestate.NodeState, map[string]*nodestate.NodeState)
		wantError string
	}{
		{"master reachable by manager", func(cs, _ map[string]*nodestate.NodeState) { cs["master"].PingOk = true }, "still reachable"},
		{"reachable master with some running replicas", func(cs, _ map[string]*nodestate.NodeState) {
			cs["master"].PingOk = true
			cs["replica2"].SlaveState.ReplicationState = mysql.ReplicationError
		}, "still reachable"},
		{"master reports healthy", func(_, health map[string]*nodestate.NodeState) { health["master"].PingOk = true }, "fresh local failure report"},
		{"missing health record", func(_, health map[string]*nodestate.NodeState) { health["master"] = &nodestate.NodeState{} }, "fresh local failure report"},
		{"health check is not local", func(_, health map[string]*nodestate.NodeState) { health["master"].CheckBy = "replica1" }, "fresh local failure report"},
		{"stale health check", func(_, health map[string]*nodestate.NodeState) { health["master"].CheckAt = time.Now().Add(-time.Hour) }, "fresh local failure report"},
		{"future health check", func(_, health map[string]*nodestate.NodeState) {
			health["master"].CheckAt = time.Now().Add(2 * time.Hour)
		}, "fresh local failure report"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newTestApp(t, minConfig(), nil)
			app.config.HealthCheckInterval = 5 * time.Second
			app.config.DBTimeout = 5 * time.Second
			cs, health := stalledFailoverCluster()
			var err error
			app.failoverProgress, err = app.checkFailoverReplication(cs, health, "master", nil, time.Now())
			require.ErrorContains(t, err, "waiting for received GTIDs")
			require.NotNil(t, app.failoverProgress)
			tc.change(cs, health)
			for _, now := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
				app.failoverProgress, err = app.checkFailoverReplication(cs, health, "master", app.failoverProgress, now)
				require.ErrorContains(t, err, tc.wantError)
				require.Nil(t, app.failoverProgress)
			}
		})
	}
}

func TestFailoverProgressOnlyTracksReceivedGTIDsFromMaster(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	app.config.TickInterval = 5 * time.Second
	app.config.HealthCheckInterval = time.Minute
	cs, health := stalledFailoverCluster()
	cs["cascade"] = aliveReplica()
	cs["cascade"].IsCascade = true
	cs["cascade"].SlaveState.MasterHost = "master"
	cs["unrelated"] = aliveReplica()
	cs["unrelated"].SlaveState.MasterHost = "other"
	now := time.Now()
	progress, err := app.checkFailoverReplication(cs, health, "master", nil, now)
	require.Error(t, err)
	// Equivalent GTID formatting is not progress.
	cs["replica1"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-5:6-11"
	cs["cascade"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-100"
	cs["unrelated"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-100"
	progress, err = app.checkFailoverReplication(cs, health, "master", progress, now.Add(4*time.Second))
	require.Error(t, err)
	progress, err = app.checkFailoverReplication(cs, health, "master", progress, now.Add(5*time.Second))
	require.NoError(t, err)

	// A replica switching to a different source is not progress from this master.
	cs["replica1"].SlaveState.MasterHost = "other"
	progress, err = app.checkFailoverReplication(cs, health, "master", progress, now.Add(6*time.Second))
	require.NoError(t, err)

	// A different failed master starts a new observation window.
	cs["replica1"].PingOk = false
	cs["replica2"].SlaveState.MasterHost = "replica1"
	health["replica1"] = &nodestate.NodeState{}
	_, err = app.checkFailoverReplication(cs, health, "replica1", progress, now.Add(7*time.Second))
	require.ErrorContains(t, err, "waiting for received GTIDs")
}

func TestFailoverProgressSurvivesFlakyReplica(t *testing.T) {
	for _, tc := range []struct {
		name    string
		disrupt func(map[string]*nodestate.NodeState)
	}{
		{"failed ping", func(cs map[string]*nodestate.NodeState) { cs["replica2"].PingOk = false }},
		{"missing replica status", func(cs map[string]*nodestate.NodeState) { cs["replica2"].SlaveState = nil }},
		{"missing replica", func(cs map[string]*nodestate.NodeState) { delete(cs, "replica2") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := minConfig()
			cfg.FailoverDelay = 30 * time.Second
			app := newTestApp(t, cfg, nil)
			cs, health := stalledFailoverCluster()
			replica := *cs["replica2"]
			now := time.Now()
			var progress *failoverProgress
			check := func(seconds int, wait bool) {
				t.Helper()
				checkedAt := now.Add(time.Duration(seconds) * time.Second)
				health["master"].CheckAt = checkedAt
				var err error
				progress, err = app.checkFailoverReplication(cs, health, "master", progress, checkedAt)
				if wait {
					require.ErrorContains(t, err, "waiting for received GTIDs")
				} else {
					require.NoError(t, err)
				}
			}
			check(0, true)
			tc.disrupt(cs)
			check(29, true)
			check(30, false)
			cs["replica2"] = &replica
			check(31, false)

			// Transactions received while unreachable still restart the window.
			restored := replica
			tc.disrupt(cs)
			check(32, false)
			restored.SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-12"
			cs["replica2"] = &restored
			check(33, true)
			check(62, true)
			check(63, false)
		})
	}
}

func TestApproveFailoverClearsProgress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(*App, map[string]*nodestate.NodeState, map[string]*nodestate.NodeState)
		wantError string
	}{
		{"failover disabled", func(app *App, _, _ map[string]*nodestate.NodeState) { app.config.Failover = false }, "auto_failover is disabled"},
		{"no running replicas", func(_ *App, cs, _ map[string]*nodestate.NodeState) {
			cs["replica1"].SlaveState.ReplicationState = mysql.ReplicationStopped
			cs["replica2"].SlaveState.ReplicationState = mysql.ReplicationStopped
		}, ""},
		{"crash recovery", func(app *App, _, health map[string]*nodestate.NodeState) {
			app.config.ResetupCrashedHosts = true
			health["master"].DaemonState = &nodestate.DaemonState{CrashRecovery: true}
		}, ""},
		{"readonly filesystem", func(_ *App, _, health map[string]*nodestate.NodeState) {
			health["master"].IsFileSystemReadonly = true
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockDCS := NewMockIAppDCS(gomock.NewController(t))
			if tc.wantError == "" {
				mockDCS.EXPECT().GetLastSwitchover(gomock.Any()).Return(dcs.ErrNotFound)
			}
			app := newTestApp(t, minConfig(), mockDCS)
			cs, health := stalledFailoverCluster()
			app.config.HealthCheckInterval = time.Minute
			active := []string{"master", "replica1", "replica2"}
			require.ErrorContains(t, app.approveFailover(cs, health, active, "master"), "waiting for received GTIDs")
			require.NotNil(t, app.failoverProgress)
			tc.change(app, cs, health)
			err := app.approveFailover(cs, health, active, "master")
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			require.Nil(t, app.failoverProgress)
		})
	}
}

func TestApproveFailoverWithStalledReplicas(t *testing.T) {
	for _, tc := range []struct {
		name      string
		noQuorum  bool
		cooldown  bool
		wantError string
	}{
		{name: "stalled running replicas allow failover"},
		{name: "quorum still required", noQuorum: true, wantError: "no quorum"},
		{name: "cooldown still required", cooldown: true, wantError: "not enough time from last failover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDCS := NewMockIAppDCS(ctrl)
			cfg := minConfig()
			cfg.SemiSync = true
			cfg.RplSemiSyncMasterWaitForSlaveCount = 1
			cfg.FailoverDelay = 30 * time.Second
			cfg.HealthCheckInterval = 5 * time.Second
			cfg.DBTimeout = 5 * time.Second
			app := newTestApp(t, cfg, mockDCS)
			app.t.Set(NodeFailedAt, "master", time.Now().Add(-time.Minute))
			cs, health := stalledFailoverCluster()
			active := []string{"master", "replica1", "replica2"}
			if tc.noQuorum {
				active = []string{"master", "unreachable1", "unreachable2"}
			}
			require.ErrorContains(t, app.approveFailover(cs, health, active, "master"), "waiting for received GTIDs")
			app.failoverProgress.changedAt = time.Now().Add(-time.Minute)
			if !tc.noQuorum {
				mockDCS.EXPECT().GetLastSwitchover(gomock.Any()).DoAndReturn(func(sw *Switchover) error {
					if tc.cooldown {
						*sw = Switchover{Cause: CauseAuto, Result: &SwitchoverResult{FinishedAt: time.Now()}}
						return nil
					}
					return dcs.ErrNotFound
				})
			}
			err := app.approveFailover(cs, health, active, "master")
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
