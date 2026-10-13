package app

import (
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
				err := app.checkFailoverReplication(cs, health, "master", checkedAt)
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

			// A new failure must not reuse the previous failure's quiet window.
			app.t.Set(NodeFailedAt, "master", now.Add(93*time.Second))
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
			tc.change(cs, health)
			for _, now := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
				require.ErrorContains(t, app.checkFailoverReplication(cs, health, "master", now), tc.wantError)
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
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now))
	// Equivalent GTID formatting is not progress.
	cs["replica1"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-5:6-11"
	cs["cascade"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-100"
	cs["unrelated"].SlaveState.RetrievedGtidSet = failoverTestUUID + ":1-100"
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now.Add(4*time.Second)))
	require.NoError(t, app.checkFailoverReplication(cs, health, "master", now.Add(5*time.Second)))

	// Reconnecting to a different source starts a new observation window.
	cs["replica1"].SlaveState.MasterHost = "other"
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now.Add(6*time.Second)))
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
