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
				err := app.checkFailoverReplication(cs, health, "master", now.Add(time.Duration(seconds)*time.Second))
				if wait {
					require.ErrorContains(t, err, "waiting for GTID progress to stop")
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

			// Applying transactions also counts, including on an errored replica.
			cs["replica2"].SlaveState.ExecutedGtidSet = failoverTestUUID + ":1-11"
			check(62, true)
			check(91, true)
			check(92, false)

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
		name   string
		change func(map[string]*nodestate.NodeState, map[string]*nodestate.NodeState)
	}{
		{"master reachable by manager", func(cs, _ map[string]*nodestate.NodeState) { cs["master"].PingOk = true }},
		{"master reports healthy", func(_, health map[string]*nodestate.NodeState) { health["master"].PingOk = true }},
		{"missing health record", func(_, health map[string]*nodestate.NodeState) { health["master"] = &nodestate.NodeState{} }},
		{"health check is not local", func(_, health map[string]*nodestate.NodeState) { health["master"].CheckBy = "replica1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newTestApp(t, minConfig(), nil)
			cs, health := stalledFailoverCluster()
			tc.change(cs, health)
			for _, now := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
				require.ErrorContains(t, app.checkFailoverReplication(cs, health, "master", now), "seems zk problems")
				require.Nil(t, app.failoverProgress)
			}
		})
	}
}

func TestFailoverProgressUsesGTIDSetsAndIgnoresCascades(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	app.config.TickInterval = 5 * time.Second
	cs, health := stalledFailoverCluster()
	cs["cascade"] = aliveReplica()
	cs["cascade"].IsCascade = true
	now := time.Now()
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now))
	// Equivalent GTID formatting is not progress.
	cs["replica1"].SlaveState.ExecutedGtidSet = failoverTestUUID + ":1-5:6-10"
	cs["cascade"].SlaveState.ExecutedGtidSet = failoverTestUUID + ":1-100"
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now.Add(4*time.Second)))
	require.NoError(t, app.checkFailoverReplication(cs, health, "master", now.Add(5*time.Second)))

	// Reconnecting to a different source starts a new observation window.
	cs["replica1"].SlaveState.MasterHost = "other"
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now.Add(6*time.Second)))
}

func TestFailoverObservesReachableMasterGTID(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	cs, health := stalledFailoverCluster()
	cs["replica2"].SlaveState.ReplicationState = mysql.ReplicationError
	cs["master"].PingOk = true
	cs["master"].MasterState = &nodestate.MasterState{ExecutedGtidSet: failoverTestUUID + ":1-11"}
	now := time.Now()
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now))
	cs["master"].MasterState.ExecutedGtidSet = failoverTestUUID + ":1-12"
	require.Error(t, app.checkFailoverReplication(cs, health, "master", now.Add(time.Hour)))
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
			app := newTestApp(t, cfg, mockDCS)
			app.t.Set(NodeFailedAt, "master", time.Now().Add(-time.Minute))
			cs, health := stalledFailoverCluster()
			active := []string{"master", "replica1", "replica2"}
			if tc.noQuorum {
				active = []string{"master", "unreachable1", "unreachable2"}
			}
			require.ErrorContains(t, app.approveFailover(cs, health, active, "master"), "waiting for GTID progress to stop")
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
