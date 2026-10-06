package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/app/optimization"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func testSwitchover() Switchover {
	return Switchover{
		To:               "new",
		InitiatedAt:      time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC),
		MasterTransition: SwitchoverTransition,
	}
}

// Regression: failure after changing replication sources must not replay
// catch-up, and failure after promotion must not freeze the writable master.
// Phase actions are simulated here to test checkpointing independently of SQL.
// Example:
//   - attempt 1 executes optimization -> freeze -> catch-up -> turn replicas -> promote (error);
//   - attempt 2 skips the completed actions, retries promote, then executes the remaining phases.
//
// The failed action runs twice; every other action runs once across both attempts.
func TestSwitchoverRetryPreservesCompletedPhases(t *testing.T) {
	for _, failedPhase := range []switchoverPhase{
		switchoverTurnReplicas, switchoverPromote, switchoverAdjustSemiSync,
		switchoverMakeWritable, switchoverReenableEvents, switchoverSetMasterInDCS,
	} {
		t.Run(failedPhase.String(), func(t *testing.T) {
			// Arrange.
			p := &switchoverProgress{request: testSwitchover(), oldMaster: "old"}
			calls := make(map[switchoverPhase]int)
			injected := errors.New("transient SQL/DCS error")
			attempt := func(fail bool) error {
				// Every attempt visits the full phase list, as performSwitchover does.
				// p.run uses the checkpoint to skip already completed actions.
				for phase := switchoverOptimization; phase < switchoverComplete; phase++ {
					err := p.run(phase, func() error {
						calls[phase]++
						if phase == switchoverFreeze {
							p.newMaster = "new"
							p.mostRecent = "old"
						}
						if fail && phase == failedPhase {
							return injected
						}
						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			}

			// Act: the first attempt fails at the selected phase.
			err := attempt(true)

			// Assert.
			require.ErrorIs(t, err, injected)
			// An error leaves the checkpoint at the failed phase, ready to retry.
			require.Equal(t, failedPhase, p.phase)

			// Act: retry without the transient error.
			err = attempt(false)

			// Assert.
			require.NoError(t, err)
			require.Equal(t, switchoverComplete, p.phase)
			// Retrying must also preserve the topology selected during freeze.
			require.Equal(t, "old", p.oldMaster)
			require.Equal(t, "new", p.newMaster)
			for phase := switchoverOptimization; phase < switchoverComplete; phase++ {
				want := 1
				if phase == failedPhase {
					want = 2
				}
				require.Equal(t, want, calls[phase], "phase %d", phase)
			}
		})
	}
}

func TestPerformSwitchoverRetryAfterDCSFailure(t *testing.T) {
	// Arrange.
	ctrl := gomock.NewController(t)
	mockDCS := NewMockIAppDCS(ctrl)
	app := newTestApp(t, minConfig(), mockDCS)
	// No MySQL nodes: replaying any completed SQL phase would panic. The live
	// new master is already writable and must remain untouched on this retry.
	app.cluster = &mysql.Cluster{}
	app.dcs = switchoverTimingDCS{}
	sw := testSwitchover()
	app.switchoverProgress = &switchoverProgress{
		request: sw, phase: switchoverSetMasterInDCS,
		oldMaster: "old", newMaster: "new",
		activeNodesWithOldMaster: []string{"old", "new", "replica"},
	}
	cs := clusterState("new", "old", "replica")
	injected := errors.New("DCS temporarily unavailable")
	var result *Switchover
	gomock.InOrder(
		mockDCS.EXPECT().SetMasterHost("new").Return("", injected),
		mockDCS.EXPECT().SetMasterHost("new").Return("new", nil),
		mockDCS.EXPECT().DeleteCurrentSwitchover().Return(nil),
		mockDCS.EXPECT().SetLastSwitchover(gomock.Any()).DoAndReturn(func(sw *Switchover) error {
			result = sw
			return nil
		}),
	)
	// The DCS active list may already have changed during semi-sync adjustment.
	// Resuming must use the original operation's hosts and target.

	// Act: writing the new master to DCS fails.
	err := app.performSwitchover(cs, []string{"old"}, &sw, "new")

	// Assert.
	require.ErrorIs(t, err, injected)
	require.Equal(t, switchoverSetMasterInDCS, app.switchoverProgress.phase)

	// Act: retry after DCS recovers.
	err = app.performSwitchover(cs, []string{"old"}, &sw, "new")

	// Assert.
	require.NoError(t, err)
	require.Equal(t, switchoverComplete, app.switchoverProgress.phase)

	// Act: record the successful result and remove the pending request.
	err = app.FinishSwitchover(&sw, nil)

	// Assert.
	require.NoError(t, err)
	require.Nil(t, app.switchoverProgress)
	require.NotNil(t, result)
	require.True(t, result.Result.Ok)
}

type switchoverTimingDCS struct{ dcs.DCS }

func (switchoverTimingDCS) Get(string, any) error { return dcs.ErrNotFound }

func TestSwitchoverLimitsOnlyApplyBeforeMySQLChanges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		phase      switchoverPhase
		runCount   int
		wantLimits bool
	}{
		{name: "before freeze", phase: switchoverOptimization, runCount: 2, wantLimits: true},
		{name: "first attempt after freeze", phase: switchoverFreeze},
		{name: "retries after freeze", phase: switchoverFreeze, runCount: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			ctrl := gomock.NewController(t)
			cfg := minConfig()
			cfg.SwitchoverMaxAttempts = 2
			cfg.SwitchoverTimeout = time.Minute
			app := newTestApp(t, cfg, NewMockIAppDCS(ctrl))
			sw := testSwitchover()
			sw.RunCount = tc.runCount
			cs := map[string]*nodestate.NodeState{"old": aliveReplica(), "new": aliveReplica()}
			now := sw.InitiatedAt.Add(time.Hour)
			app.switchoverProgress = &switchoverProgress{request: sw, phase: tc.phase, oldMaster: "old"}

			// Act.
			timedOut := app.switchoverTimedOut(&sw, now)
			err := app.approveSwitchover(&sw, []string{"old", "new"}, cs)

			// Assert: starting MySQL changes, even on the first attempt, disables the limits.
			require.Equal(t, tc.wantLimits, timedOut)
			if tc.wantLimits {
				require.ErrorContains(t, err, "switchover_max_attempts")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFinishSwitchoverCannotRejectStartedRequest(t *testing.T) {
	// Arrange.
	ctrl := gomock.NewController(t)
	app := newTestApp(t, minConfig(), NewMockIAppDCS(ctrl))
	sw := testSwitchover()
	app.switchoverProgress = &switchoverProgress{request: sw, phase: switchoverFreeze, oldMaster: "old"}
	// No DeleteCurrentSwitchover or result-writing calls are allowed here.

	// Act.
	err := app.FinishSwitchover(&sw, errors.New("timeout"))

	// Assert.
	require.ErrorContains(t, err, "cannot reject switchover")
}

func TestGetMasterForPendingSwitchover(t *testing.T) {
	for _, phase := range []switchoverPhase{switchoverTurnReplicas, switchoverSetMasterInDCS} {
		t.Run(phase.String(), func(t *testing.T) {
			// Arrange.
			ctrl := gomock.NewController(t)
			app := newTestApp(t, minConfig(), NewMockIAppDCS(ctrl))
			sw := testSwitchover()
			app.switchoverProgress = &switchoverProgress{
				request: sw, phase: phase, oldMaster: "old", newMaster: "new",
			}
			cs := map[string]*nodestate.NodeState{
				"old": aliveReplica(), "new": aliveReplica(), "replica": aliveReplica(),
			}
			if phase == switchoverSetMasterInDCS {
				cs = clusterState("new", "old", "replica")
			}
			// Neither master discovery nor updating DCS is allowed mid-transition.

			// Act.
			master, err := app.getMasterForSwitchover(cs, &sw)

			// Assert.
			require.NoError(t, err)
			require.Equal(t, "old", master)
		})
	}
}

func TestSwitchoverProgressDoesNotSurviveManagerStateTransitions(t *testing.T) {
	for _, state := range []appState{stateCandidate, stateLost, stateMaintenance} {
		t.Run(string(state), func(t *testing.T) {
			// Arrange.
			ctrl := gomock.NewController(t)
			mockDCS := NewMockIAppDCS(ctrl)
			app := newTestApp(t, minConfig(), mockDCS)
			app.state = stateManager
			sw := testSwitchover()
			sw.To = ""
			sw.RunCount = 1
			progress := &switchoverProgress{
				request: sw, phase: switchoverPromote, oldMaster: "old", newMaster: "stale-candidate",
			}
			app.switchoverProgress = progress
			cs := clusterState("current-master")
			mockDCS.EXPECT().GetMasterHostFromDcs().Return("current-master", nil)

			// Act: stay in Manager.
			app.setState(stateManager)

			// Assert.
			require.Same(t, progress, app.switchoverProgress)

			// Act: leave Manager.
			app.setState(state)

			// Assert.
			require.Nil(t, app.switchoverProgress)

			// Arrange: simulate stale progress before reentering Manager.
			// Reentering Manager must also discard any stale progress, even for
			// the same request, and read the current master from DCS again.
			app.switchoverProgress = progress

			// Act.
			app.setState(stateManager)
			master, err := app.getMasterForSwitchover(cs, &sw)

			// Assert.
			require.Nil(t, app.switchoverProgress)
			require.NoError(t, err)
			require.Equal(t, "current-master", master)
		})
	}
}

func TestCalcActiveNodesChangesExcludesMasterWithReplicaState(t *testing.T) {
	// Arrange.
	app := &App{cluster: &mysql.Cluster{}}
	cs := map[string]*nodestate.NodeState{
		"old": aliveReplica(),
		"new": aliveReplica(),
	}
	cs["new"].SemiSyncState = &nodestate.SemiSyncState{SlaveEnabled: true}
	active := []string{"old", "new"}

	// Act.
	becomeActive, becomeInactive, _, err := app.calcActiveNodesChanges(cs, active, active, "old")

	// Assert.
	require.NoError(t, err)
	require.Empty(t, becomeActive)
	require.Empty(t, becomeInactive)
}

func TestUpdateActiveNodesRejectsReplicaAsMaster(t *testing.T) {
	// Arrange.
	app := &App{cluster: &mysql.Cluster{}}
	cs := map[string]*nodestate.NodeState{"old": aliveReplica()}
	// No MySQL node is configured: failure must occur before SQL mutations.

	// Act.
	err := app.updateActiveNodes(cs, cs, []string{"old"}, "old")

	// Assert.
	require.ErrorContains(t, err, "no live master state")
}

func TestSwitchoverProgressMatchesRequestAcrossRetries(t *testing.T) {
	// Arrange: retry metadata changes, but the request identity stays the same.
	sw := testSwitchover()
	p := &switchoverProgress{request: sw}
	sw.RunCount++
	sw.StartedAt = time.Now()
	sw.StartedBy = "manager"
	sw.Result = &SwitchoverResult{Error: "transient error"}

	// Act.
	matches := p.matches(&sw)

	// Assert.
	require.True(t, matches)

	// Arrange: change the request identity.
	sw.InitiatedAt = sw.InitiatedAt.Add(time.Second)

	// Act.
	matches = p.matches(&sw)

	// Assert.
	require.False(t, matches)
}

type deadlineOptimizationController struct {
	OptimizationController
	waits int
}

func (*deadlineOptimizationController) Enable(optimization.Node) error { return nil }

func (c *deadlineOptimizationController) Wait(context.Context, optimization.Node) error {
	c.waits++
	return optimization.ErrDeadlineExceeded
}

func TestOptimizationTimeoutRejectsOnlyNewSwitchover(t *testing.T) {
	// Arrange: optimization times out before any MySQL topology changes.
	ctrl := gomock.NewController(t)
	mockDCS := NewMockIAppDCS(ctrl)
	cfg := minConfig()
	cfg.SemiSync = true
	cfg.OptimizationConfig.LowReplicationMark = time.Minute
	app := newTestApp(t, cfg, mockDCS)
	app.cluster = &mysql.Cluster{}
	app.dcs = switchoverTimingDCS{}
	controller := &deadlineOptimizationController{}
	app.optController = controller
	sw := testSwitchover()
	cs := clusterState("old", "new")
	cs["new"].SlaveState.ReplicationLag = util.Ptr(500.0)
	app.switchoverProgress = &switchoverProgress{request: sw, oldMaster: "old"}
	var rejected *Switchover
	mockDCS.EXPECT().DeleteCurrentSwitchover().Return(nil)
	mockDCS.EXPECT().SetLastRejectedSwitchover(gomock.Any()).DoAndReturn(func(result *Switchover) error {
		rejected = result
		return nil
	})

	// Act.
	err := app.optimizationPhase([]string{"old", "new"}, &sw, "old", cs)

	// Assert.
	require.ErrorIs(t, err, optimization.ErrDeadlineExceeded)
	require.Equal(t, 1, controller.waits)
	require.Nil(t, app.switchoverProgress)
	require.NotNil(t, rejected)
	require.False(t, rejected.Result.Ok)
	require.Equal(t, "turbo mode exceeded deadline", rejected.Result.Error)

	// Arrange: the same request has already changed replication sources.
	app.switchoverProgress = &switchoverProgress{request: sw, phase: switchoverTurnReplicas, oldMaster: "old"}
	cs["old"] = aliveReplica()

	// Act.
	err = app.optimizationPhase([]string{"old", "new"}, &sw, "old", cs)

	// Assert: turbo must not wait again or reject the started request.
	require.NoError(t, err)
	require.Equal(t, 1, controller.waits)
	require.NotNil(t, app.switchoverProgress)
}

func TestSwitchoverResumeStillRequiresQuorum(t *testing.T) {
	// Arrange.
	ctrl := gomock.NewController(t)
	cfg := minConfig()
	cfg.SemiSync = true
	cfg.RplSemiSyncMasterWaitForSlaveCount = 1
	app := newTestApp(t, cfg, NewMockIAppDCS(ctrl))
	sw := testSwitchover()
	app.switchoverProgress = &switchoverProgress{
		request: sw, phase: switchoverTurnReplicas, oldMaster: "old", newMaster: "new",
		activeNodesWithOldMaster: []string{"old", "new", "replica"},
		frozenActiveNodes:        []string{"old", "new", "replica"},
	}
	cs := map[string]*nodestate.NodeState{
		"old": {}, "new": aliveReplica(), "replica": {},
	}
	// No cluster is configured: quorum must be checked before changing MySQL.

	// Act.
	err := app.performSwitchover(cs, nil, &sw, "old")

	// Assert.
	require.ErrorContains(t, err, "no quorum")
	require.Equal(t, switchoverTurnReplicas, app.switchoverProgress.phase)
}
