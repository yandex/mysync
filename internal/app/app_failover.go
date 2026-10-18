package app

import (
	"errors"
	"fmt"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type failoverProgress struct {
	master    string
	changedAt time.Time
	received  map[string]string
}

// checkFailoverReplication distinguishes running threads from transaction progress.
// It is only used after the master has been reported unavailable in DCS.
func (app *App) checkFailoverReplication(clusterState, clusterStateDcs map[string]*nodestate.NodeState, master string, previous *failoverProgress, now time.Time) (*failoverProgress, error) {
	running := countRunningHASlaves(clusterState)
	if running == 0 {
		return nil, nil
	}
	if clusterState[master].PingOk {
		return nil, fmt.Errorf("master %s is still reachable", master)
	}

	// An idle master can keep serving while disconnected from ZooKeeper. Only
	// override the all-replicas-running guard when its local health check has
	// recently reported a MySQL failure. Allow one health-check interval plus
	// the time needed for the next failed MySQL query.
	masterHealth := clusterStateDcs[master]
	healthAge := now.Sub(masterHealth.CheckAt)
	if running == countHANodes(clusterState)-1 &&
		(masterHealth.PingOk || masterHealth.CheckBy != master || masterHealth.CheckAt.IsZero() ||
			healthAge < 0 || healthAge > app.config.HealthCheckInterval+app.config.DBTimeout) {
		return nil, fmt.Errorf("%d replicas report running, but master %s has no fresh local failure report", running, master)
	}

	p := previous
	changed := p == nil || p.master != master
	if changed {
		p = &failoverProgress{master: master, received: make(map[string]string)}
	}
	// Applying relay logs is not evidence that the master is alive. Catch-up
	// of already received transactions is handled during the switchover.
	// Keep the last observation for unavailable replicas so flapping alone
	// does not restart the inactivity window.
	for host, state := range clusterState {
		if !state.PingOk || state.IsCascade || state.SlaveState == nil || state.SlaveState.MasterHost != master {
			continue
		}
		received := gtids.ParseGtidSet(state.SlaveState.RetrievedGtidSet).String()
		if last, ok := p.received[host]; !ok || last != received {
			p.received[host] = received
			changed = true
		}
	}

	if changed {
		p.changedAt = now
	}
	// Even with failover_delay=0, require another observation of the GTIDs.
	timeout := max(app.config.FailoverDelay, app.config.TickInterval)
	if changed || now.Sub(p.changedAt) < timeout {
		return p, fmt.Errorf("waiting for received GTIDs from master %s to stop changing: remaining %v", master, max(0, timeout-now.Sub(p.changedAt)))
	}
	app.logger.Info().Msgf("approve failover: %d replicas report running, but received GTIDs have not changed for %v", running, now.Sub(p.changedAt))
	return p, nil
}

func (app *App) approveFailover(clusterState, clusterStateDcs map[string]*nodestate.NodeState, activeNodes []string, master string) error {
	previous := app.failoverProgress
	app.failoverProgress = nil

	if !app.config.Failover {
		return fmt.Errorf("auto_failover is disabled in config")
	}
	afterCrashRecovery := clusterStateDcs[master].DaemonState != nil && clusterStateDcs[master].DaemonState.CrashRecovery && app.config.ResetupCrashedHosts
	if afterCrashRecovery {
		app.logger.Info().Msgf("approve failover: after crash recovery, skip replication and delay checks")
	} else if clusterStateDcs[master].IsFileSystemReadonly {
		app.logger.Info().Msgf("approve failover: filesystem is readonly, skip replication and delay checks")
	} else {
		var err error
		app.failoverProgress, err = app.checkFailoverReplication(clusterState, clusterStateDcs, master, previous, time.Now())
		if err != nil {
			return err
		}
		if app.config.FailoverDelay > 0 {
			failingTime := time.Since(app.t.Get(NodeFailedAt, master))
			if failingTime < app.config.FailoverDelay {
				return fmt.Errorf("failover delay is not yet elapsed: remaining %v", app.config.FailoverDelay-failingTime)
			}
		}
	}

	app.logger.Info().Msgf("approve failover: active nodes are %v", activeNodes)
	// number of active slaves that we can use to perform switchover
	permissibleSlaves := countAliveHASlavesWithinNodes(activeNodes, clusterState)
	err := app.switchHelper.CheckFailoverQuorum(activeNodes, permissibleSlaves)
	if err != nil {
		return err
	}

	lastSwitchover := new(Switchover)
	err = app.appDCS.GetLastSwitchover(lastSwitchover)
	if !errors.Is(err, dcs.ErrNotFound) {
		if err != nil {
			return err
		}
		if lastSwitchover.Result == nil {
			return fmt.Errorf("another switchover in progress. this should never happen")
		}
		timeAfterLastSwitchover := time.Since(lastSwitchover.Result.FinishedAt)
		if timeAfterLastSwitchover < app.config.FailoverCooldown && lastSwitchover.Cause == CauseAuto {
			return fmt.Errorf("not enough time from last failover %s (cooldown %s)", lastSwitchover.Result.FinishedAt, app.config.FailoverCooldown)
		}
	}
	return nil
}
