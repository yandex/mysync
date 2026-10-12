package app

import (
	"fmt"
	"maps"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type failoverPosition struct {
	source    string
	executed  string
	retrieved string
}

type failoverProgress struct {
	master    string
	failedAt  time.Time
	changedAt time.Time
	positions map[string]failoverPosition
}

// checkFailoverReplication distinguishes running threads from transaction progress.
// It is only used after the master has been reported unavailable in DCS.
func (app *App) checkFailoverReplication(clusterState, clusterStateDcs map[string]*nodestate.NodeState, master string, now time.Time) error {
	running := countRunningHASlaves(clusterState)
	if running == 0 {
		app.failoverProgress = nil
		return nil
	}

	// An idle master can keep serving while disconnected from ZooKeeper. Only
	// override the all-replicas-running guard when its local health check has
	// actually reported a MySQL failure and the manager cannot reach it either.
	masterHealth := clusterStateDcs[master]
	if running == countHANodes(clusterState)-1 &&
		(clusterState[master].PingOk || masterHealth.PingOk || masterHealth.CheckBy != master || masterHealth.CheckAt.IsZero()) {
		app.failoverProgress = nil
		return fmt.Errorf("%d replicas are alive and running replication, seems zk problems", running)
	}

	positions := make(map[string]failoverPosition)
	for host, state := range clusterState {
		if !state.PingOk || state.IsCascade {
			continue
		}
		if host == master && state.MasterState != nil {
			positions[host] = failoverPosition{executed: gtids.ParseGtidSet(state.MasterState.ExecutedGtidSet).String()}
		} else if state.SlaveState != nil {
			positions[host] = failoverPosition{
				source:    state.SlaveState.MasterHost,
				executed:  gtids.ParseGtidSet(state.SlaveState.ExecutedGtidSet).String(),
				retrieved: gtids.ParseGtidSet(state.SlaveState.RetrievedGtidSet).String(),
			}
		}
	}

	p := app.failoverProgress
	failedAt := app.t.Get(NodeFailedAt, master)
	changed := p == nil || p.master != master || p.failedAt != failedAt || !maps.Equal(p.positions, positions)
	if changed {
		p = &failoverProgress{master: master, failedAt: failedAt, changedAt: now, positions: positions}
		app.failoverProgress = p
	}
	// Even with failover_delay=0, require another observation of the GTIDs.
	timeout := max(app.config.FailoverDelay, app.config.TickInterval)
	if changed || now.Sub(p.changedAt) < timeout {
		return fmt.Errorf("waiting for GTID progress to stop on master %s and HA replicas: remaining %v", master, max(0, timeout-now.Sub(p.changedAt)))
	}
	app.logger.Info().Msgf("approve failover: %d replicas report running, but GTIDs have not changed for %v", running, now.Sub(p.changedAt))
	return nil
}
