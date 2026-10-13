package app

import (
	"fmt"
	"maps"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type failoverProgress struct {
	master    string
	changedAt time.Time
	received  map[string]string
}

// checkFailoverReplication distinguishes running threads from transaction progress.
// It is only used after the master has been reported unavailable in DCS.
func (app *App) checkFailoverReplication(clusterState, clusterStateDcs map[string]*nodestate.NodeState, master string, now time.Time) error {
	running := countRunningHASlaves(clusterState)
	if running == 0 {
		app.failoverProgress = nil
		return nil
	}
	if clusterState[master].PingOk {
		app.failoverProgress = nil
		return fmt.Errorf("master %s is still reachable", master)
	}

	// An idle master can keep serving while disconnected from ZooKeeper. Only
	// override the all-replicas-running guard when its local health check has
	// recently reported a MySQL failure. Allow one health-check interval plus
	// the time needed for its failed MySQL query.
	masterHealth := clusterStateDcs[master]
	healthAge := now.Sub(masterHealth.CheckAt)
	if running == countHANodes(clusterState)-1 &&
		(masterHealth.PingOk || masterHealth.CheckBy != master || masterHealth.CheckAt.IsZero() ||
			healthAge < 0 || healthAge > app.config.HealthCheckInterval+app.config.DBTimeout) {
		app.failoverProgress = nil
		return fmt.Errorf("%d replicas report running, but master %s has no fresh local failure report", running, master)
	}

	// Applying relay logs is not evidence that the master is alive. Catch-up
	// of already received transactions is handled during the switchover.
	received := make(map[string]string)
	for host, state := range clusterState {
		if !state.PingOk || state.IsCascade || state.SlaveState == nil || state.SlaveState.MasterHost != master {
			continue
		}
		received[host] = gtids.ParseGtidSet(state.SlaveState.RetrievedGtidSet).String()
	}

	p := app.failoverProgress
	changed := p == nil || p.master != master || p.changedAt.Before(app.t.Get(NodeFailedAt, master)) || !maps.Equal(p.received, received)
	if changed {
		p = &failoverProgress{master: master, changedAt: now, received: received}
		app.failoverProgress = p
	}
	// Even with failover_delay=0, require another observation of the GTIDs.
	timeout := max(app.config.FailoverDelay, app.config.TickInterval)
	if changed || now.Sub(p.changedAt) < timeout {
		return fmt.Errorf("waiting for received GTIDs from master %s to stop changing: remaining %v", master, max(0, timeout-now.Sub(p.changedAt)))
	}
	app.logger.Info().Msgf("approve failover: %d replicas report running, but received GTIDs have not changed for %v", running, now.Sub(p.changedAt))
	return nil
}
