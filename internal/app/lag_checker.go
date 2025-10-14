package app

import (
	"context"
	"time"
)

const (
	prefix = "lag check"
)

// separated gorutine for checking local mysql lag
func (app *App) replicationLagChecker(ctx context.Context) {
	// 30s
	// TODO: we should use 2 tikers - fast (recovery, ticker, etc) and slow - lag check
	ticker := time.NewTicker(6 * app.config.RecoveryCheckInterval)
	for {
		select {
		case <-ticker.C:
			app.checkReplicationLag()
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) checkReplicationLag() {
	if app.doesResetupFileExist() {
		app.logger.Infof("%s: resetup file exists, waiting for resetup to complete", prefix)
		return
	}

	localNode := app.cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		app.logger.Errorf("%s: host %s failed to get slave status %v", prefix, localNode.Host(), err)
		return
	}

	// definitely not a replica
	if sstatus == nil {
		return
	}

	var master string
	err = app.dcs.Get(pathMasterNode, &master)
	if err != nil {
		app.logger.Errorf("%s: failed to get current master from dcs: %v", prefix, err)
		return
	}

	// probably not a replica, we need to wait
	if master == localNode.Host() {
		return
	}

	lag := sstatus.GetReplicationLag()
	// We have another problems, or small lag
	if !lag.Valid || lag.Float64 <= app.config.ResetupHostLag.Seconds() {
		return
	}

	// so, we clearly have replica with large lag
	ifOffline, err := localNode.IsOffline()
	if err != nil {
		app.logger.Errorf("%s: failed to check local node if offline: %v", prefix, err)
		return
	}

	masterNode := app.cluster.Get(master)
	masterRO, _, err := masterNode.IsReadOnly()
	if err != nil {
		app.logger.Errorf("%s: failed to check master (%s) if RO: %v", prefix, master, err)
		return
	}

	if ifOffline && !masterRO {
		app.logger.Infof("%s: local host set to resetup, because ReplicationLag (%f s) > ResetupHostLag (%v)",
			prefix, lag.Float64, app.config.ResetupHostLag.Seconds())

		app.writeResetupFile()
	}
}
