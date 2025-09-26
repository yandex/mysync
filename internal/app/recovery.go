package app

import (
	"context"
	"time"
)

// separated gorutine for resetuping local mysql
func (app *App) recoveryChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.RecoveryCheckInterval)
	for {
		select {
		case <-ticker.C:
			app.checkRecovery()
			app.checkCrashRecovery()
			app.SetResetupStatus()
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) SetResetupStatus() {
	err := app.setResetupStatus(app.cluster.Local().Host(), app.doesResetupFileExist())
	if err != nil {
		app.logger.Errorf("recovery: failed to set resetup status: %v", err)
	}
}

func (app *App) checkRecovery() {
	if !app.IsRecoveryNeeded(app.config.Hostname) {
		return
	}
	if app.doesResetupFileExist() {
		app.logger.Infof("recovery: resetup file exists, waiting for resetup to complete")
		return
	}

	localNode := app.cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get slave status %v", localNode.Host(), err)
		return
	}

	// Old master may be stuck on 'Waiting for semi-sync'
	oldMasterStuck, err := localNode.IsWaitingSemiSyncAck()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get stuck processes %v", localNode.Host(), err)
	}
	if oldMasterStuck {
		app.logger.Errorf("recovery: old master %s has stuck processes", localNode.Host())
	}

	if sstatus == nil && !oldMasterStuck {
		app.logger.Info("recovery: waiting for manager to turn us to a new master")
		return
	}

	var master string
	err = app.dcs.Get(pathMasterNode, &master)
	if err != nil {
		app.logger.Errorf("recovery: failed to get current master from dcs: %v", err)
		return
	}
	err = app.cluster.UpdateHostsInfo()
	if err != nil {
		app.logger.Errorf("recovery: updating hosts info failed due: %s", err)
		return
	}
	masterNode := app.cluster.Get(master)
	mgtids, err := masterNode.GTIDExecutedParsed()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get master status %v", masterNode, err)
		return
	}

	if oldMasterStuck && master != localNode.Host() {
		// double-check master is stuck
		oldMasterStuck, err := localNode.IsWaitingSemiSyncAck()
		if err != nil {
			app.logger.Errorf("recovery: host %s failed to double-check stuck processes %v", localNode.Host(), err)
			return
		}
		if !oldMasterStuck {
			return
		}

		app.logger.Infof("recovery: new master is found %s, and current node is stuck. Writing resetup file", master)
		app.writeResetupFile("")
		return
	}

	app.logger.Infof("recovery: master %s has GTIDs %s", master, mgtids)
	app.logger.Infof("recovery: local node %s has GTIDs %s", localNode.Host(), sstatus.GetExecutedGtidSet())

	if isSlavePermanentlyLost(sstatus, mgtids) {
		rp, err := localNode.GetReplicaStatus()
		if err == nil {
			if rp.GetLastError() != "" {
				app.logger.Errorf("recovery: local node %s has error: %s", localNode.Host(), rp.GetLastError())
			}
			if rp.GetLastIOError() != "" {
				app.logger.Errorf("recovery: local node %s has IO error: %s", localNode.Host(), rp.GetLastIOError())
			}
		} else {
			app.logger.Errorf("recovery: local node %s is NOT behind the master %s, need RESETUP", localNode.Host(), masterNode)
		}
		app.writeResetupFile("")
	} else {
		readOnly, _, err := localNode.IsReadOnly()
		if err != nil {
			app.logger.Errorf("recovery: failed to check if host is read-only: %v", err)
			return
		}

		if !readOnly {
			app.logger.Errorf("recovery: host is not read-only, we should wait for it...")
			return
		}

		app.logger.Infof("recovery: local node %s is not ahead of master, recovery finished", localNode.Host())
		err = app.ClearRecovery(app.config.Hostname)
		if err != nil {
			app.logger.Errorf("recovery: failed to clear recovery flag in zk: %v", err)
		}
	}
}

func (app *App) checkCrashRecovery() {
	if !app.config.ResetupCrashedHosts {
		return
	}
	if app.doesResetupFileExist() {
		app.logger.Infof("recovery: resetup file exists, waiting for resetup to complete")
		return
	}
	ds, err := app.getLocalDaemonState()
	if err != nil {
		app.logger.Errorf("recovery: failed to get local daemon state: %v", err)
		return
	}
	if !ds.StartTime.IsZero() {
		app.logger.Debugf("recovery: daemon state: start time: %s", ds.StartTime)
	}
	if !ds.RecoveryTime.IsZero() {
		app.logger.Debugf("recovery: daemon state: crash recovery time: %s", ds.RecoveryTime)
	}
	if !ds.CrashRecovery {
		return
	}
	localNode := app.cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get slave status %v", localNode.Host(), err)
		return
	}
	if sstatus == nil {
		app.logger.Info("recovery: resetup after crash recovery may happen only on replicas")
		return
	}
	app.logger.Errorf("recovery: local node %s is running after crash recovery %v, need RESETUP", localNode.Host(), ds.RecoveryTime)
	app.writeResetupFile("")
}
