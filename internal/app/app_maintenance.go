package app

import "errors"

func (app *App) enterMaintenance(maintenance *Maintenance, master string) error {
	if app.config.DisableSemiSyncReplicationOnMaintenance {
		node := app.cluster.Get(master)
		err := node.SemiSyncDisable()
		if err != nil {
			return err
		}
		err = app.DeleteActiveNodes()
		if err != nil {
			return err
		}
	}
	maintenance.MySyncPaused = true
	return app.SetMaintenance(maintenance)
}

func (app *App) leaveMaintenance() error {
	err := app.cluster.UpdateHostsInfo()
	if err != nil {
		return err
	}
	clusterState := app.getClusterStateFromDB()
	master, err := app.ensureCurrentMaster(clusterState)
	if err != nil {
		if errors.Is(err, ErrManyMasters) {
			app.writeEmergeFile(err.Error())
		}
		return err
	}
	clusterStateDcs, err := app.getClusterStateFromDcs()
	if err != nil {
		return err
	}
	app.repairCluster(clusterState, clusterStateDcs, master)
	clusterState = app.getClusterStateFromDB()
	err = app.updateActiveNodes(clusterState, clusterStateDcs, []string{}, master)
	if err != nil {
		return err
	}
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		return err
	}
	if len(activeNodes) == 0 {
		return ErrNoActiveNodes
	}
	return app.DeleteMaintenance()
}
