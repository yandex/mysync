package app

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v2"

	"github.com/yandex/mysync/internal/dcs"
)

// CliInfo is CLI command printing information from DCS to the stdout
func (app *App) CliInfo(short bool) int {	
  cancel, err := app.cliInitApp()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer cancel()

	var tree interface{}
	if short {
		data := make(map[string]interface{})

		haNodes, err := app.cluster.GetClusterHAHostsFromDcs()
		if err != nil {
			app.logger.Errorf("failed to get ha nodes: %v", err)
			return 1
		}
		data[pathHANodes] = haNodes

		cascadeNodes, err := app.cluster.GetClusterCascadeHostsFromDcs()
		if err != nil {
			app.logger.Errorf("failed to get cascade nodes: %v", err)
			return 1
		}
		data[pathCascadeNodesPrefix] = cascadeNodes

		activeNodes, err := app.GetActiveNodes()
		if err != nil {
			app.logger.Error(err.Error())
			return 1
		}
		sort.Strings(activeNodes)
		data[pathActiveNodes] = activeNodes

		nodesOnRecovery, err := app.GetHostsOnRecovery()
		if err != nil {
			app.logger.Errorf("failed to get nodes on recovery: %v", err)
			return 1
		}
		if len(nodesOnRecovery) > 0 {
			sort.Strings(nodesOnRecovery)
			data[pathRecovery] = nodesOnRecovery
		}

		clusterState, err := app.getClusterStateFromDcs()
		if err != nil {
			app.logger.Errorf("failed to get cluster state: %v", err)
			return 1
		}
		health := make(map[string]interface{})
		for host, state := range clusterState {
			health[host] = state.String()
		}
		data[pathHealthPrefix] = health

		for _, path := range []string{pathLastSwitch, pathCurrentSwitch, pathLastRejectedSwitch} {
			var switchover Switchover
			err = app.dcs.Get(path, &switchover)
			if err == nil {
				data[path] = switchover.String()
			} else if err != dcs.ErrNotFound {
				app.logger.Errorf("failed to get %s: %v", path, err)
				return 1
			}
		}

		var maintenance Maintenance
		err = app.dcs.Get(pathMaintenance, &maintenance)
		if err == nil {
			data[pathMaintenance] = maintenance.String()
		} else if err != dcs.ErrNotFound {
			app.logger.Errorf("failed to get %s: %v", pathMaintenance, err)
			return 1
		}

		var manager dcs.LockOwner
		err = app.dcs.Get(pathManagerLock, &manager)
		if err != nil && err != dcs.ErrNotFound {
			app.logger.Errorf("failed to get %s: %v", pathManagerLock, err)
			return 1
		}
		data[pathManagerLock] = manager.Hostname

		var master string
		err = app.dcs.Get(pathMasterNode, &master)
		if err != nil && err != dcs.ErrNotFound {
			app.logger.Errorf("failed to get %s: %v", pathMasterNode, err)
			return 1
		}
		data[pathMasterNode] = master
		tree = data
	} else {
		tree, err = app.dcs.GetTree("")
		if err != nil {
			app.logger.Error(err.Error())
			return 1
		}
	}
	data, err := yaml.Marshal(tree)
	if err != nil {
		app.logger.Errorf("failed to marshal yaml: %v", err)
		return 1
	}
	fmt.Print(string(data))
	return 0
}

