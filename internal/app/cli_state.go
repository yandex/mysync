package app

import (
	"fmt"
	"gopkg.in/yaml.v2"
)

// CliState print state of the cluster to the stdout
func (app *App) CliState(short bool) int {
  cancel, err := app.cliInitApp()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer cancel()

	clusterState := app.getClusterStateFromDB()
	var tree interface{}
	if short {
		clusterStateStrings := make(map[string]string)
		for host, state := range clusterState {
			clusterStateStrings[host] = state.String()
		}
		tree = clusterStateStrings
	} else {
		tree = clusterState
	}
	data, err := yaml.Marshal(tree)
	if err != nil {
		app.logger.Errorf("failed to marshal yaml: %v", err)
		return 1
	}
	fmt.Print(string(data))
	return 0
}

