package app

import (
	"fmt"

	"github.com/yandex/mysync/internal/mysql"
)

// CliEnableOptimization enables optimization mode
func (app *App) CliEnableOptimization() int {
	cancel, err := app.cliInitApp()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	defer cancel()

	node := app.cluster.Local()
	status, err := app.cliGetHostOptimizationStatus(node)
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}

	if status == Optimizable {
		err = app.optimizationController.Enable(node)
		if err != nil {
			fmt.Printf("%s\n", err)
			return 1
		}
		fmt.Println("The host optimization has been started.")
		return 0
	}

	fmt.Printf("Can't optimize host in status '%s'\n", status)
	return 1
}

// CliDisableOptimization disables optimization mode
func (app *App) CliDisableOptimization() int {
	cancel, err := app.cliInitApp()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	defer cancel()

	master, err := app.GetMasterHostFromDcs()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	masterNode := app.cluster.Get(master)

	node := app.cluster.Local()
	err = app.optimizationController.Disable(masterNode, node)
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	return 0
}

// CliDisableOptimization disables optimization mode on all hosts
func (app *App) CliDisableAllOptimization() int {
	cancel, err := app.cliInitApp()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	defer cancel()

	master, err := app.GetMasterHostFromDcs()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}

	hosts := app.cluster.AllNodeHosts()
	var nodes []*mysql.Node
	for _, host := range hosts {
		nodes = append(nodes, app.cluster.Get(host))
	}

	err = app.optimizationController.DisableAll(
		app.cluster.Get(master),
		convertNodesToReplicationControllers(nodes),
	)
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	return 0
}

// CliGetOptimization gets optimization mode
func (app *App) CliGetOptimization() int {
	cancel, err := app.cliInitApp()
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	defer cancel()

	node := app.cluster.Local()
	status, err := app.cliGetHostOptimizationStatus(node)
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}

	fmt.Printf("The host is in status '%s'\n", status)

	return 0
}

func (app *App) cliGetHostOptimizationStatus(localNode *mysql.Node) (HostOptimizationStatus, error) {
	status, err := localNode.GetReplicaStatus()
	if err != nil {
		return Unknown, err
	}
	if status == nil {
		return HostRoleMaster, nil
	}

	replicationSettings, err := localNode.GetReplicationSettings()
	if err != nil {
		return Unknown, nil
	}
	if replicationSettings.CanBeOptimized() {
		return Optimizable, nil
	}

	masterFqdn, err := app.GetMasterHostFromDcs()
	if err != nil {
		return Unknown, nil
	}

	master := app.cluster.Get(masterFqdn)
	masterReplicationSettings, err := master.GetReplicationSettings()
	if err != nil {
		return Unknown, nil
	}

	if masterReplicationSettings.Equal(&replicationSettings) {
		return UnoptimizableConfiguration, nil
	}
	return OptimizationRunning, nil
}

type HostOptimizationStatus string

const (
	OptimizationRunning        HostOptimizationStatus = "optimization is running"
	Optimizable                HostOptimizationStatus = "can be optimized"
	UnoptimizableConfiguration HostOptimizationStatus = "configuration of the host is already optimized"
	HostRoleMaster             HostOptimizationStatus = "host is master"
	Unknown                    HostOptimizationStatus = "unknown"
)
