package app

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v2"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

// CliHostList prints list of managed HA/cascade hosts
func (app *App) CliHostList() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	app.dcs.Initialize()
	defer app.dcs.Close()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.cluster.Close()

	data := make(map[string]interface{})

	haNodes, err := app.cluster.GetClusterHAFqdnsFromDcs()
	if err != nil {
		app.logger.Errorf("failed to get ha nodes: %v", err)
		return 1
	}
	sort.Strings(haNodes)
	data[pathHANodes] = haNodes

	cascadeNodes, err := app.cluster.GetClusterCascadeHostsFromDcs()
	if err != nil {
		app.logger.Errorf("failed to get cascade nodes: %v", err)
		return 1
	}
	data[pathCascadeNodesPrefix] = cascadeNodes

	out, err := yaml.Marshal(data)
	if err != nil {
		app.logger.Errorf("failed to marshal yaml: %v", err)
		return 1
	}
	fmt.Print(string(out))
	return 0
}

// CliHostAdd add hosts to the list of managed HA/cascade hosts
func (app *App) CliHostAdd(host string, streamFrom *string, priority *int64, dryRun bool, skipMySQLCheck bool) int {
	err := validatePriority(priority)
	if err != nil {
		fmt.Println(err.Error())
		app.logger.Error(err.Error())
		return 1
	}

	err = app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.cluster.Close()

	// root paths may not exist
	err = app.dcs.Create(dcs.JoinPath(pathHANodes), nil)
	if err != nil && err != dcs.ErrExists {
		return 1
	}
	err = app.dcs.Create(dcs.JoinPath(pathCascadeNodesPrefix), nil)
	if err != nil && err != dcs.ErrExists {
		return 1
	}

	err = app.cluster.UpdateHostsInfo()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}

	ok, err := pingMysql(skipMySQLCheck, app.cluster, host)
	if err != nil {
		app.logger.Errorf("failed to check connection to %q: %v, can't tell if it's alive", host, err)
		return 1
	}
	if !ok {
		app.logger.Errorf("host %q is inaccessible, please start it before adding to the cluster", host)
		return 1
	}

	changes := false

	if streamFrom != nil {
		changesNew, err := app.processReplicationSource(*streamFrom, dryRun, host, skipMySQLCheck)
		if err != nil {
			return 1
		}

		changes = changes || changesNew
	} else {
		// node may not exist, we should add it to HA hosts
		if !app.cluster.IsHAHost(host) && !app.cluster.IsCascadeHost(host) {
			err = app.dcs.Set(dcs.JoinPath(pathHANodes, host), mysql.NodeConfiguration{Priority: 0})
			if err != nil && err != dcs.ErrExists {
				return 1
			}
		}
	}

	if priority != nil {
		changesNew, err := app.processPriority(*priority, dryRun, host)
		if err != nil {
			return 1
		}

		changes = changes || changesNew
	}

	if dryRun {
		if !changes {
			fmt.Println("dry run finished: no changes detected")
			return 0
		}
		return 2
	}

	fmt.Println("host has been added")
	return 0
}

// CliHostRemove removes hosts from the list of managed HA/cascade hosts
func (app *App) CliHostRemove(host string) int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.cluster.Close()

	ok, err := app.cluster.PingNode(host)
	if err != nil {
		app.logger.Errorf("failed to check connection to %q: %v, can't tell if it's alive", host, err)
		return 1
	}
	if ok {
		app.logger.Errorf("host %q is still accessible, please stop it before removing from the cluster", host)
		return 1
	}
	err = app.dcs.Delete(dcs.JoinPath(pathHANodes, host))
	if err != nil && err != dcs.ErrNotFound {
		return 1
	}
	err = app.dcs.Delete(dcs.JoinPath(pathCascadeNodesPrefix, host))
	if err != nil && err != dcs.ErrNotFound {
		return 1
	}
	err = app.dcs.Delete(dcs.JoinPath(pathResetupStatus, host))
	if err != nil && err != dcs.ErrNotFound {
		return 1
	}
	fmt.Println("host has been removed")
	return 0
}

func pingMysql(skipMySQLCheck bool, cluster *mysql.Cluster, host string) (bool, error) {
	if skipMySQLCheck {
		return true, nil
	}
	return cluster.PingNode(host)
}

func (app *App) processReplicationSource(streamFrom string, dryRun bool, host string,
	skipMySQLCheck bool) (changes bool, err error) {
	if streamFrom == "" {
		if dryRun {
			if app.cluster.IsHAHost(host) {
				fmt.Printf("dry run: node is already in HA-group\n")
				return false, nil
			}
			fmt.Printf("dry run: replica can be added to HA-group\n")
			return true, nil
		}
		err := app.dcs.Delete(dcs.JoinPath(pathCascadeNodesPrefix, host))
		if err != nil && err != dcs.ErrNotFound {
			return false, err
		}
		err = app.dcs.Set(dcs.JoinPath(pathHANodes, host), mysql.NodeConfiguration{Priority: 0})
		if err != nil && err != dcs.ErrExists {
			return false, err
		}
	} else {
		var master string
		err = app.dcs.Get(pathMasterNode, &master)
		if err != nil {
			return false, err
		}
		if master == host {
			fmt.Printf("Master host cannot be converted to cascade replica.\nIf you really want to convert this host to cascade one execute\n   mysync switch --from %s\n", master)
			return false, fmt.Errorf("master host cannot be converted to cascade replica")
		}
		if host == streamFrom {
			fmt.Printf("source and destination hosts are the same\n")
			return false, fmt.Errorf("source and destination hosts are the same")
		}

		ok, err := pingMysql(skipMySQLCheck, app.cluster, streamFrom)
		if err != nil {
			app.logger.Errorf("failed to check connection to %q: %v, can't tell if it's alive", streamFrom, err)
			return false, err
		}
		if !ok {
			err = fmt.Errorf("host %q is inaccessible, please start it before adding it as stream_from source", streamFrom)
			app.logger.Error(err.Error())
			return false, err
		}

		if dryRun {
			var cns mysql.CascadeNodeConfiguration
			err = app.dcs.Get(dcs.JoinPath(pathCascadeNodesPrefix, host), &cns)
			if err != nil && err != dcs.ErrNotFound {
				return false, err
			}
			if cns.StreamFrom == streamFrom {
				fmt.Printf("dry run: node is already streaming from %s\n", streamFrom)
				return false, nil
			}
			fmt.Printf("dry run: replica can be set cascade\n")
			return true, nil
		}

		err = app.dcs.Delete(dcs.JoinPath(pathHANodes, host))
		if err != nil && err != dcs.ErrNotFound {
			return false, err
		}
		err = app.dcs.Set(dcs.JoinPath(pathCascadeNodesPrefix, host), mysql.CascadeNodeConfiguration{StreamFrom: streamFrom})
		if err != nil && err != dcs.ErrExists {
			return false, err
		}
	}

	return true, nil
}

func (app *App) processPriority(priority int64, dryRun bool, host string) (changes bool, err error) {
	if dryRun {
		var nc mysql.NodeConfiguration
		err = app.dcs.Get(dcs.JoinPath(pathHANodes, host), &nc)
		if err != nil {
			if err != dcs.ErrNotFound {
				return false, err
			}
			fmt.Printf("dry run: node %s is not HA node, priority cannot be set", host)
			return false, fmt.Errorf("dry run: node %s is not HA node, priority cannot be set", host)
		}
		if nc.Priority == priority {
			fmt.Printf("dry run: node already has priority %d set\n", priority)
			return false, nil
		}
		fmt.Printf("dry run: node priority can be set to %d (current priority %d)\n", priority, nc.Priority)
		return true, nil
	}

	if app.cluster.IsCascadeHost(host) {
		fmt.Printf("node %s is not HA node, priority cannot be set", host)
		return false, nil
	}

	err = app.dcs.Set(dcs.JoinPath(pathHANodes, host), mysql.NodeConfiguration{Priority: priority})
	if err != nil && err != dcs.ErrExists {
		return false, err
	}

	return true, nil
}
