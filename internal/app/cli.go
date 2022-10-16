package app

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

// CliInfo is CLI command printing information from DCS to the stdout
func (app *App) CliInfo(short bool) int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	app.dcs.Initialize()
	defer app.dcs.Close()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.cluster.Close()
	if err := app.cluster.UpdateHostsInfo(); err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

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
			app.logger.Errorf(err.Error())
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
			app.logger.Errorf(err.Error())
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

// CliState print state of the cluster to the stdout
func (app *App) CliState(short bool) int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()
	err = app.newDBCluster()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.cluster.Close()

	if err := app.cluster.UpdateHostsInfo(); err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

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

// CliSwitch performs manual switch-over of the master node
// nolint: gocyclo
func (app *App) CliSwitch(switchFrom, switchTo string, waitTimeout time.Duration) int {
	ctx := app.baseContext()
	if switchFrom == "" && switchTo == "" {
		app.logger.Errorf("Either --from or --to should be set")
		return 1
	}
	if switchFrom != "" && switchTo != "" {
		app.logger.Errorf("Option --from and --to can't be used in the same time")
		return 1
	}
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()
	err = app.newDBCluster()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.cluster.Close()

	if err := app.cluster.UpdateHostsInfo(); err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

	if len(app.cluster.HANodeHosts()) == 1 {
		app.logger.Info("switchover has not sense on single HA-node cluster")
		fmt.Println("switchover done")
		return 0
	}

	var fromHost, toHost string

	var currentMaster string
	if err := app.dcs.Get(pathMasterNode, &currentMaster); err != nil {
		app.logger.Errorf("failed to get current master: %v", err)
		return 1
	}
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

	if switchTo != "" {
		// switch to particular host
		desired := util.SelectNodes(app.cluster.HANodeHosts(), switchTo)
		if len(desired) == 0 {
			app.logger.Errorf("no HA-nodes matching '%s'", switchTo)
			return 1
		}
		if len(desired) > 1 {
			app.logger.Errorf("two or more nodes matching '%s'", switchTo)
			return 1
		}
		toHost = desired[0]
		if toHost == currentMaster {
			app.logger.Infof("master is already on %s, skipping...", toHost)
			fmt.Println("switchover done")
			return 0
		}
		if !util.ContainsString(activeNodes, toHost) {
			app.logger.Errorf("%s is not active, can't switch to it", toHost)
			return 1
		}
	} else {
		// switch away from specified host(s)
		notDesired := util.SelectNodes(app.cluster.HANodeHosts(), switchFrom)
		if len(notDesired) == 0 {
			app.logger.Errorf("no HA-nodes matches '%s', check --from param", switchFrom)
			return 1
		}
		if !util.ContainsString(notDesired, currentMaster) {
			app.logger.Infof("master is already not on %v, skipping...", notDesired)
			fmt.Println("switchover done")
			return 0
		}
		var candidates []string
		for _, node := range activeNodes {
			if !util.ContainsString(notDesired, node) {
				candidates = append(candidates, node)
			}
		}
		if len(candidates) == 0 {
			app.logger.Errorf("there are no active nodes, not matching '%s'", switchFrom)
			return 1
		}
		if len(notDesired) == 1 {
			fromHost = notDesired[0]
		} else {
			// there are multiple hosts matching --from pattern
			// to avoid switching from one to another, use switch to behaviour
			positions, err := app.getNodePositions(candidates)
			if err != nil {
				app.logger.Errorf(err.Error())
				return 1
			}
			toHost, err = getMostDesirableNode(app.logger, positions, app.config.PriorityChoiceMaxLag)
			if err != nil {
				app.logger.Errorf(err.Error())
				return 1
			}
		}
	}

	var switchover Switchover
	err = app.dcs.Get(pathCurrentSwitch, &switchover)
	if err == nil {
		app.logger.Errorf("Another switchover in progress %v", switchover)
		return 2
	}
	if err != dcs.ErrNotFound {
		app.logger.Errorf(err.Error())
		return 2
	}

	switchover.From = fromHost
	switchover.To = toHost
	switchover.InitiatedBy = app.config.Hostname
	switchover.InitiatedAt = time.Now()
	switchover.Cause = CauseManual

	err = app.dcs.Create(pathCurrentSwitch, switchover)
	if err == dcs.ErrExists {
		app.logger.Error("Another switchover in progress")
		return 2
	}
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	// wait for switchover to complete
	if waitTimeout > 0 {
		var lastSwitchover Switchover
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				lastSwitchover = app.getLastSwitchover()
				if lastSwitchover.InitiatedBy == switchover.InitiatedBy && lastSwitchover.InitiatedAt.Unix() == switchover.InitiatedAt.Unix() {
					break Out
				} else {
					lastSwitchover = Switchover{}
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if lastSwitchover.Result == nil {
			app.logger.Error("could not wait for switchover to complete")
			return 1
		} else if !lastSwitchover.Result.Ok {
			app.logger.Error("could not wait for switchover to complete because of errors")
			return 1
		}
		fmt.Println("switchover done")
	} else {
		fmt.Println("switchover scheduled")
	}
	return 0
}

// CliEnableMaintenance enables maintenance mode
func (app *App) CliEnableMaintenance(waitTimeout time.Duration) int {
	ctx := app.baseContext()
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	maintenance := &Maintenance{
		InitiatedBy: app.config.Hostname,
		InitiatedAt: time.Now(),
	}
	err = app.dcs.Create(pathMaintenance, maintenance)
	if err != nil && err != dcs.ErrExists {
		app.logger.Errorf(err.Error())
		return 1
	}
	// wait for mysync to pause
	if waitTimeout > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				err = app.dcs.Get(pathMaintenance, maintenance)
				if err != nil {
					app.logger.Errorf(err.Error())
				}
				if maintenance.MySyncPaused {
					break Out
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if !maintenance.MySyncPaused {
			app.logger.Error("could not wait for mysync to enter maintenance")
			return 1
		}
		fmt.Println("maintenance enabled")
	} else {
		fmt.Println("maintenance scheduled")
	}
	return 0
}

// CliDisableMaintenance disables maintenance mode
func (app *App) CliDisableMaintenance(waitTimeout time.Duration) int {
	ctx := app.baseContext()
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	maintenance := &Maintenance{}
	err = app.dcs.Get(pathMaintenance, maintenance)
	if err == dcs.ErrNotFound {
		fmt.Println("maintenance disabled")
		return 0
	} else if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	maintenance.ShouldLeave = true
	err = app.dcs.Set(pathMaintenance, maintenance)
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	if waitTimeout > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				err = app.dcs.Get(pathMaintenance, maintenance)
				if err == dcs.ErrNotFound {
					maintenance = nil
					break Out
				}
				if err != nil {
					app.logger.Errorf(err.Error())
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if maintenance != nil {
			app.logger.Error("could not wait for mysync to leave maintenance")
			return 1
		}
		fmt.Println("maintenance disabled")
	} else {
		fmt.Println("maintenance disable scheduled")
	}
	return 0
}

// CliGetMaintenance prints on/off depending on current maintenance status
func (app *App) CliGetMaintenance() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.dcs.Get(pathMaintenance, new(Maintenance))
	if err == nil {
		fmt.Println("on")
		return 0
	} else if err == dcs.ErrNotFound {
		fmt.Println("off")
		return 0
	} else {
		app.logger.Error(err.Error())
		return 1
	}
}

// CliAbort cleans switchover node from DCS
func (app *App) CliAbort() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.dcs.Get(pathCurrentSwitch, new(Switchover))
	if err == dcs.ErrNotFound {
		fmt.Println("no active switchover")
		return 0
	}
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

	const phrase = "yes, abort switch"
	fmt.Printf("please, confirm aborting switchover by typing '%s'\n", phrase)
	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	if strings.TrimSpace(response) != phrase {
		fmt.Printf("doesn't match, do nothing")
		return 1
	}

	err = app.dcs.Delete(pathCurrentSwitch)
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}

	fmt.Printf("switchover aborted\n")
	return 0
}

// CliHostList prints list of managed HA/cascade hosts
func (app *App) CliHostList() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	app.dcs.Initialize()
	defer app.dcs.Close()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Errorf(err.Error())
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
		app.logger.Errorf(err.Error())
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
		app.logger.Errorf(err.Error())
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
		app.logger.Errorf(err.Error())
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
			fmt.Printf("Master host cannot be converted to cascade replica.\nIf you really whant to convert this host to cascade one execute\n   mysync switch --from %s\n", master)
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
