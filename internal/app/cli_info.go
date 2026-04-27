package app

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"gopkg.in/yaml.v2"

	"github.com/yandex/mysync/internal/dcs"
)

type lagFilter struct {
	op    byte
	value float64
}

// CliInfo is CLI command printing information from DCS to the stdout
func (app *App) CliInfo(short bool, zone string, lag string, hostFilter string) int {
	cancel, err := app.cliInitApp()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer cancel()

	parsedLag, err := parseLagFilter(lag)
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}

	var tree any
	if short {
		tree, err = app.buildShortInfo(zone, hostFilter, parsedLag)
		if err != nil {
			app.logger.Error(err.Error())
			return 1
		}
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

func (app *App) buildShortInfo(zone string, hostFilter string, parsedLag *lagFilter) (map[string]any, error) {
	data := make(map[string]any)

	haNodes, err := app.cluster.GetClusterHAHostsFromDcs()
	if err != nil {
		return nil, fmt.Errorf("failed to get ha nodes: %v", err)
	}
	data[pathHANodes] = filterHostsMap(haNodes, zone, hostFilter, app.config.OfflineModeAZSeparator)

	cascadeNodes, err := app.cluster.GetClusterCascadeHostsFromDcs()
	if err != nil {
		return nil, fmt.Errorf("failed to get cascade nodes: %v", err)
	}
	data[pathCascadeNodesPrefix] = filterHostsMap(cascadeNodes, zone, hostFilter, app.config.OfflineModeAZSeparator)

	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		return nil, err
	}
	activeNodes = filterHostsByFilters(activeNodes, zone, hostFilter, app.config.OfflineModeAZSeparator)
	sort.Strings(activeNodes)
	data[pathActiveNodes] = activeNodes

	nodesOnRecovery, err := app.GetHostsOnRecovery()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes on recovery: %v", err)
	}
	if nodesOnRecovery != nil {
		nodesOnRecovery = filterHostsByFilters(nodesOnRecovery, zone, hostFilter, app.config.OfflineModeAZSeparator)
		if len(nodesOnRecovery) > 0 {
			sort.Strings(nodesOnRecovery)
			data[pathRecovery] = nodesOnRecovery
		}
	}

	clusterState, err := app.getClusterStateFromDcs()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster state: %v", err)
	}

	var master string
	err = app.dcs.Get(pathMasterNode, &master)
	if err != nil && err != dcs.ErrNotFound {
		return nil, fmt.Errorf("failed to get %s: %v", pathMasterNode, err)
	}

	health := make([]string, 0, len(clusterState))
	if state, ok := clusterState[master]; ok && filterHostsArray(master, zone, hostFilter, app.config.OfflineModeAZSeparator) && matchesLagFilter(state, parsedLag) {
		health = append(health, fmt.Sprintf("===> %q: %s", master, state.String()))
	}
	healthHosts := make([]string, 0, len(clusterState))
	for host, state := range clusterState {
		if host == master {
			continue
		}
		if !filterHostsArray(host, zone, hostFilter, app.config.OfflineModeAZSeparator) {
			continue
		}
		if !matchesLagFilter(state, parsedLag) {
			continue
		}
		healthHosts = append(healthHosts, host)
	}
	sort.Strings(healthHosts)
	for _, host := range healthHosts {
		health = append(health, fmt.Sprintf("     %q: %s", host, clusterState[host].String()))
	}
	data[pathHealthPrefix] = health

	for _, path := range []string{pathLastSwitch, pathCurrentSwitch, pathLastRejectedSwitch} {
		var switchover Switchover
		err = app.dcs.Get(path, &switchover)
		if err == nil {
			data[path] = switchover.String()
		} else if err != dcs.ErrNotFound {
			return nil, fmt.Errorf("failed to get %s: %v", path, err)
		}
	}

	var maintenance Maintenance
	err = app.dcs.Get(pathMaintenance, &maintenance)
	if err == nil {
		data[pathMaintenance] = maintenance.String()
	} else if err != dcs.ErrNotFound {
		return nil, fmt.Errorf("failed to get %s: %v", pathMaintenance, err)
	}

	var manager dcs.LockOwner
	err = app.dcs.Get(pathManagerLock, &manager)
	if err != nil && err != dcs.ErrNotFound {
		return nil, fmt.Errorf("failed to get %s: %v", pathManagerLock, err)
	}
	data[pathManagerLock] = manager.Hostname
	data[pathMasterNode] = master

	return data, nil
}

func filterHostsByFilters(hosts []string, zone string, hostFilter string, separator string) []string {
	if zone == "" && hostFilter == "" {
		return hosts
	}
	filtered := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if filterHostsArray(host, zone, hostFilter, separator) {
			filtered = append(filtered, host)
		}
	}
	return filtered
}

func filterHostsMap[T any](hosts map[string]T, zone string, hostFilter string, separator string) map[string]T {
	if zone == "" && hostFilter == "" {
		return hosts
	}
	filtered := make(map[string]T)
	for host, value := range hosts {
		if filterHostsArray(host, zone, hostFilter, separator) {
			filtered[host] = value
		}
	}
	return filtered
}

func filterHostsArray(host string, zone string, hostFilter string, separator string) bool {
	if zone != "" && getAvailabilityZone(host, separator) != zone {
		return false
	}
	if hostFilter != "" && !strings.Contains(host, hostFilter) {
		return false
	}
	return true
}

// Simple parser for expressions like ">10 or <10"
func parseLagFilter(raw string) (*lagFilter, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if len(raw) < 2 {
		return nil, fmt.Errorf("invalid lag filter %q: expected format >10 or <10", raw)
	}
	op := raw[0]
	if op != '>' && op != '<' {
		return nil, fmt.Errorf("invalid lag filter %q: expected format >10 or <10", raw)
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw[1:]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid lag filter %q: %v", raw, err)
	}
	return &lagFilter{op: op, value: value}, nil
}

func matchesLagFilter(state *nodestate.NodeState, filter *lagFilter) bool {
	if filter == nil {
		return true
	}
	if state == nil || state.SlaveState == nil {
		// if we meet master, its replication lag considered as 0
		if state.SlaveState == nil {
			return filter.op == '<'
		}
		return filter.op == '>'
	}

	lag := *state.SlaveState.ReplicationLag
	if filter.op == '>' {
		return lag > filter.value
	}
	return lag < filter.value
}
