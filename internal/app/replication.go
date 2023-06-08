package app

import (
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/mysql"
)

type RepairReplicationAlgorithm func(app *App, node *mysql.Node, master string) error

type ReplicationRepairAlgorithmType int

const (
	StartSlave ReplicationRepairAlgorithmType = iota
	ResetSlave
)

type ReplicationRepairState struct {
	LastAttempt time.Time
	History     map[ReplicationRepairAlgorithmType]int
}

func (app *App) MarkReplicationRunning(node *mysql.Node) {
	var replState *ReplicationRepairState
	if state, ok := app.replRepairState[node.Host()]; ok {
		replState = state
	} else {
		return
	}

	if replState.cooldownPassed(app.config.ReplicationRepairCooldown) {
		delete(app.replRepairState, node.Host())
	}
}

func (app *App) TryRepairReplication(node *mysql.Node, master string) {
	replState := app.getOrCreateHostRepairState(node.Host())

	if !replState.cooldownPassed(app.config.ReplicationRepairCooldown) {
		return
	}

	algorithmType, count, err := app.getSuitableAlgorithmType(replState)
	if err != nil {
		app.logger.Errorf("repair error: host %s, %v", node.Host(), err)
		return
	}

	algorithm := getRepairAlgorithm(algorithmType)
	err = algorithm(app, node, master)
	if err != nil {
		app.logger.Errorf("repair error: %v", err)
	}

	replState.History[algorithmType] = count + 1
	replState.LastAttempt = time.Now()
}

func StartSlaveAlgorithm(app *App, node *mysql.Node, _ string) error {
	app.logger.Infof("repair: trying to repair replication using StartSlaveAlgorithm...")
	return node.StartSlave()
}

func ResetSlaveAlgorithm(app *App, node *mysql.Node, master string) error {
	app.logger.Infof("repair: trying to repair replication using ResetSlaveAlgorithm...")
	app.logger.Infof("repair: executing set slave offline")
	err := node.SetOffline()
	if err != nil {
		return err
	}

	app.logger.Infof("repair: executing set slave readonly")
	err = node.SetReadOnly(true)
	if err != nil {
		return err
	}

	app.logger.Infof("repair: executing stop slave")
	err = node.StopSlave()
	if err != nil {
		return err
	}

	app.logger.Infof("repair: executing reset slave all")
	err = node.ResetSlaveAll()
	if err != nil {
		return err
	}

	app.logger.Infof("repair: executing change master on slave")
	err = node.ChangeMaster(master)
	if err != nil {
		return err
	}

	app.logger.Infof("repair: executing start slave")
	err = node.StartSlave()
	if err != nil {
		return err
	}

	return nil
}

func (app *App) getSuitableAlgorithmType(state *ReplicationRepairState) (ReplicationRepairAlgorithmType, int, error) {
	for i := range app.getAlgorithmOrder() {
		algorithmType := ReplicationRepairAlgorithmType(i)
		count := state.History[algorithmType]
		if count < app.config.ReplicationRepairMaxAttempts {
			return algorithmType, count, nil
		}
	}

	return 0, 0, fmt.Errorf("we have tried everything, but we have failed")
}

func (state *ReplicationRepairState) cooldownPassed(replicationRepairCooldown time.Duration) bool {
	cooldown := time.Now().Add(-replicationRepairCooldown)

	return state.LastAttempt.Before(cooldown)
}

func (app *App) getOrCreateHostRepairState(host string) *ReplicationRepairState {
	var replState *ReplicationRepairState
	if state, ok := app.replRepairState[host]; ok {
		replState = state
	} else {
		replState = app.createRepairState()
		app.replRepairState[host] = replState
	}

	return replState
}

func (app *App) createRepairState() *ReplicationRepairState {
	result := ReplicationRepairState{
		LastAttempt: time.Now(),
		History:     make(map[ReplicationRepairAlgorithmType]int),
	}

	for i := range app.getAlgorithmOrder() {
		result.History[ReplicationRepairAlgorithmType(i)] = 0
	}

	return &result
}

var defaultOrder = []ReplicationRepairAlgorithmType{
	StartSlave,
}

var aggressiveOrder = []ReplicationRepairAlgorithmType{
	StartSlave,
	ResetSlave,
}

func (app *App) getAlgorithmOrder() []ReplicationRepairAlgorithmType {
	if app.config.ReplicationRepairAggressiveMode {
		return aggressiveOrder
	} else {
		return defaultOrder
	}
}

var mapping = map[ReplicationRepairAlgorithmType]RepairReplicationAlgorithm{
	StartSlave: StartSlaveAlgorithm,
	ResetSlave: ResetSlaveAlgorithm,
}

func getRepairAlgorithm(algoType ReplicationRepairAlgorithmType) RepairReplicationAlgorithm {
	return mapping[algoType]
}
