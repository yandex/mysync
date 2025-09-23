package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type RepairReplicationAlgorithm func(app *App, node *mysql.Node, master string, channel string) error

type ReplicationRepairAlgorithmType int

const (
	StartSlave ReplicationRepairAlgorithmType = iota
	ResetSlave
)

type ReplicationRepairState struct {
	LastAttempt      time.Time
	History          map[ReplicationRepairAlgorithmType]int
	LastGTIDExecuted string
}

func (app *App) MarkReplicationRunning(node *mysql.Node, channel string) {
	var replState *ReplicationRepairState
	key := app.makeReplStateKey(node, channel)
	if state, ok := app.replRepairState[key]; ok {
		replState = state
	} else {
		return
	}

	if replState.cooldownPassed(app.config.ReplicationRepairCooldown) {
		status, err := node.ReplicaStatusWithTimeout(app.config.DBTimeout, channel)
		if err != nil {
			return
		}

		newGtidSet := gtids.ParseGtidSet(status.GetExecutedGtidSet())
		oldGtidSet := gtids.ParseGtidSet(replState.LastGTIDExecuted)

		if gtids.IsSlaveAhead(newGtidSet, oldGtidSet) {
			delete(app.replRepairState, key)
		}
	}
}

func (app *App) TryRepairReplication(node *mysql.Node, master string, channel string) {
	replState, err := app.getOrCreateHostRepairState(app.makeReplStateKey(node, channel), node.Host(), channel)
	if err != nil {
		app.logger.Errorf("repair error: host %s, %v", node.Host(), err)
		return
	}

	if !replState.cooldownPassed(app.config.ReplicationRepairCooldown) {
		return
	}

	algorithmType, count, err := app.getSuitableAlgorithmType(replState)
	if err != nil {
		app.logger.Errorf("repair error: host %s, %v", node.Host(), err)
		return
	}

	algorithm := getRepairAlgorithm(algorithmType)
	err = algorithm(app, node, master, channel)
	if err != nil {
		app.logger.Errorf("repair error: %v", err)
	}

	replState.History[algorithmType] = count + 1
	replState.LastAttempt = time.Now()
}

func (app *App) makeReplStateKey(node *mysql.Node, channel string) string {
	if channel == app.config.ExternalReplicationChannel {
		return fmt.Sprintf("%s-%s", node.Host(), channel)
	}
	return node.Host()
}

func StartSlaveAlgorithm(app *App, node *mysql.Node, _ string, channel string) error {
	app.logger.Infof("repair: trying to repair replication using StartSlaveAlgorithm...")
	if channel == app.config.ExternalReplicationChannel {
		return app.externalReplication.Start(node)
	}
	return node.StartSlave()
}

func ResetSlaveAlgorithm(app *App, node *mysql.Node, master string, channel string) error {
	// TODO we don't want reset slave on external replication
	// May be we should split algorithms by channel type (ext/int)
	if channel == app.config.ExternalReplicationChannel {
		app.logger.Infof("external repair: don't want to use ResetSlaveAlgorithm, leaving")
		return nil
	}
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

func (app *App) getOrCreateHostRepairState(stateKey, hostname, channel string) (*ReplicationRepairState, error) {
	var replState *ReplicationRepairState
	if state, ok := app.replRepairState[stateKey]; ok {
		replState = state
	} else {
		var err error
		replState, err = app.createRepairState(hostname, channel)
		if err != nil {
			return nil, err
		}

		app.replRepairState[stateKey] = replState
	}

	return replState, nil
}

func (app *App) createRepairState(hostname, channel string) (*ReplicationRepairState, error) {
	status, err := app.cluster.Get(hostname).ReplicaStatusWithTimeout(app.config.DBTimeout, channel)
	if err != nil {
		return nil, err
	}

	result := ReplicationRepairState{
		LastAttempt:      time.Now(),
		History:          make(map[ReplicationRepairAlgorithmType]int),
		LastGTIDExecuted: status.GetExecutedGtidSet(),
	}

	for i := range app.getAlgorithmOrder() {
		result.History[ReplicationRepairAlgorithmType(i)] = 0
	}

	return &result, nil
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

func (app *App) optimizeReplicaWithSmallestLag(
	replicas []string,
	optionalDesirableReplica string,
) error {
	hostnameToOptimize, err := app.chooseReplicaToOptimize(optionalDesirableReplica, replicas)
	if err != nil {
		return err
	}
	replicaToOptimize := app.cluster.Get(hostnameToOptimize)

	err = app.replicationOptimizer.EnableNodeOptimization(replicaToOptimize)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), app.config.ReplicationConvergenceTimeoutSwitchover)
	defer cancel()

	return app.replicationOptimizer.WaitOptimization(ctx, replicaToOptimize, 3*time.Second)
}

func (app *App) chooseReplicaToOptimize(
	optionalDesirableReplica string,
	replicas []string,
) (string, error) {
	if len(optionalDesirableReplica) > 0 {
		return optionalDesirableReplica, nil
	}

	positions, err := app.getNodePositions(replicas)
	if err != nil {
		return "", err
	}

	hostnameToOptimize, err := app.getMostDesirableReplicaToOptimize(positions)
	if err != nil {
		return "", err
	}
	app.logger.Infof("replica optimization: the replica is '%s'", hostnameToOptimize)

	return hostnameToOptimize, nil
}

func (app *App) getMostDesirableReplicaToOptimize(positions []nodePosition) (string, error) {
	lagThreshold := app.config.OptimizationConfig.HighReplicationMark
	return getMostDesirableNode(app.logger, positions, lagThreshold)
}

func (app *App) optimizationPhase(activeNodes []string, switchover *Switchover, oldMaster string, clusterState map[string]*nodestate.NodeState) error {
	forceOptimizationStop := switchover.MasterTransition != SwitchoverTransition
	err := app.stopAllNodeOptimization(
		oldMaster,
		clusterState,
		forceOptimizationStop,
	)
	if err != nil {
		return err
	}

	if !app.switchHelper.IsOptimizationPhaseAllowed() {
		app.logger.Info("switchover: phase 0: turbo mode is skipped")
		return nil
	}

	appropriateReplicas := filterOut(activeNodes, []string{oldMaster, switchover.From})
	desirableReplica := switchover.To

	app.logger.Infof(
		"switchover: phase 0: enter turbo mode; replicas: %v, oldMaster: '%s', desirable replica: '%s'",
		appropriateReplicas,
		oldMaster,
		desirableReplica,
	)
	err = app.optimizeReplicaWithSmallestLag(
		appropriateReplicas,
		desirableReplica,
	)
	if err != nil && errors.Is(err, ErrOptimizationPhaseDeadlineExceeded) {
		app.logger.Infof("switchover: phase 0: turbo mode failed: %v", err)
		switchErr := app.FinishSwitchover(switchover, fmt.Errorf("turbo mode exceeded deadline"))
		if switchErr != nil {
			return fmt.Errorf("switchover: failed to reject switchover %s", switchErr)
		}
		app.logger.Info("switchover: rejected")
		return err
	}

	// Conceptually, we should only reject the switchover if we encounter a DeadlineExceeded error.
	// This indicates that the replica with the freshest data is too far from convergence,
	// and we can't optimize it within a limited time frame.
	// Other cases can be handled in subsequent steps, so no special action is needed here.
	app.logger.Info("switchover: phase 0: turbo mode is complete")
	return nil
}
