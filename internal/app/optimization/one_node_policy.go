package optimization

import (
	"math/rand"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

type OneNodePolicy struct {
	DCS    dcs.DCS
	config config.OptimizationConfig
	logger log.ILogger
}

func NewOneNodePolicy(
	cfg config.OptimizationConfig,
	logger log.ILogger,
) *OneNodePolicy {
	return &OneNodePolicy{
		config: cfg,
		logger: logger,
	}
}

func (onp *OneNodePolicy) Initialize(DCS dcs.DCS) error {
	onp.DCS = DCS
	onp.logger.Info("OneNodePolicy is Initialized")
	return nil
}

func (onp *OneNodePolicy) Apply(
	master NodeReplicationController,
	nodeStates map[string]*nodestate.NodeState,
	dcsStatuses map[string]Status,
	nodes map[string]NodeReplicationController,
) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}

	dcsStatuses, err = onp.disableAlreadyOptimizedHosts(master, dcsStatuses, nodeStates, nodes, rs)
	if err != nil {
		return err
	}

	hostname, err := onp.makeSureAtMostOneNodeIsOptimizing(dcsStatuses, nodes, rs)
	if err != nil {
		return err
	}
	if hostname == "" {
		return onp.chooseHostAndOptimizeIt(dcsStatuses, nodes)
	}

	node := nodes[hostname]
	optimal, lag, err := isOptimal(node, onp.config.LowReplicationMark.Seconds())
	if err != nil {
		return err
	}
	if optimal {
		onp.logger.Infof("optimization (OneNodePolicy): node [%s] is optimized", hostname)
		return disableHostWithDCS(nodes[hostname], rs, onp.DCS)
	}
	onp.logger.Infof("optimization (OneNodePolicy): node [%s] is optimizing and has lag %f", hostname, lag)

	rs, err = node.GetReplicationSettings()
	if err != nil {
		return err
	}

	if rs.CanBeOptimized() {
		onp.logger.Infof("optimization (OneNodePolicy): node [%s] is not optimizing; something went wrong, recovering optimization process...", hostname)
		err = node.OptimizeReplication()
		if err != nil {
			return err
		}
	}

	return nil
}

func (onp *OneNodePolicy) chooseHostAndOptimizeIt(
	dcsStatuses map[string]Status,
	nodes map[string]NodeReplicationController,
) error {
	for hostname := range dcsStatuses {
		onp.logger.Infof("optimization (OneNodePolicy): host [%s] started optimization", hostname)
		return enableHost(nodes[hostname], onp.DCS)
	}
	return nil
}

func (onp *OneNodePolicy) makeSureAtMostOneNodeIsOptimizing(
	dcsStatuses map[string]Status,
	nodes map[string]NodeReplicationController,
	rs mysql.ReplicationSettings,
) (string, error) {
	enabledHosts := filterEnabledStatus(dcsStatuses)

	if len(enabledHosts) == 0 {
		onp.logger.Infof("optimization (OneNodePolicy): there are no enabled hosts")
		return "", nil
	}

	if len(enabledHosts) == 1 {
		onp.logger.Infof("optimization (OneNodePolicy): one host [%s] is optimizing", enabledHosts[0])
		return enabledHosts[0], nil
	}

	onp.logger.Infof("optimization (OneNodePolicy): several hosts %+v are enabled; we have to remain only one of them", enabledHosts)
	rand.Shuffle(len(enabledHosts), func(i, j int) {
		enabledHosts[i], enabledHosts[j] = enabledHosts[j], enabledHosts[i]
	})

	for _, hostname := range enabledHosts[1:] {
		onp.logger.Infof("optimization (OneNodePolicy): disabling host [%s]", hostname)
		err := disableHost(nodes[hostname], rs)
		if err != nil {
			return "", err
		}
	}

	return "", nil
}

func (onp *OneNodePolicy) disableAlreadyOptimizedHosts(
	master NodeReplicationController,
	dcsStatuses map[string]Status,
	nodeStates map[string]*nodestate.NodeState,
	nodes map[string]NodeReplicationController,
	rs mysql.ReplicationSettings,
) (map[string]Status, error) {
	for hostname, status := range dcsStatuses {
		isMaster := hostname == master.Host()
		isSlaveLost := nodeStates[hostname].SlaveState == nil || nodeStates[hostname].SlaveState.ReplicationLag == nil
		isEnabled := status == StatusEnabled
		isConverged := !isSlaveLost && *nodeStates[hostname].SlaveState.ReplicationLag < onp.config.LowReplicationMark.Seconds()
		isCompletelyConverged := !isSlaveLost && *nodeStates[hostname].SlaveState.ReplicationLag < onp.config.LowReplicationMark.Seconds()

		if isMaster || isSlaveLost ||
			(isConverged && !isEnabled) ||
			(isCompletelyConverged && isEnabled) {
			err := disableHostWithDCS(nodes[hostname], rs, onp.DCS)
			if err != nil {
				return nil, err
			}
			delete(dcsStatuses, hostname)
		}
	}

	return dcsStatuses, nil
}

func filterEnabledStatus(statuses map[string]Status) []string {
	var hostnames []string
	for hostname, dcsStatus := range statuses {
		if dcsStatus == StatusEnabled {
			hostnames = append(hostnames, hostname)
		}
	}
	return hostnames
}
