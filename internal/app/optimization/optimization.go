package optimization

import (
	"context"
	"errors"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

const (
	// List of nodes which are going to run optimization mode. May be modified by external tools (e.g. add/remove node)
	// structure: pathOptimizationNodes/hostname -> nil
	pathOptimizationNodes = "optimization_nodes"
)

var errOptimizationWaitingDeadlineExceeded = errors.New("optimization waiting deadline exceeded")

// NodeReplicationController is just a contract to have methods to control MySQL replication process.
type NodeReplicationController interface {
	SetReplicationSettings(rs mysql.ReplicationSettings) error
	GetReplicationSettings() (mysql.ReplicationSettings, error)
	OptimizeReplication() error

	GetReplicaStatus() (mysql.ReplicaStatus, error)
	Host() string
}

type ReplicationOpitimizer interface {
	// Initialize initializes components
	// Must be called before any other method
	Initialize(dcs dcs.DCS) error

	// WaitOptimization blocks until node is optimized
	WaitOptimization(ctx context.Context, node NodeReplicationController, checkInterval time.Duration) error

	// SyncState synchronizes optimization settings,
	// and applies replication adjustments if needed.
	// Master can be nil. In that case, node will be returned to the most safest default replication settings.
	// Returns an error if synchronization fails.
	SyncState(
		master NodeReplicationController,
		nodeStates map[string]*nodestate.NodeState,
		nodes []NodeReplicationController,
	) error

	// EnableNodeOptimization activates optimization mode for the specified node.
	// The change may not take effect immediately (e.g., pending retries or submissions).
	// By default, only one optimized replica is allowed per setup to avoid data loss.
	// Returns an error if enabling fails (e.g., due to existing optimizations or channel closures).
	EnableNodeOptimization(node NodeReplicationController) error

	// DisableNodeOptimization deactivates optimization mode for the specified node,
	// using the master for context (e.g., resetting replication settings).
	// Changes take effect immediately, as these options can be dangerous.
	// Master can be nil. In that case, node will be returned to the most safest default replication settings.
	// Returns an error if disabling fails.
	DisableNodeOptimization(master, node NodeReplicationController) error

	// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
	// using the master for context. This is a bulk operation with immediate effects,
	// and it carries risks similar to disabling a single node.
	// Master can be nil. In that case, node will be returned to the most safest default replication settings.
	// Returns an error if disabling any node fails.
	DisableAllNodeOptimization(master NodeReplicationController, nodes ...NodeReplicationController) error
}

func NewOptimizer(
	logger log.ILogger,
	config config.OptimizationConfig,
) *Optimizer {
	return &Optimizer{
		logger: logger,
		config: config,
		policy: NewOneNodePolicy(config, logger),
	}
}

type Optimizer struct {
	logger log.ILogger
	config config.OptimizationConfig
	DCS    dcs.DCS
	policy Policy
}

// Status represents the state of an optimization process:
// - It starts as Pending
// - If the submission succeeds, it becomes Enabled.
// - If it optimized, it becomes Disabled.
type Status string

const (
	StatusNew     Status = ""
	StatusEnabled Status = "enabled"
)

func parseStatus(status string) Status {
	switch status {
	case string(StatusEnabled):
		return StatusEnabled
	}

	return StatusNew
}

func (opt *Optimizer) Initialize(DCS dcs.DCS) error {
	opt.DCS = DCS
	opt.logger.Info("Optimizer started initialization")

	err := DCS.Create(pathOptimizationNodes, "")
	if err != nil && err != dcs.ErrExists {
		return err
	}

	opt.logger.Info("Optimizer started policy initialization")
	err = opt.policy.Initialize(DCS)
	if err != nil {
		return err
	}
	opt.logger.Info("Optimizer policy is initialized")
	opt.logger.Info("Optimized is initialized")

	return err
}

func (opt *Optimizer) WaitOptimization(ctx context.Context, node NodeReplicationController, checkInterval time.Duration) error {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	maxConsequentErrors := 3
	consequentErrors := 0

	for {
		select {
		case <-ctx.Done():
			return errOptimizationWaitingDeadlineExceeded

		case <-ticker.C:
			isOptimal, err := opt.isOptimizedDuringWaiting(node)
			if err != nil {
				opt.logger.Errorf("optimization: waiting; err %s", err)
				consequentErrors += 1
			}
			if isOptimal {
				opt.logger.Infof("optimization: waiting is complete")
				return nil
			}
			if consequentErrors > maxConsequentErrors {
				return err
			}
		}
	}
}

func (opt *Optimizer) isOptimizedDuringWaiting(node NodeReplicationController) (bool, error) {
	exist, err := nodeExist(node, opt.DCS)
	if err != nil {
		return false, err
	}

	if exist {
		opt.logger.Info("optimization: waiting; node exists in DCS")
	} else {
		return true, nil
	}

	optimal, lag, err := isOptimal(node, opt.config.LowReplicationMark.Seconds())
	if err != nil {
		return false, err
	}
	if optimal {
		opt.logger.Errorf("optimization: waiting is complete, as replication lag was converged: %s", err)
		return true, opt.DCS.Delete(pathOptimizationNodes)
	}

	opt.logger.Errorf("optimization: waiting; replication lag is: %f", lag)
	return false, nil
}

func (opt *Optimizer) EnableNodeOptimization(node NodeReplicationController) error {
	opt.logger.Infof("optimization: enabling node [%s] optimization", node.Host())
	err := opt.DCS.Create(dcs.JoinPath(pathOptimizationNodes, node.Host()), "")
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (opt *Optimizer) DisableNodeOptimization(master, node NodeReplicationController) error {
	opt.logger.Infof("optimization: disabling node [%s] optimization", node.Host())
	rs, err := master.GetReplicationSettings()
	if err != nil {
		opt.logger.Errorf("cannot acquire master replication settings %s", err)
		rs = mysql.SafeReplicationSettings
	}
	return disableHostWithDCS(node, rs, opt.DCS)
}

func (opt *Optimizer) DisableAllNodeOptimization(
	master NodeReplicationController,
	nodes ...NodeReplicationController,
) error {
	opt.logger.Info("optimization: disabling all nodes optimization")

	rs, err := master.GetReplicationSettings()
	if err != nil {
		opt.logger.Errorf("cannot acquire master replication settings %s", err)
		rs = mysql.SafeReplicationSettings
	}

	hostnames, err := opt.DCS.GetChildren(pathOptimizationNodes)
	if err != nil {
		return err
	}

	m := makeHostToNodeMap(nodes...)
	for _, hostname := range hostnames {
		opt.logger.Infof("optimization: disabling node [%s] optimization", hostname)
		err := disableHostWithDCS(m[hostname], rs, opt.DCS)
		if err != nil {
			return err
		}
	}

	return nil
}

func (opt *Optimizer) getStatuses() (map[string]Status, error) {
	hostnames, err := opt.DCS.GetChildren(pathOptimizationNodes)
	if err != nil {
		return nil, err
	}

	statuses := map[string]Status{}
	for _, hostname := range hostnames {
		path := dcs.JoinPath(pathOptimizationNodes, hostname)

		var status string
		err := opt.DCS.Get(path, &status)
		if err != nil && err != dcs.ErrNotFound && err != dcs.ErrMalformed {
			return nil, err
		}
		if err != nil && (err == dcs.ErrNotFound || err == dcs.ErrMalformed) {
			status = ""
		}

		statuses[hostname] = parseStatus(status)
	}

	return statuses, nil
}

func (opt *Optimizer) SyncState(
	master NodeReplicationController,
	nodeStates map[string]*nodestate.NodeState,
	nodes []NodeReplicationController,
) error {
	statuses, err := opt.getStatuses()
	if err != nil {
		return err
	}

	hostToNode := makeHostToNodeMap(nodes...)

	return opt.policy.Apply(
		master,
		nodeStates,
		statuses,
		hostToNode,
	)
}

func makeHostToNodeMap(nodes ...NodeReplicationController) map[string]NodeReplicationController {
	m := map[string]NodeReplicationController{}
	for _, node := range nodes {
		m[node.Host()] = node
	}
	return m
}
