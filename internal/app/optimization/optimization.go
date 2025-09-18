package optimization

import (
	"fmt"
	"os"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

const (
	// List of nodes running optimization mode. May be modified by external tools (e.g. add/remove node)
	// structure: pathOptimizationNodes/hostname -> nil
	pathOptimizationNodes = "optimization_nodes"

	// At most one node can be optimized simulatiniously.
	// structure: pathOptimizationLock
	pathOptimizationLock = "optimization_lock"
)

// NodeReplicationController is just a contract to have methods to control MySQL replication process.
type NodeReplicationController interface {
	SetReplicationSettings(rs mysql.ReplicationSettings) error
	GetReplicationSettings() (mysql.ReplicationSettings, error)
	OptimizeReplication() error

	GetReplicaStatus() (mysql.ReplicaStatus, error)
	Host() string
}

type ReplicationOpitimizer interface {
	// IsLocalNodeOptimizing shows whether local node is started optimizing or not.
	IsLocalNodeOptimizing() bool

	// Initialize initializes components
	Initialize(dcs dcs.DCS) error

	// SyncLocalOptimizationSettings synchronizes local optimization settings,
	// and applies replication adjustments if needed.
	// Returns an error if synchronization fails.
	SyncLocalOptimizationSettings(master, node NodeReplicationController, dcs dcs.DCS) error

	// EnableNodeOptimization activates optimization mode for the specified node.
	// The change may not take effect immediately (e.g., pending retries or submissions).
	// By default, only one optimized replica is allowed per setup to avoid data loss.
	// Returns an error if enabling fails (e.g., due to existing optimizations or channel closures).
	EnableNodeOptimization(node NodeReplicationController, dcs dcs.DCS) error

	// DisableNodeOptimization deactivates optimization mode for the specified node,
	// using the master for context (e.g., resetting replication settings).
	// Changes take effect immediately, as these options can be dangerous.
	// Returns an error if disabling fails.
	DisableNodeOptimization(master, node NodeReplicationController, dcs dcs.DCS, force bool) error

	// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
	// using the master for context. This is a bulk operation with immediate effects,
	// and it carries risks similar to disabling a single node.
	// Returns an error if disabling any node fails.
	DisableAllNodeOptimization(master NodeReplicationController, dcs dcs.DCS, force bool, nodes ...NodeReplicationController) error
}

func NewOptimizer(
	logger log.ILogger,
	config config.OptimizationConfig,
) *Optimizer {
	return &Optimizer{
		logger: logger,
		config: config,
	}
}

type Optimizer struct {
	logger   log.ILogger
	config   config.OptimizationConfig
	lastSync time.Time
}

// Status represents the state of an optimization process:
// - It starts as Pending (e.g., needs to retry/submit a command).
// - If the submission succeeds , it becomes Enabled.
// - If it fails, it becomes Disabled
type Status string

const (
	StatusPending  Status = "pending"
	StatusEnabled  Status = "enabled"
	StatusDisabled Status = "disabled"
)

func (opt *Optimizer) Initialize(DCS dcs.DCS) error {
	err := DCS.Create(pathOptimizationNodes, struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (opt *Optimizer) EnableNodeOptimization(node NodeReplicationController, DCS dcs.DCS) error {
	err := DCS.Create(dcs.JoinPath(pathOptimizationNodes, node.Host()), struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (opt *Optimizer) DisableNodeOptimization(master, node NodeReplicationController, DCS dcs.DCS, force bool) error {
	masterRS, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}
	return opt.disableOptimizationOnNodeAndDcs(node, masterRS, DCS, force)
}

func (opt *Optimizer) DisableAllNodeOptimization(
	master NodeReplicationController,
	DCS dcs.DCS,
	force bool,
	nodes ...NodeReplicationController,
) error {
	opt.logger.Debug("optimization: disable all node optimization")

	hostnames, err := opt.getOptimizationNodesHostnamesFromDCS(force, DCS, nodes...)
	if err != nil {
		return err
	}
	if len(hostnames) == 0 {
		return nil
	}

	hostnameToNode := map[string]NodeReplicationController{}
	for _, node := range nodes {
		hostnameToNode[node.Host()] = node
	}

	rs, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}
	for _, host := range hostnames {
		node, ok := hostnameToNode[host]
		if !ok && !force {
			opt.logger.Warnf("optimization: host %s has not been found in the provided node list; it is expected to be disabled eventually", host)
			continue
		}
		if !ok && force {
			return fmt.Errorf("host %s has not been found in the provided node list", host)
		}

		err := opt.disableOptimizationOnNodeAndDcs(node, rs, DCS, force)
		if err != nil {
			return err
		}
	}

	return nil
}

func (opt *Optimizer) getOptimizationNodesHostnamesFromDCS(
	force bool,
	DCS dcs.DCS,
	nodes ...NodeReplicationController,
) ([]string, error) {
	hostnames, err := DCS.GetChildren(pathOptimizationNodes)
	if err != nil && !force {
		return nil, err
	}
	if err != nil && force {
		for _, node := range nodes {
			hostnames = append(hostnames, node.Host())
		}
		opt.logger.Warnf("optimization: DCS unreachable; optimization on the following hosts are going to be disabled: %v", hostnames)
	}
	return hostnames, nil
}

func (opt *Optimizer) IsLocalNodeOptimizing() bool {
	status := opt.readOptimizationFile()
	return status == StatusEnabled
}

func (opt *Optimizer) SyncLocalOptimizationSettings(
	master,
	node NodeReplicationController,
	DCS dcs.DCS,
) error {
	now := time.Now()
	if now.Sub(opt.lastSync) < opt.config.SyncInterval {
		return nil
	}
	if node.Host() == master.Host() {
		opt.logger.Debugf("optimization: skipping local sync on master host [%s]", master.Host())
		return nil
	}

	opt.lastSync = now

	status := opt.readOptimizationFile()
	isOptimizingDcs, err := opt.isHostOptimizingDcs(node.Host(), DCS)
	if err != nil {
		// DCS may be unreachable for an indefinite period (e.g., due to network issues).
		// To prioritize safety, default to disabling optimization until stability is restored.
		opt.logger.Warnf("optimization: DCS unreachable for host [%s], defaulting to safe (non-optimizing) state: %v", node.Host(), err)
		isOptimizingDcs = false
	}
	opt.logger.Debugf(
		"optimization: starting local sync for host [%s] (local status: %s, DCS: %t)",
		node.Host(),
		status,
		isOptimizingDcs,
	)

	switch {
	case isOptimizingDcs && status == StatusEnabled:
		return opt.phaseCheckOptimizationProgress(master, node, DCS)

	case isOptimizingDcs && status != StatusEnabled:
		return opt.phaseEnableOptimization(node, DCS)

	case !isOptimizingDcs && status != StatusDisabled:
		return opt.phaseDisableOptimization(master, node, DCS)
	}

	return nil
}

func (opt *Optimizer) phaseCheckOptimizationProgress(
	master, node NodeReplicationController,
	DCS dcs.DCS,
) error {
	opt.logger.Debugf("optimization: host [%s] is already optimizing", node.Host())

	lagUnderThreshold, currentLag, err := opt.isNodeReplicationLagUnderThreshold(node)
	if err != nil {
		return err
	}
	opt.logger.Debugf(
		"optimization: host [%s] status is (lag: %f, isOptimal: %t)",
		node.Host(), currentLag, lagUnderThreshold,
	)
	if !lagUnderThreshold {
		return nil
	}

	rs, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}
	err = node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}

	err = opt.removeDcsOptimizationHost(node.Host(), DCS)
	if err != nil {
		return err
	}

	DCS.ReleaseLock(pathOptimizationLock)

	return opt.removeOptimizationFile()
}

func (opt *Optimizer) phaseEnableOptimization(
	node NodeReplicationController,
	DCS dcs.DCS,
) error {
	opt.logger.Debugf("optimization: host [%s] is going to start optimization", node.Host())

	lagUnderThreshold, currentLag, err := opt.isNodeReplicationLagUnderThreshold(node)
	if err != nil {
		return err
	}
	opt.logger.Debugf(
		"optimization: host [%s] status is (lag: %f, isOptimal: %t)",
		node.Host(), currentLag, lagUnderThreshold,
	)

	rs, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}

	if lagUnderThreshold || !rs.CanBeOptimized() {
		err = opt.removeDcsOptimizationHost(node.Host(), DCS)
		if err != nil {
			return err
		}
		DCS.ReleaseLock(pathOptimizationLock)
		return opt.removeOptimizationFile()
	}

	err = opt.writeOptimizationFileStatus(StatusPending)
	if err != nil {
		return err
	}

	acquired := DCS.AcquireLock(pathOptimizationLock)
	if !acquired {
		opt.logger.Info("optimization: the node is ready to switch to optimization mode, but lock is already acquired")
		return nil
	}

	err = node.OptimizeReplication()
	if err != nil {
		return err
	}

	return opt.writeOptimizationFileStatus(StatusEnabled)
}

func (opt *Optimizer) phaseDisableOptimization(
	master, node NodeReplicationController,
	DCS dcs.DCS,
) error {
	opt.logger.Debugf("optimization: host [%s] is going to stop optimizing", node.Host())

	rs, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}

	err = node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}

	DCS.ReleaseLock(pathOptimizationLock)

	return opt.removeOptimizationFile()
}

func (opt *Optimizer) writeOptimizationFileStatus(status Status) error {
	return os.WriteFile(opt.config.File, []byte(status), 0644)
}

func (opt *Optimizer) removeOptimizationFile() error {
	err := os.Remove(opt.config.File)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (opt *Optimizer) removeDcsOptimizationHost(host string, DCS dcs.DCS) error {
	return DCS.Delete(dcs.JoinPath(pathOptimizationNodes, host))
}

func (opt *Optimizer) readOptimizationFile() Status {
	fileContent, err := os.ReadFile(opt.config.File)
	if err != nil {
		return StatusDisabled
	}
	if len(fileContent) == 0 {
		return StatusDisabled
	}
	return Status(fileContent)
}

func (opt *Optimizer) isHostOptimizingDcs(host string, DCS dcs.DCS) (bool, error) {
	err := DCS.Get(dcs.JoinPath(pathOptimizationNodes, host), &struct{}{})
	if err == dcs.ErrNotFound {
		return false, nil
	}
	if err == dcs.ErrExists {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (opt *Optimizer) isNodeReplicationLagUnderThreshold(node NodeReplicationController) (bool, float64, error) {
	replicationStatus, err := node.GetReplicaStatus()
	if err != nil {
		return false, 0.0, err
	}

	lag := replicationStatus.GetReplicationLag()
	if lag.Valid && lag.Float64 < opt.config.ReplicationLagThreshold.Seconds() {
		return true, lag.Float64, nil
	}

	return false, lag.Float64, nil
}

func (opt *Optimizer) disableOptimizationOnNodeAndDcs(
	node NodeReplicationController,
	masterSettings mysql.ReplicationSettings,
	DCS dcs.DCS,
	force bool,
) error {
	opt.logger.Debugf("optimization: disable optimization on %s", node.Host())
	err := DCS.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	if err != nil && err != dcs.ErrNotFound {
		if force {
			opt.logger.Warnf("optimization: optimization mode on host %s has not been disabled on DCS: %s", node.Host(), err)
		} else {
			return err
		}
	}
	return node.SetReplicationSettings(masterSettings)
}
