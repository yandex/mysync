package optimization

import (
	"os"

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
	DisableNodeOptimization(master, node NodeReplicationController, dcs dcs.DCS) error

	// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
	// using the master for context. This is a bulk operation with immediate effects,
	// and it carries risks similar to disabling a single node.
	// Returns an error if disabling any node fails.
	DisableAllNodeOptimization(master NodeReplicationController, dcs dcs.DCS, nodes ...NodeReplicationController) error
}

func NewOptimizationModule(
	logger log.ILogger,
	config config.OptimizationConfig,
) *OptimizationModule {
	return &OptimizationModule{
		logger: logger,
		config: config,
	}
}

type OptimizationModule struct {
	logger log.ILogger
	config config.OptimizationConfig
}

// OptimizationStatus represents the state of an optimization process:
// - It starts as Pending (e.g., needs to retry/submit a command).
// - If the submission succeeds , it becomes Enabled.
// - If it fails, it becomes Disabled
type OptimizationStatus string

const (
	OptimizationStatusPending  OptimizationStatus = "pending"
	OptimizationStatusEnabled  OptimizationStatus = "enabled"
	OptimizationStatusDisabled OptimizationStatus = "disabled"
)

func (om *OptimizationModule) Initialize(DCS dcs.DCS) error {
	err := DCS.Create(pathOptimizationNodes, struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (om *OptimizationModule) EnableNodeOptimization(node NodeReplicationController, DCS dcs.DCS) error {
	err := DCS.Create(dcs.JoinPath(pathOptimizationNodes, node.Host()), struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (om *OptimizationModule) DisableNodeOptimization(master, node NodeReplicationController, DCS dcs.DCS) error {
	masterRS, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}
	return om.disableOptimizationOnNodeAndDcs(node, masterRS, DCS)
}

// BLUEPRINT: return array of errors
func (om *OptimizationModule) DisableAllNodeOptimization(master NodeReplicationController, DCS dcs.DCS, nodes ...NodeReplicationController) error {
	om.logger.Debug("optimization: disable all node optimization")

	hostnames, err := DCS.GetChildren(pathOptimizationNodes)
	if err != nil {
		for _, node := range nodes {
			hostnames = append(hostnames, node.Host())
		}
		om.logger.Warnf("optimization: DCS unreachable; optimization on the following hosts are going to be disabled: %v", hostnames)
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
		if !ok {
			om.logger.Warnf("optimization: host %s has not been found in the provided node list; it is expected to be disabled eventually", host)
			continue
		}
		err = om.disableOptimizationOnNodeAndDcs(node, rs, DCS)
		if err != nil {
			om.logger.Warnf("optimization: optimization mode on host %s has not been disabled: %s", host, err)
		}
	}

	return nil
}

func (om *OptimizationModule) IsLocalNodeOptimizing() bool {
	optimizationStatus := om.readOptimizationFile()
	if optimizationStatus == OptimizationStatusEnabled {
		return true
	}
	return false
}

func (om *OptimizationModule) SyncLocalOptimizationSettings(master, node NodeReplicationController, DCS dcs.DCS) error {
	if node.Host() == master.Host() {
		om.logger.Debugf("optimization: skipping local sync on master host [%s]", master.Host())
		return nil
	}

	optimizationStatus := om.readOptimizationFile()
	isOptimizingDcs, err := om.isHostOptimizingDcs(node.Host(), DCS)
	if err != nil {
		// DCS may be unreachable for an indefinite period (e.g., due to network issues).
		// To prioritize safety, default to disabling optimization until stability is restored.
		om.logger.Warnf("optimization: DCS unreachable for host [%s], defaulting to safe (non-optimizing) state: %v", node.Host(), err)
		isOptimizingDcs = false
	}
	om.logger.Debugf(
		"optimization: starting local sync for host [%s] (local status: %s, DCS: %t)",
		node.Host(),
		optimizationStatus,
		isOptimizingDcs,
	)

	switch {
	case isOptimizingDcs && optimizationStatus == OptimizationStatusEnabled:
		return om.phaseCheckOptimizationProgress(optimizationStatus, isOptimizingDcs, master, node, DCS)

	case isOptimizingDcs && optimizationStatus != OptimizationStatusEnabled:
		return om.phaseEnableOptimization(optimizationStatus, isOptimizingDcs, master, node, DCS)

	case !isOptimizingDcs && optimizationStatus != OptimizationStatusDisabled:
		return om.phaseDisableOptimization(optimizationStatus, isOptimizingDcs, master, node, DCS)
	}

	return nil
}

func (om *OptimizationModule) phaseCheckOptimizationProgress(
	optimizationStatus OptimizationStatus,
	isOptimizingDcs bool,
	master, node NodeReplicationController,
	DCS dcs.DCS,
) error {
	om.logger.Debugf("optimization: host [%s] is already optimizing", node.Host())

	lagUnderThreshold, currentLag, err := om.isNodeReplicationLagUnderThreshold(node)
	if err != nil {
		return err
	}
	om.logger.Debugf(
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

	err = om.removeDcsOptimizationHost(node.Host(), DCS)
	if err != nil {
		return err
	}

	DCS.ReleaseLock(pathOptimizationLock)

	return om.removeOptimizationFile()
}

func (om *OptimizationModule) phaseEnableOptimization(
	optimizationStatus OptimizationStatus,
	isOptimizingDcs bool,
	master, node NodeReplicationController,
	DCS dcs.DCS,
) error {
	om.logger.Debugf("optimization: host [%s] is going to start optimization", node.Host())

	lagUnderThreshold, currentLag, err := om.isNodeReplicationLagUnderThreshold(node)
	if err != nil {
		return err
	}
	om.logger.Debugf(
		"optimization: host [%s] status is (lag: %f, isOptimal: %t)",
		node.Host(), currentLag, lagUnderThreshold,
	)

	rs, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}

	if lagUnderThreshold || !rs.CanBeOptimized() {
		err = om.removeDcsOptimizationHost(node.Host(), DCS)
		if err != nil {
			return err
		}
		DCS.ReleaseLock(pathOptimizationLock)
		return om.removeOptimizationFile()
	}

	err = om.writeOptimizationFileStatus(OptimizationStatusPending)
	if err != nil {
		return err
	}

	acquired := DCS.AcquireLock(pathOptimizationLock)
	if !acquired {
		om.logger.Info("optimization: the node is ready to switch to optimization mode, but lock is already acquired")
		return nil
	}

	err = node.OptimizeReplication()
	if err != nil {
		return err
	}

	return om.writeOptimizationFileStatus(OptimizationStatusEnabled)
}

func (om *OptimizationModule) phaseDisableOptimization(
	optimizationStatus OptimizationStatus,
	isOptimizingDcs bool,
	master, node NodeReplicationController,
	DCS dcs.DCS,
) error {
	om.logger.Debugf("optimization: host [%s] is going to stop optimizing", node.Host())

	rs, err := master.GetReplicationSettings()
	if err != nil {
		return err
	}

	err = node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}

	DCS.ReleaseLock(pathOptimizationLock)

	return om.removeOptimizationFile()
}

func (om *OptimizationModule) writeOptimizationFileStatus(status OptimizationStatus) error {
	return os.WriteFile(om.config.Optimizationfile, []byte(status), 0644)
}

func (om *OptimizationModule) removeOptimizationFile() error {
	err := os.Remove(om.config.Optimizationfile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (om *OptimizationModule) removeDcsOptimizationHost(host string, DCS dcs.DCS) error {
	return DCS.Delete(dcs.JoinPath(pathOptimizationNodes, host))
}

func (om *OptimizationModule) readOptimizationFile() OptimizationStatus {
	fileContent, err := os.ReadFile(om.config.Optimizationfile)
	if err != nil {
		return OptimizationStatusDisabled
	}
	if len(fileContent) == 0 {
		return OptimizationStatusDisabled
	}
	return OptimizationStatus(fileContent)
}

func (om *OptimizationModule) isHostOptimizingDcs(host string, DCS dcs.DCS) (bool, error) {
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

func (om *OptimizationModule) isNodeReplicationLagUnderThreshold(node NodeReplicationController) (bool, float64, error) {
	replicationStatus, err := node.GetReplicaStatus()
	if err != nil {
		return false, 0.0, err
	}

	lag := replicationStatus.GetReplicationLag()
	if lag.Valid && lag.Float64 < om.config.OptimizeReplicationLagThreshold.Seconds() {
		return true, lag.Float64, nil
	}

	return false, lag.Float64, nil
}

// BLUEPRINT: return error on error
func (om *OptimizationModule) disableOptimizationOnNodeAndDcs(
	node NodeReplicationController,
	masterSettings mysql.ReplicationSettings,
	DCS dcs.DCS,
) error {
	om.logger.Debugf("optimization: disable optimization on %s", node.Host())
	err := DCS.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	if err != nil {
		om.logger.Errorf("optimization: cannot delete %s from dcs: %s:", node.Host(), err)
	}
	return node.SetReplicationSettings(masterSettings)
}
