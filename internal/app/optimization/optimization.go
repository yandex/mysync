package optimization

import (
	"errors"
	"fmt"
	"os"
	"reflect"
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

var errMasterIsNil = errors.New("master is nil")

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
	// Must be called before any other method
	Initialize(dcs dcs.DCS) error

	// SyncLocalOptimizationSettings synchronizes local optimization settings,
	// and applies replication adjustments if needed.
	// Master can be nil. In that case, node will be returned to the most safest default replication settings.
	// Returns an error if synchronization fails.
	SyncLocalOptimizationSettings(master, node NodeReplicationController) error

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
	// ignoreErrors makes ReplicationOpitimizer ignore all errors and try to disable optimization at least on mysql/DCS
	DisableNodeOptimization(master, node NodeReplicationController, ignoreErrors bool) error

	// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
	// using the master for context. This is a bulk operation with immediate effects,
	// and it carries risks similar to disabling a single node.
	// Master can be nil. In that case, node will be returned to the most safest default replication settings.
	// Returns an error if disabling any node fails.
	// ignoreErrors makes ReplicationOpitimizer ignore all errors and try to disable optimization at least on mysql/DCS
	// on as many hosts as it can
	DisableAllNodeOptimization(master NodeReplicationController, ignoreErrors bool, nodes ...NodeReplicationController) error
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
	DCS      dcs.DCS
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
	opt.DCS = DCS
	err := DCS.Create(pathOptimizationNodes, struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (opt *Optimizer) EnableNodeOptimization(node NodeReplicationController) error {
	err := opt.DCS.Create(dcs.JoinPath(pathOptimizationNodes, node.Host()), struct{}{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

func (opt *Optimizer) DisableNodeOptimization(master, node NodeReplicationController, ignoreErrors bool) error {
	masterRS, err := opt.getMasterReplicationSettings(master, ignoreErrors)
	if err != nil {
		return err
	}
	return opt.disableOptimizationOnNodeAndDcs(node, masterRS, ignoreErrors)
}

func (opt *Optimizer) DisableAllNodeOptimization(
	master NodeReplicationController,
	ignoreErrors bool,
	nodes ...NodeReplicationController,
) error {
	opt.logger.Debug("optimization: disable all node optimization")

	hostnames, err := opt.getOptimizationNodesHostnamesFromDCS(ignoreErrors, nodes...)
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

	rs, err := opt.getMasterReplicationSettings(master, ignoreErrors)
	if err != nil {
		return err
	}
	for _, host := range hostnames {
		node, ok := hostnameToNode[host]
		if !ok && !ignoreErrors {
			opt.logger.Warnf("optimization: host %s has not been found in the provided node list; it is expected to be disabled eventually", host)
			continue
		}
		if !ok && ignoreErrors {
			return fmt.Errorf("host %s has not been found in the provided node list", host)
		}

		err := opt.disableOptimizationOnNodeAndDcs(node, rs, ignoreErrors)
		if err != nil {
			return err
		}
	}

	return nil
}

func (opt *Optimizer) getOptimizationNodesHostnamesFromDCS(
	ignoreErrors bool,
	nodes ...NodeReplicationController,
) ([]string, error) {
	hostnames, err := opt.DCS.GetChildren(pathOptimizationNodes)
	if err != nil && !ignoreErrors {
		return nil, err
	}
	if err != nil && ignoreErrors {
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
	master, node NodeReplicationController,
) error {
	if !opt.shouldSync() {
		return nil
	}

	// Case when master is lost
	// we should not implicitly change dcs state, as the optimization can continue after recovery
	if master == nil || reflect.ValueOf(master).IsNil() {
		err := node.SetReplicationSettings(mysql.SafeReplicationSettings)
		if err != nil {
			opt.logger.Warnf("can not set default replication settings on the host %s", node.Host())
		}
		return errMasterIsNil
	}

	if node.Host() == master.Host() {
		opt.logger.Debugf("optimization: skipping local sync on master host [%s]", master.Host())
		// Case when optimization is set manually on DCS/was not disabled on failover/switchover
		return opt.disableMasterOptimizationIfNeeded(master)
	}

	status := opt.readOptimizationFile()
	isOptimizingDcs, err := opt.isHostOptimizingDcs(node.Host())
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
		return opt.phaseCheckOptimizationProgress(master, node)

	case isOptimizingDcs && status != StatusEnabled:
		return opt.phaseEnableOptimization(node)

	case !isOptimizingDcs && status != StatusDisabled:
		return opt.phaseDisableOptimization(master, node)
	}

	return nil
}

func (opt *Optimizer) shouldSync() bool {
	now := time.Now()
	if now.Sub(opt.lastSync) < opt.config.SyncInterval {
		return false
	}
	opt.lastSync = now
	return true
}

func (opt *Optimizer) phaseCheckOptimizationProgress(
	master, node NodeReplicationController,
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

	err = opt.removeDcsOptimizationHost(node.Host())
	if err != nil {
		return err
	}

	opt.DCS.ReleaseLock(pathOptimizationLock)

	return opt.removeOptimizationFile()
}

func (opt *Optimizer) phaseEnableOptimization(
	node NodeReplicationController,
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
		err = opt.removeDcsOptimizationHost(node.Host())
		if err != nil {
			return err
		}
		opt.DCS.ReleaseLock(pathOptimizationLock)
		return opt.removeOptimizationFile()
	}

	err = opt.writeOptimizationFileStatus(StatusPending)
	if err != nil {
		return err
	}

	acquired := opt.DCS.AcquireLock(pathOptimizationLock)
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

	opt.DCS.ReleaseLock(pathOptimizationLock)

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

func (opt *Optimizer) removeDcsOptimizationHost(host string) error {
	return opt.DCS.Delete(dcs.JoinPath(pathOptimizationNodes, host))
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

func (opt *Optimizer) isHostOptimizingDcs(host string) (bool, error) {
	err := opt.DCS.Get(dcs.JoinPath(pathOptimizationNodes, host), &struct{}{})
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
	ignoreErrors bool,
) error {
	opt.logger.Debugf("optimization: disable optimization on %s", node.Host())
	err := opt.DCS.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	if err != nil && err != dcs.ErrNotFound {
		if ignoreErrors {
			opt.logger.Warnf("optimization: optimization mode on host %s has not been disabled on DCS: %s", node.Host(), err)
		} else {
			return err
		}
	}

	err = node.SetReplicationSettings(masterSettings)
	if err != nil && ignoreErrors {
		opt.logger.Warnf("optimization: optimization mode on host %s has not been disabled on mysql: %s", node.Host(), err)
		err = nil
	}
	return err
}

// disableMasterOptimizationIfNeeded removes stale optimization artifacts from DCS/MySQL
// that can remain after switchover/failover events or manual DCS path edits.
func (opt *Optimizer) disableMasterOptimizationIfNeeded(
	master NodeReplicationController,
) error {
	err := opt.DCS.Delete(dcs.JoinPath(pathOptimizationNodes, master.Host()))
	if err != nil && err != dcs.ErrNotFound {
		return err
	}
	if err != dcs.ErrNotFound {
		opt.logger.Warnf("optimization: master [%s] had optimization mode enabled in DCS; it has been removed", master.Host())
	}

	if opt.optimizationFileExists() {
		opt.logger.Warnf("optimization: found an optimization file on master [%s]; this indicates an error."+
			"The file will be removed and replication settings reset to safe defaults", master.Host())

		err := master.SetReplicationSettings(mysql.SafeReplicationSettings)
		if err != nil {
			return err
		}

		err = opt.removeOptimizationFile()
		if err != nil {
			return err
		}
	}

	return nil
}

func (opt *Optimizer) optimizationFileExists() bool {
	_, err := os.Stat(opt.config.File)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func (opt *Optimizer) getMasterReplicationSettings(master NodeReplicationController, ignoreErrors bool) (mysql.ReplicationSettings, error) {
	if master == nil || reflect.ValueOf(master).IsNil() {
		if ignoreErrors {
			opt.logger.Warnf("optimization: master is nil")
			return mysql.SafeReplicationSettings, nil
		} else {
			return mysql.ReplicationSettings{}, errMasterIsNil
		}
	}

	masterRS, err := master.GetReplicationSettings()
	if err != nil && ignoreErrors {
		opt.logger.Warnf("optimization: optimization settings on %s can not be acquired; fall back to default ones", master.Host(), err)
		masterRS = mysql.SafeReplicationSettings
		err = nil
	}
	return masterRS, err
}
