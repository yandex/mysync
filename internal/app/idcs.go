//go:generate mockgen -source=idcs.go -destination=mock_idcs_test.go -package=app . IAppDCS
package app

import (
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql"
)

// IAppDCS is a high-level interface for mysync ZooKeeper operations.
// It encapsulates all ZK paths and serialization, hiding the low-level dcs.DCS.
// Note: FinishSwitchover, StartSwitchover, FailSwitchover are NOT part of this
// interface because they call timing methods (startTiming/stopTiming/logSwitchoverFailure)
// defined on *App. They remain as *App methods that delegate to appDCS for pure ZK ops.
type IAppDCS interface {
	// Active nodes
	GetActiveNodes() ([]string, error)
	SetActiveNodes(nodes []string) error
	DeleteActiveNodes() error

	// Master
	GetMasterHostFromDcs() (string, error)
	SetMasterHost(master string) (string, error)

	// Health state (ephemeral per-host node state written by healthChecker)
	SetHealthState(host string, state *nodestate.NodeState) error
	GetHealthState(host string, state *nodestate.NodeState) error

	// Maintenance
	GetMaintenance() (*Maintenance, error)
	SetMaintenance(maintenance *Maintenance) error
	DeleteMaintenance() error

	// Recovery
	GetHostsOnRecovery() ([]string, error)
	SetRecovery(host string) error
	ClearRecovery(host string) error
	IsRecoveryNeeded(host string) bool

	// Resetup
	GetResetupStatus(host string) (mysql.ResetupStatus, error)
	SetResetupStatus(host string, status *mysql.ResetupStatus) error

	// Switchover state (pure ZK ops, no timing side-effects)
	GetCurrentSwitchover(switchover *Switchover) error
	CreateCurrentSwitchover(switchover *Switchover) error
	GetLastSwitchover(switchover *Switchover) error
	SetCurrentSwitchover(switchover *Switchover) error
	DeleteCurrentSwitchover() error
	SetLastSwitchover(switchover *Switchover) error
	SetLastRejectedSwitchover(switchover *Switchover) error
	GetLastRejectedSwitchover(switchover *Switchover) error

	// Shutdown tracking
	GetOrCreateLastShutdownNodeTime() (time.Time, error)
	UpdateLastShutdownNodeTime() error

	// Misc
	SetLowSpace(lowSpace bool) error

	// ReplMon
	GetReplMonTS() (string, error)
	SetReplMonTS(ts string) error

	// Cascade nodes
	GetClusterCascadeFqdnsFromDcs() ([]string, error)
	FetchCascadeNodeConfigurations() (map[string]mysql.CascadeNodeConfiguration, error)

	// HA node configuration
	GetNodeConfiguration(host string) (mysql.NodeConfiguration, error)
}
