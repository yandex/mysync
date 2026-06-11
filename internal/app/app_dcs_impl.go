package app

import (
	"errors"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

// appDCS implements IAppDCS by wrapping a low-level dcs.DCS.
// It owns all ZK path knowledge and serialization for mysync business logic.
// The struct is unexported; callers depend on the IAppDCS interface.
type appDCS struct {
	dcs    dcs.DCS
	config *config.Config
	logger *log.Logger
}

// NewAppDCS creates a new appDCS. Returns IAppDCS so callers depend on the interface.
func NewAppDCS(d dcs.DCS, cfg *config.Config, logger *log.Logger) IAppDCS {
	return &appDCS{dcs: d, config: cfg, logger: logger}
}

// compile-time assertion
var _ IAppDCS = (*appDCS)(nil)

// GetActiveNodes returns master + alive running replicas.
func (a *appDCS) GetActiveNodes() ([]string, error) {
	var activeNodes []string
	err := a.dcs.Get(pathActiveNodes, &activeNodes)
	if err != nil {
		if errors.Is(err, dcs.ErrNotFound) || errors.Is(err, dcs.ErrMalformed) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get active nodes from zk %w", err)
	}
	return activeNodes, nil
}

// SetActiveNodes writes the active nodes list to ZK.
func (a *appDCS) SetActiveNodes(nodes []string) error {
	return a.dcs.Set(pathActiveNodes, nodes)
}

// GetClusterCascadeFqdnsFromDcs returns cascade node FQDNs stored in ZK.
func (a *appDCS) GetClusterCascadeFqdnsFromDcs() ([]string, error) {
	fqdns, err := a.dcs.GetChildren(dcs.PathCascadeNodesPrefix)
	if errors.Is(err, dcs.ErrNotFound) {
		return make([]string, 0), nil
	}
	if err != nil {
		return nil, err
	}
	return fqdns, nil
}

// GetMaintenance returns the current maintenance record from ZK.
func (a *appDCS) GetMaintenance() (*Maintenance, error) {
	maintenance := new(Maintenance)
	err := a.dcs.Get(pathMaintenance, maintenance)
	if err != nil {
		return nil, err
	}
	return maintenance, err
}

// GetHostsOnRecovery returns hosts currently marked for recovery.
func (a *appDCS) GetHostsOnRecovery() ([]string, error) {
	hosts, err := a.dcs.GetChildren(pathRecovery)
	if errors.Is(err, dcs.ErrNotFound) {
		return nil, nil
	}
	return hosts, err
}

// ClearRecovery removes the recovery marker for a host.
func (a *appDCS) ClearRecovery(host string) error {
	return a.dcs.Delete(dcs.JoinPath(pathRecovery, host))
}

// SetRecovery marks a host for recovery and removes it from active nodes.
func (a *appDCS) SetRecovery(host string) error {
	activeNodes, err := a.GetActiveNodes()
	if err != nil {
		return err
	}
	activeNodes = util.FilterStrings(activeNodes, func(n string) bool {
		return n != host
	})
	err = a.dcs.Set(pathActiveNodes, activeNodes)
	if err != nil {
		return err
	}
	err = a.dcs.Create(pathRecovery, nil)
	if err != nil && !errors.Is(err, dcs.ErrExists) {
		return err
	}
	err = a.dcs.Create(dcs.JoinPath(pathRecovery, host), nil)
	if err != nil && !errors.Is(err, dcs.ErrExists) {
		return err
	}
	return nil
}

// IsRecoveryNeeded returns true if the host has a recovery marker in ZK.
func (a *appDCS) IsRecoveryNeeded(host string) bool {
	err := a.dcs.Get(dcs.JoinPath(pathRecovery, host), &struct{}{})
	// we ignore any zk errors here, as it will appear on next iteration
	// and lead to lost state followed by recovery
	return err == nil
}

// SetResetupStatus writes the resetup status for a host.
func (a *appDCS) SetResetupStatus(host string, status bool) error {
	err := a.dcs.Create(pathResetupStatus, nil)
	if err != nil && !errors.Is(err, dcs.ErrExists) {
		return err
	}
	resetupStatus := &mysql.ResetupStatus{
		Status:     status,
		UpdateTime: time.Now(),
	}
	return a.dcs.Set(dcs.JoinPath(pathResetupStatus, host), resetupStatus)
}

// GetResetupStatus reads the resetup status for a host.
func (a *appDCS) GetResetupStatus(host string) (mysql.ResetupStatus, error) {
	resetupStatus := mysql.ResetupStatus{}
	err := a.dcs.Get(dcs.JoinPath(pathResetupStatus, host), &resetupStatus)
	return resetupStatus, err
}

// UpdateLastShutdownNodeTime records the current time as the last shutdown time.
func (a *appDCS) UpdateLastShutdownNodeTime() error {
	return a.dcs.Set(pathLastShutdownNodeTime, time.Now())
}

// GetLastShutdownNodeTime returns the last recorded shutdown time, creating it if absent.
func (a *appDCS) GetLastShutdownNodeTime() (time.Time, error) {
	var t time.Time
	err := a.dcs.Get(pathLastShutdownNodeTime, &t)
	if errors.Is(err, dcs.ErrNotFound) {
		err = a.dcs.Create(pathLastShutdownNodeTime, time.Now())
		if err != nil {
			return time.Now(), err
		}
		return time.Now(), nil
	}
	return t, err
}

// GetLastSwitchover returns the most recent switchover (finished or rejected).
func (a *appDCS) GetLastSwitchover() Switchover {
	var lastSwitch, lastRejectedSwitch Switchover
	err := a.dcs.Get(pathLastSwitch, &lastSwitch)
	if err != nil && !errors.Is(err, dcs.ErrNotFound) {
		a.logger.Error().Err(err).Msg(pathLastSwitch)
	}
	errRejected := a.dcs.Get(pathLastRejectedSwitch, &lastRejectedSwitch)
	if errRejected != nil && !errors.Is(errRejected, dcs.ErrNotFound) {
		a.logger.Error().Err(errRejected).Msg(pathLastRejectedSwitch)
	}
	if lastRejectedSwitch.InitiatedAt.After(lastSwitch.InitiatedAt) {
		return lastRejectedSwitch
	}
	return lastSwitch
}

// GetCurrentSwitchover reads the current in-progress switchover from ZK.
// Returns dcs.ErrNotFound if no switchover is in progress.
func (a *appDCS) GetCurrentSwitchover(switchover *Switchover) error {
	return a.dcs.Get(pathCurrentSwitch, switchover)
}

// CreateCurrentSwitchover creates a new switchover record in ZK (fails if one already exists).
func (a *appDCS) CreateCurrentSwitchover(switchover *Switchover) error {
	return a.dcs.Create(pathCurrentSwitch, switchover)
}

// SetCurrentSwitchover writes the current in-progress switchover to ZK.
func (a *appDCS) SetCurrentSwitchover(switchover *Switchover) error {
	return a.dcs.Set(pathCurrentSwitch, switchover)
}

// DeleteCurrentSwitchover removes the current switchover node from ZK.
func (a *appDCS) DeleteCurrentSwitchover() error {
	return a.dcs.Delete(pathCurrentSwitch)
}

// SetLastSwitchover writes the completed switchover result to ZK.
func (a *appDCS) SetLastSwitchover(switchover *Switchover) error {
	return a.dcs.Set(pathLastSwitch, switchover)
}

// SetLastRejectedSwitchover writes the rejected switchover result to ZK.
func (a *appDCS) SetLastRejectedSwitchover(switchover *Switchover) error {
	return a.dcs.Set(pathLastRejectedSwitch, switchover)
}

// IssueFailover creates a new failover switchover record in ZK.
func (a *appDCS) IssueFailover(master string) error {
	var switchover Switchover
	switchover.From = master
	switchover.InitiatedBy = a.config.Hostname
	switchover.InitiatedAt = time.Now()
	switchover.Cause = CauseAuto
	switchover.MasterTransition = FailoverTransition
	return a.dcs.Create(pathCurrentSwitch, switchover)
}

// SetMasterHost writes the current master hostname to ZK.
func (a *appDCS) SetMasterHost(master string) (string, error) {
	err := a.dcs.Set(pathMasterNode, master)
	if err != nil {
		return "", fmt.Errorf("failed to set current master to dcs: %w", err)
	}
	return master, nil
}

// GetMasterHostFromDcs reads the current master hostname from ZK.
func (a *appDCS) GetMasterHostFromDcs() (string, error) {
	var master string
	err := a.dcs.Get(pathMasterNode, &master)
	if err != nil && !errors.Is(err, dcs.ErrNotFound) {
		return "", fmt.Errorf("failed to get current master from dcs: %w", err)
	}
	return master, nil
}

// SetReplMonTS writes the replication monitor timestamp to ZK.
func (a *appDCS) SetReplMonTS(ts string) error {
	err := a.dcs.Create(pathMasterReplMonTS, ts)
	if err != nil && !errors.Is(err, dcs.ErrExists) {
		return err
	}
	return a.dcs.Set(pathMasterReplMonTS, ts)
}

// GetReplMonTS reads the replication monitor timestamp from ZK.
func (a *appDCS) GetReplMonTS() (string, error) {
	var ts string
	err := a.dcs.Get(pathMasterReplMonTS, &ts)
	if errors.Is(err, dcs.ErrNotFound) {
		return "", nil
	}
	return ts, err
}

// SetLowSpace writes the low-space flag to ZK.
func (a *appDCS) SetLowSpace(lowSpace bool) error {
	return a.dcs.Set(pathLowSpace, lowSpace)
}
