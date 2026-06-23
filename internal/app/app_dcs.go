package app

import (
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

// The methods below are thin wrappers on *App that delegate to app.appDCS.
// They exist so that existing callers (cli_*.go, recovery.go, async.go, etc.)
// continue to work without modification. New code should call app.appDCS directly.

// GetActiveNodes returns master + alive running replicas.
func (app *App) GetActiveNodes() ([]string, error) {
	return app.appDCS.GetActiveNodes()
}

// SetActiveNodes writes the active nodes list to ZK.
func (app *App) SetActiveNodes(nodes []string) error {
	return app.appDCS.SetActiveNodes(nodes)
}

// DeleteActiveNodes removes the active nodes list from ZK.
func (app *App) DeleteActiveNodes() error {
	return app.appDCS.DeleteActiveNodes()
}

// SetHealthState writes the ephemeral per-host health state to ZK.
func (app *App) SetHealthState(host string, state *nodestate.NodeState) error {
	return app.appDCS.SetHealthState(host, state)
}

// GetHealthState reads the per-host health state from ZK.
func (app *App) GetHealthState(host string, state *nodestate.NodeState) error {
	return app.appDCS.GetHealthState(host, state)
}

// SetMaintenance writes the maintenance record to ZK.
func (app *App) SetMaintenance(maintenance *Maintenance) error {
	return app.appDCS.SetMaintenance(maintenance)
}

// DeleteMaintenance removes the maintenance record from ZK.
func (app *App) DeleteMaintenance() error {
	return app.appDCS.DeleteMaintenance()
}

// FetchCascadeNodeConfigurations reads all cascade node configurations from ZK.
func (app *App) FetchCascadeNodeConfigurations() (map[string]mysql.CascadeNodeConfiguration, error) {
	return app.appDCS.FetchCascadeNodeConfigurations()
}

// GetNodeConfiguration reads the HA node configuration (priority etc.) from ZK.
func (app *App) GetNodeConfiguration(host string) (mysql.NodeConfiguration, error) {
	return app.appDCS.GetNodeConfiguration(host)
}

// GetClusterCascadeFqdnsFromDcs returns cascade node FQDNs stored in ZK.
func (app *App) GetClusterCascadeFqdnsFromDcs() ([]string, error) {
	return app.appDCS.GetClusterCascadeFqdnsFromDcs()
}

// GetMaintenance returns the current maintenance record from ZK.
func (app *App) GetMaintenance() (*Maintenance, error) {
	return app.appDCS.GetMaintenance()
}

// GetHostsOnRecovery returns hosts currently marked for recovery.
func (app *App) GetHostsOnRecovery() ([]string, error) {
	return app.appDCS.GetHostsOnRecovery()
}

// ClearRecovery removes the recovery marker for a host.
func (app *App) ClearRecovery(host string) error {
	return app.appDCS.ClearRecovery(host)
}

// SetRecovery marks a host for recovery and removes it from active nodes.
func (app *App) SetRecovery(host string) error {
	activeNodes, err := app.appDCS.GetActiveNodes()
	if err != nil {
		return err
	}
	activeNodes = util.FilterStrings(activeNodes, func(n string) bool {
		return n != host
	})
	if err = app.appDCS.SetActiveNodes(activeNodes); err != nil {
		return err
	}
	return app.appDCS.SetRecovery(host)
}

// IsRecoveryNeeded returns true if the host has a recovery marker in ZK.
func (app *App) IsRecoveryNeeded(host string) bool {
	return app.appDCS.IsRecoveryNeeded(host)
}

// setResetupStatus is an unexported wrapper kept for internal callers (recovery.go).
func (app *App) setResetupStatus(host string, status bool) error {
	return app.appDCS.SetResetupStatus(host, &mysql.ResetupStatus{
		Status:     status,
		UpdateTime: time.Now(),
	})
}

// GetResetupStatus reads the resetup status for a host.
func (app *App) GetResetupStatus(host string) (mysql.ResetupStatus, error) {
	return app.appDCS.GetResetupStatus(host)
}

// UpdateLastShutdownNodeTime records the current time as the last shutdown time.
func (app *App) UpdateLastShutdownNodeTime() error {
	return app.appDCS.UpdateLastShutdownNodeTime()
}

// GetOrCreateLastShutdownNodeTime returns the last recorded shutdown time.
func (app *App) GetOrCreateLastShutdownNodeTime() (time.Time, error) {
	return app.appDCS.GetOrCreateLastShutdownNodeTime()
}

// FinishSwitchover finishes the current switchover and writes the result.
// Kept on *App because it calls timing methods (stopTiming, logSwitchoverFailure).
func (app *App) FinishSwitchover(switchover *Switchover, switchErr error) error {
	result := true
	action := "finished"
	path := pathLastSwitch
	if switchErr != nil {
		result = false
		action = "rejected"
		path = pathLastRejectedSwitch
	}

	app.logger.Info().Msgf("switchover: %s => %s %s", switchover.From, switchover.To, action)
	switchover.Result = new(SwitchoverResult)
	switchover.Result.Ok = result
	switchover.Result.FinishedAt = time.Now()

	if switchErr != nil {
		switchover.Result.Error = switchErr.Error()
	}

	if switchErr != nil {
		app.logSwitchoverFailure(switchover)
	} else if switchover.MasterTransition != FailoverTransition {
		app.stopTiming(timingSwitchover)
	} else {
		app.stopTiming(timingFailover)
	}

	err := app.appDCS.DeleteCurrentSwitchover()
	if err != nil {
		return err
	}
	if path == pathLastSwitch {
		return app.appDCS.SetLastSwitchover(switchover)
	}
	return app.appDCS.SetLastRejectedSwitchover(switchover)
}

// FailSwitchover marks the current switchover as failed (will be retried next cycle).
// Kept on *App for symmetry with FinishSwitchover and StartSwitchover.
func (app *App) FailSwitchover(switchover *Switchover, err error) error {
	app.logger.Error().Err(err).Msgf("switchover: %s => %s failed", switchover.From, switchover.To)
	switchover.RunCount++
	switchover.Result = new(SwitchoverResult)
	switchover.Result.Ok = false
	switchover.Result.Error = err.Error()
	switchover.Result.FinishedAt = time.Now()
	return app.appDCS.SetCurrentSwitchover(switchover)
}

// StartSwitchover records that a switchover has started.
// Kept on *App because it calls startTiming.
func (app *App) StartSwitchover(switchover *Switchover) error {
	app.logger.Info().Msgf("switchover: %s => %s starting...", switchover.From, switchover.To)
	switchover.StartedAt = time.Now()
	switchover.StartedBy = app.config.Hostname
	if switchover.MasterTransition != FailoverTransition {
		app.startTiming(timingSwitchover, switchover.InitiatedAt)
	}
	return app.appDCS.SetCurrentSwitchover(switchover)
}

// GetCurrentSwitchover reads the current in-progress switchover from ZK.
// Returns dcs.ErrNotFound if no switchover is in progress.
func (app *App) GetCurrentSwitchover(switchover *Switchover) error {
	return app.appDCS.GetCurrentSwitchover(switchover)
}

// CreateCurrentSwitchover creates a new switchover record in ZK (fails if one already exists).
func (app *App) CreateCurrentSwitchover(switchover *Switchover) error {
	return app.appDCS.CreateCurrentSwitchover(switchover)
}

// DeleteCurrentSwitchover removes the current switchover node from ZK.
func (app *App) DeleteCurrentSwitchover() error {
	return app.appDCS.DeleteCurrentSwitchover()
}

// GetLastSwitchover returns the most recent switchover (finished or rejected).
func (app *App) GetLastSwitchover(switchover *Switchover) error {
	return app.appDCS.GetLastSwitchover(switchover)
}

// GetLastRejectedSwitchover returns the most recent rejected switchover.
func (app *App) GetLastRejectedSwitchover(switchover *Switchover) error {
	return app.appDCS.GetLastRejectedSwitchover(switchover)
}

// IssueFailover creates a new failover switchover record in ZK.
func (app *App) IssueFailover(master string) error {
	switchover := Switchover{
		From:             master,
		InitiatedBy:      app.config.Hostname,
		InitiatedAt:      time.Now(),
		Cause:            CauseAuto,
		MasterTransition: FailoverTransition,
	}
	return app.appDCS.CreateCurrentSwitchover(&switchover)
}

// SetMasterHost writes the current master hostname to ZK.
func (app *App) SetMasterHost(master string) (string, error) {
	return app.appDCS.SetMasterHost(master)
}

// GetMasterHostFromDcs reads the current master hostname from ZK.
func (app *App) GetMasterHostFromDcs() (string, error) {
	return app.appDCS.GetMasterHostFromDcs()
}

// SetReplMonTS writes the replication monitor timestamp to ZK.
func (app *App) SetReplMonTS(ts string) error {
	return app.appDCS.SetReplMonTS(ts)
}

// GetReplMonTS reads the replication monitor timestamp from ZK.
func (app *App) GetReplMonTS() (string, error) {
	return app.appDCS.GetReplMonTS()
}

// SetLowSpace writes the low-space flag to ZK.
func (app *App) SetLowSpace(lowSpace bool) error {
	return app.appDCS.SetLowSpace(lowSpace)
}
