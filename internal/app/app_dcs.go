package app

import (
	"errors"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

// The methods below are thin wrappers on *App that delegate to app.appDCS.
// They exist so that existing callers (cli_*.go, recovery.go, async.go, etc.)
// continue to work without modification. New code should call app.appDCS directly.

// GetActiveNodes returns master + alive running replicas.
func (app *App) GetActiveNodes() ([]string, error) {
	return app.appDCS.GetActiveNodes()
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
	return app.appDCS.SetRecovery(host)
}

// IsRecoveryNeeded returns true if the host has a recovery marker in ZK.
func (app *App) IsRecoveryNeeded(host string) bool {
	return app.appDCS.IsRecoveryNeeded(host)
}

// setResetupStatus is an unexported wrapper kept for internal callers (recovery.go).
func (app *App) setResetupStatus(host string, status bool) error {
	return app.appDCS.SetResetupStatus(host, status)
}

// GetResetupStatus reads the resetup status for a host.
func (app *App) GetResetupStatus(host string) (mysql.ResetupStatus, error) {
	return app.appDCS.GetResetupStatus(host)
}

// UpdateLastShutdownNodeTime records the current time as the last shutdown time.
func (app *App) UpdateLastShutdownNodeTime() error {
	return app.appDCS.UpdateLastShutdownNodeTime()
}

// GetLastShutdownNodeTime returns the last recorded shutdown time.
func (app *App) GetLastShutdownNodeTime() (time.Time, error) {
	return app.appDCS.GetLastShutdownNodeTime()
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
	} else if switchover.MasterTransition == SwitchoverTransition {
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
	if switchover.MasterTransition == SwitchoverTransition {
		app.startTiming(timingSwitchover, switchover.InitiatedAt)
	}
	return app.appDCS.SetCurrentSwitchover(switchover)
}

// GetLastSwitchover returns the most recent switchover (finished or rejected).
func (app *App) GetLastSwitchover() Switchover {
	return app.appDCS.GetLastSwitchover()
}

// IssueFailover creates a new failover switchover record in ZK.
func (app *App) IssueFailover(master string) error {
	return app.appDCS.IssueFailover(master)
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

// checkCurrentSwitchover reads the current in-progress switchover from ZK.
// Used internally by stateManager/stateCandidate.
func (app *App) checkCurrentSwitchover() (*Switchover, error) {
	var switchover Switchover
	err := app.dcs.Get(pathCurrentSwitch, &switchover)
	if errors.Is(err, dcs.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get current switchover from dcs: %w", err)
	}
	return &switchover, nil
}
