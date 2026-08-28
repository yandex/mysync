package app

import (
	"errors"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/dcs"
)

// switchoverAbortDeadline exists while the persisted switchover is still at a
// safe abort point. Abortable remains true across retries and manager restarts
// and is cleared in DCS before the first replication-topology change.
type switchoverAbortDeadline struct {
	at      time.Time
	timeout time.Duration
}

func (app *App) newSwitchoverAbortDeadline(switchover *Switchover) *switchoverAbortDeadline {
	// Records created by an older worker have no abortable field. It is safe to
	// initialize it only before their first attempt has started.
	if !switchover.Abortable && switchover.StartedAt.IsZero() && switchover.RunCount == 0 {
		switchover.Abortable = true
	}
	if switchover.InitiatedAt.IsZero() || !switchover.Abortable {
		return nil
	}
	return &switchoverAbortDeadline{
		at:      switchover.InitiatedAt.Add(app.config.SwitchoverTimeout),
		timeout: app.config.SwitchoverTimeout,
	}
}

func (app *App) markSwitchoverUnabortable(switchover *Switchover) error {
	updated := *switchover
	updated.Abortable = false
	updated.TopologyChanged = true
	if err := app.appDCS.SetCurrentSwitchover(&updated); err != nil {
		return fmt.Errorf("failed to persist switchover safe-abort boundary: %w", err)
	}
	*switchover = updated
	return nil
}

func (deadline *switchoverAbortDeadline) exceeded(now time.Time) error {
	if deadline == nil || now.Before(deadline.at) {
		return nil
	}
	return fmt.Errorf("%w after %s", ErrSwitchoverTimeout, deadline.timeout)
}

func switchoverAbortRequestedError(switchover *Switchover) error {
	if switchover.AbortRequestedBy == "" {
		return ErrSwitchoverAbortRequested
	}
	return fmt.Errorf("%w by %s", ErrSwitchoverAbortRequested, switchover.AbortRequestedBy)
}

func (app *App) checkSwitchoverAbort(
	switchover *Switchover,
	deadline *switchoverAbortDeadline,
	now time.Time,
	refresh bool,
) error {
	if err := deadline.exceeded(now); err != nil {
		return err
	}
	if switchover != nil && switchover.Abortable && switchover.AbortRequested {
		return switchoverAbortRequestedError(switchover)
	}
	if switchover != nil && !switchover.Abortable {
		return nil
	}
	if !refresh || switchover == nil {
		return nil
	}

	current := new(Switchover)
	if err := app.GetCurrentSwitchover(current); err != nil {
		return err
	}
	if current.OperationID != switchover.OperationID {
		return dcs.ErrVersionMismatch
	}
	if current.DCSVersion == switchover.DCSVersion {
		return nil
	}
	if !current.Abortable || !current.AbortRequested {
		return dcs.ErrVersionMismatch
	}

	switchover.AbortRequested = current.AbortRequested
	switchover.AbortRequestedBy = current.AbortRequestedBy
	switchover.AbortRequestedAt = current.AbortRequestedAt
	switchover.DCSVersion = current.DCSVersion
	return switchoverAbortRequestedError(switchover)
}

// recordSwitchoverAttemptResult retries regular errors. Safe timeouts, explicit
// abort requests, and failures classified as terminal move to last_rejected.
func (app *App) recordSwitchoverAttemptResult(switchover *Switchover, switchErr error) error {
	if switchErr == nil || errors.Is(switchErr, ErrSwitchoverTimeout) || errors.Is(switchErr, ErrSwitchoverAbortRequested) || errors.Is(switchErr, ErrSwitchoverTerminal) {
		return app.FinishSwitchover(switchover, switchErr)
	}
	return app.FailSwitchover(switchover, switchErr)
}
