package app

import (
	"errors"
	"fmt"
	"time"
)

// switchoverAbortDeadline exists while the persisted switchover is still at a
// safe abort point. Abortable remains true across retries and manager restarts
// and is cleared in DCS before phase 5 changes the replication topology.
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

// recordSwitchoverAttemptResult retries regular errors, but a timeout observed
// at a safe abort point is terminal and moves the switch to last_rejected.
func (app *App) recordSwitchoverAttemptResult(switchover *Switchover, switchErr error) error {
	if switchErr == nil || errors.Is(switchErr, ErrSwitchoverTimeout) {
		return app.FinishSwitchover(switchover, switchErr)
	}
	return app.FailSwitchover(switchover, switchErr)
}
