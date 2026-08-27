package app

import (
	"errors"
	"fmt"
	"time"
)

// switchoverAbortDeadline is created only for a switchover that has never been
// attempted before. Without a persisted phase, a retry cannot be aborted
// safely: an earlier attempt might have already changed the replication
// topology in phase 5.
type switchoverAbortDeadline struct {
	at      time.Time
	timeout time.Duration
}

func (app *App) newSwitchoverAbortDeadline(switchover *Switchover) *switchoverAbortDeadline {
	if switchover.InitiatedAt.IsZero() || !switchover.StartedAt.IsZero() || switchover.RunCount != 0 {
		return nil
	}
	return &switchoverAbortDeadline{
		at:      switchover.InitiatedAt.Add(app.config.SwitchoverTimeout),
		timeout: app.config.SwitchoverTimeout,
	}
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
