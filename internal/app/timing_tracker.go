package app

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/util"
)

// names of tracked timings
const (
	timingDowntime   = "downtime"
	timingFailover   = "failover"
	timingSwitchover = "switchover"
)

func (app *App) timingPath(name string) string {
	return dcs.JoinPath(pathTimings, name)
}

// getTimingStart returns stored start time and whether it exists
func (app *App) getTimingStart(name string) (time.Time, bool) {
	var ts time.Time
	if err := app.dcs.Get(app.timingPath(name), &ts); err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// startTiming stores timing start in dcs (zero ts means now)
func (app *App) startTiming(name string, ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}
	if err := app.dcs.Set(app.timingPath(name), ts); err != nil {
		app.logger.Warn().Err(err).Msgf("failed to start timing %s", name)
	}
}

func (app *App) clearTiming(name string) {
	if err := app.dcs.Delete(app.timingPath(name)); err != nil {
		app.logger.Warn().Err(err).Msgf("failed to clear timing %s", name)
	}
}

// stopTiming logs elapsed time since start and clears it
func (app *App) stopTiming(name string) {
	start, ok := app.getTimingStart(name)
	if !ok {
		return
	}
	app.clearTiming(name)
	app.logTiming(name, time.Since(start))
}

// logSwitchoverFailure records time spent on a failed switchover as a negative value.
// Marker-guarded so it is logged once, and only when the switchover actually started.
func (app *App) logSwitchoverFailure(sw *Switchover) {
	if sw.MasterTransition != SwitchoverTransition {
		return
	}
	start, ok := app.getTimingStart(timingSwitchover)
	if !ok {
		return
	}
	app.clearTiming(timingSwitchover)
	since := sw.InitiatedAt
	if since.IsZero() {
		since = start
	}
	app.logTiming(timingSwitchover+"_failure", -time.Since(since))
}

// logTiming runs the configured log_timing command with name and elapsed seconds
func (app *App) logTiming(name string, d time.Duration) {
	command, ok := app.config.Commands["log_timing"]
	if !ok || command == "" {
		return
	}
	command = fmt.Sprintf(command, name, strconv.FormatFloat(d.Seconds(), 'f', 3, 64))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shell := util.GetEnvVariable("SHELL", "sh")
	if err := exec.CommandContext(ctx, shell, "-c", command).Run(); err != nil {
		app.logger.Warn().Err(err).Msgf("failed to execute log_timing command for %s", name)
	}
}
