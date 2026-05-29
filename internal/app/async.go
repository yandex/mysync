package app

import (
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/mysql"
)

func (app *App) CheckAsyncSwitchAllowed(node *mysql.Node, switchover *Switchover) bool {
	if app.config.ASync && switchover.Cause == CauseAuto && app.config.AsyncAllowedLag > 0 {
		app.logger.Info().Msg("async mode is active and this is auto switch so we checking new master delay")
		ts, err := app.GetReplMonTS()
		if err != nil {
			app.logger.Error().Err(err).Msg("failed to get mdb repl mon ts")
			return false
		}
		delay, err := node.CalcReplMonTSDelay(app.config.ReplMonSchemeName, app.config.ReplMonTableName, ts)
		if err != nil {
			app.logger.Error().Err(err).Msg("failed to calc mdb repl mon ts")
			return false
		}
		if time.Duration(delay)*time.Second < app.config.AsyncAllowedLag {
			app.logger.Info().Msgf("async allowed lag is %f seconds and current lag on host %s is %d, so we don't wait for catch up any more",
				app.config.AsyncAllowedLag.Seconds(), node.Host(), delay)
			return true
		}
	}
	return false
}

func (app *App) updateReplMonTS(master string) error {
	masterNode := app.cluster.Get(master)
	ts, err := masterNode.GetReplMonTS(app.config.ReplMonSchemeName, app.config.ReplMonTableName)
	if err != nil {
		return fmt.Errorf("failed to get master repl_mon timestamp: %w", err)
	}
	return app.SetReplMonTS(ts)
}
