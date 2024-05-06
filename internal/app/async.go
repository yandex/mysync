package app

import (
	"fmt"
	"github.com/yandex/mysync/internal/mysql"
)

func (app *App) CheckAsyncSwitchAllowed(node *mysql.Node, switchover *Switchover) bool {
	if app.config.ASync && switchover.Cause == CauseAuto {
		app.logger.Infof("async mode is active and this is auto switch so we checking new master delay")
		ts, err := app.GetMdbReplMonTS()
		if err != nil {
			app.logger.Errorf("failed to get mdb repl mon ts: %v", err)
			return false
		}
		delay, err := node.CalcMdbReplMonTSDelay(ts)
		if err != nil {
			app.logger.Errorf("failed to calc mdb repl mon ts: %v", err)
			return false
		}
		if delay < app.config.AsyncAllowedLag {
			app.logger.Infof("async allowed lag is %d and current lag on host %s is %d, so we don't wait for catch up any more",
				app.config.AsyncAllowedLag, node.Host(), delay)
			return true
		}
	}
	return false
}

func (app *App) updateMdbReplMonTS(master string) error {
	masterNode := app.cluster.Get(master)
	ts, err := masterNode.GetMdbReplMonTS()
	if err != nil {
		return fmt.Errorf("failed to get master mdb_repl_mon timestamp: %v", err)
	}
	return app.SetMdbReplMonTS(ts)
}