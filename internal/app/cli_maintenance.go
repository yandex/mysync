package app

import (
	"context"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/util"
)

// CliEnableMaintenance enables maintenance mode
func (app *App) CliEnableMaintenance(waitTimeout time.Duration, reason string) int {
	ctx := app.baseContext()
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	maintenance := &Maintenance{
		InitiatedBy: util.GuessWhoRunning() + "@" + app.config.Hostname,
		InitiatedAt: time.Now(),
		Reason:      reason,
	}
	err = app.dcs.Create(pathMaintenance, maintenance)
	if err != nil && err != dcs.ErrExists {
		app.logger.Error(err.Error())
		return 1
	}
	// wait for mysync to pause
	if waitTimeout > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				err = app.dcs.Get(pathMaintenance, maintenance)
				if err != nil {
					app.logger.Error(err.Error())
				}
				if maintenance.MySyncPaused {
					break Out
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if !maintenance.MySyncPaused {
			app.logger.Error("could not wait for mysync to enter maintenance")
			return 1
		}
		fmt.Println("maintenance enabled")
	} else {
		fmt.Println("maintenance scheduled")
	}
	return 0
}

// CliDisableMaintenance disables maintenance mode
func (app *App) CliDisableMaintenance(waitTimeout time.Duration) int {
	ctx := app.baseContext()
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	maintenance := &Maintenance{}
	err = app.dcs.Get(pathMaintenance, maintenance)
	if err == dcs.ErrNotFound {
		fmt.Println("maintenance disabled")
		return 0
	} else if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	maintenance.ShouldLeave = true
	err = app.dcs.Set(pathMaintenance, maintenance)
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	if waitTimeout > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				err = app.dcs.Get(pathMaintenance, maintenance)
				if err == dcs.ErrNotFound {
					maintenance = nil
					break Out
				}
				if err != nil {
					app.logger.Error(err.Error())
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if maintenance != nil {
			app.logger.Error("could not wait for mysync to leave maintenance")
			return 1
		}
		fmt.Println("maintenance disabled")
	} else {
		fmt.Println("maintenance disable scheduled")
	}
	return 0
}

// CliGetMaintenance prints on/off depending on current maintenance status
func (app *App) CliGetMaintenance() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.dcs.Get(pathMaintenance, new(Maintenance))
	switch err {
	case nil:
		fmt.Println("on")
		return 0
	case dcs.ErrNotFound:
		fmt.Println("off")
		return 0
	default:
		app.logger.Error(err.Error())
		return 1
	}
}
