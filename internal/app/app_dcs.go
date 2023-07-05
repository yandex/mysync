package app

import (
	"errors"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

// GetActiveNodes returns master + alive running replicas
func (app *App) GetActiveNodes() ([]string, error) {
	var activeNodes []string
	err := app.dcs.Get(pathActiveNodes, &activeNodes)
	if err != nil {
		if err == dcs.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get active nodes from zk %v", err)
	}
	return activeNodes, nil
}

func (app *App) GetClusterCascadeFqdnsFromDcs() ([]string, error) {
	fqdns, err := app.dcs.GetChildren(dcs.PathCascadeNodesPrefix)
	if err == dcs.ErrNotFound {
		return make([]string, 0), nil
	}
	if err != nil {
		return nil, err
	}

	return fqdns, nil
}

func (app *App) GetMaintenance() (*Maintenance, error) {
	maintenance := new(Maintenance)
	err := app.dcs.Get(pathMaintenance, maintenance)
	if err != nil {
		return nil, err
	}
	return maintenance, err
}

func (app *App) GetHostsOnRecovery() ([]string, error) {
	hosts, err := app.dcs.GetChildren(pathRecovery)
	if err == dcs.ErrNotFound {
		return nil, nil
	}
	return hosts, err
}

func (app *App) ClearRecovery(host string) error {
	return app.dcs.Delete(dcs.JoinPath(pathRecovery, host))
}

func (app *App) SetRecovery(host string) error {
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		return err
	}
	activeNodes = util.FilterStrings(activeNodes, func(n string) bool {
		return n != host
	})
	err = app.dcs.Set(pathActiveNodes, activeNodes)
	if err != nil {
		return err
	}
	err = app.dcs.Create(pathRecovery, nil)
	if err != nil && err != dcs.ErrExists {
		return err
	}
	err = app.dcs.Create(dcs.JoinPath(pathRecovery, host), nil)
	if err != nil && err != dcs.ErrExists {
		return err
	}
	return nil
}

func (app *App) IsRecoveryNeeded(host string) bool {
	err := app.dcs.Get(dcs.JoinPath(pathRecovery, host), &struct{}{})
	// we ignore any zk errors here, as it will appear on next iteration
	// and lead to lost state followed by recovery
	return err == nil
}

func (app *App) setResetupStatus(host string, status bool) error {
	err := app.dcs.Create(pathResetupStatus, nil)
	if err != nil && err != dcs.ErrExists {
		return err
	}
	resetupStatus := &mysql.ResetupStatus{
		Status:     status,
		UpdateTime: time.Now(),
	}
	err = app.dcs.Create(dcs.JoinPath(pathResetupStatus, host), resetupStatus)
	if err != nil && err != dcs.ErrExists {
		return err
	}
	err = app.dcs.Set(dcs.JoinPath(pathResetupStatus, host), resetupStatus)
	if err != nil {
		return err
	}
	return nil
}

func (app *App) GetResetupStatus(host string) (mysql.ResetupStatus, error) {
	resetupStatus := mysql.ResetupStatus{}
	err := app.dcs.Get(dcs.JoinPath(pathResetupStatus, host), &resetupStatus)
	return resetupStatus, err
}

func (app *App) UpdateLastShutdownNodeTime() error {
	err := app.dcs.Create(pathLastShutdownNodeTime, time.Now())
	if err != nil && err != dcs.ErrExists {
		return err
	}
	err = app.dcs.Set(pathLastShutdownNodeTime, time.Now())
	if err != nil {
		return err
	}
	return nil
}

func (app *App) GetLastShutdownNodeTime() (time.Time, error) {
	var t time.Time
	err := app.dcs.Get(pathLastShutdownNodeTime, &t)
	if errors.Is(err, dcs.ErrNotFound) {
		err = app.dcs.Create(pathLastShutdownNodeTime, time.Now())
		if err != nil {
			return time.Now(), err
		}
		return time.Now(), nil
	}
	return t, err
}
