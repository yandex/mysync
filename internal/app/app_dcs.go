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

// FinishSwitchover finish current switchover and write the result
func (app *App) FinishSwitchover(switchover *Switchover, switchErr error) error {
	result := true
	action := "finished"
	path := pathLastSwitch
	if switchErr != nil {
		result = false
		action = "rejected"
		path = pathLastRejectedSwitch
	}

	app.logger.Infof("switchover: %s => %s %s", switchover.From, switchover.To, action)
	switchover.Result = new(SwitchoverResult)
	switchover.Result.Ok = result
	switchover.Result.FinishedAt = time.Now()

	if switchErr != nil {
		switchover.Result.Error = switchErr.Error()
	}

	err := app.dcs.Delete(pathCurrentSwitch)
	if err != nil {
		return err
	}
	return app.dcs.Set(path, switchover)
}

// Fail current switchover, it will be repeated next cycle
func (app *App) FailSwitchover(switchover *Switchover, err error) error {
	app.logger.Errorf("switchover: %s => %s failed: %s", switchover.From, switchover.To, err)
	switchover.RunCount++
	switchover.Result = new(SwitchoverResult)
	switchover.Result.Ok = false
	switchover.Result.Error = err.Error()
	switchover.Result.FinishedAt = time.Now()
	return app.dcs.Set(pathCurrentSwitch, switchover)
}

func (app *App) StartSwitchover(switchover *Switchover) error {
	app.logger.Infof("switchover: %s => %s starting...", switchover.From, switchover.To)
	switchover.StartedAt = time.Now()
	switchover.StartedBy = app.config.Hostname
	return app.dcs.Set(pathCurrentSwitch, switchover)
}

func (app *App) GetLastSwitchover() Switchover {
	var lastSwitch, lastRejectedSwitch Switchover
	err := app.dcs.Get(pathLastSwitch, &lastSwitch)
	if err != nil && err != dcs.ErrNotFound {
		app.logger.Errorf("%s: %s", pathLastSwitch, err.Error())
	}
	errRejected := app.dcs.Get(pathLastRejectedSwitch, &lastRejectedSwitch)
	if errRejected != nil && errRejected != dcs.ErrNotFound {
		app.logger.Errorf("%s: %s", pathLastRejectedSwitch, errRejected.Error())
	}

	if lastRejectedSwitch.InitiatedAt.After(lastSwitch.InitiatedAt) {
		return lastRejectedSwitch
	}

	return lastSwitch
}

func (app *App) IssueFailover(master string) error {
	var switchover Switchover
	switchover.From = master
	switchover.InitiatedBy = app.config.Hostname
	switchover.InitiatedAt = time.Now()
	switchover.Cause = CauseAuto
	return app.dcs.Create(pathCurrentSwitch, switchover)
}

func (app *App) SetMasterHost(master string) (string, error) {
	err := app.dcs.Set(pathMasterNode, master)
	if err != nil {
		return "", fmt.Errorf("failed to set current master to dcs: %s", err)
	}
	return master, nil
}

func (app *App) GetMasterHostFromDcs() (string, error) {
	var master string
	err := app.dcs.Get(pathMasterNode, &master)
	if err != nil && err != dcs.ErrNotFound {
		return "", fmt.Errorf("failed to get current master from dcs: %s", err)
	}
	if master != "" {
		return master, nil
	}
	return "", nil
}

func (app *App) SetReplMonTS(ts string) error {
	err := app.dcs.Create(pathMasterReplMonTS, ts)
	if err != nil && err != dcs.ErrExists {
		return err
	}
	err = app.dcs.Set(pathMasterReplMonTS, ts)
	if err != nil {
		return err
	}
	return nil
}

func (app *App) GetReplMonTS() (string, error) {
	var ts string
	err := app.dcs.Get(pathMasterReplMonTS, &ts)
	if errors.Is(err, dcs.ErrNotFound) {
		return "", nil
	}
	return ts, err
}
