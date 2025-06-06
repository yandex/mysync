package app

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/util"
)

// CliSwitch performs manual switch-over of the master node
// nolint: gocyclo, funlen
func (app *App) CliSwitch(switchFrom, switchTo string, waitTimeout time.Duration, failover bool) int {
	ctx := app.baseContext()
	if switchFrom == "" && switchTo == "" {
		app.logger.Errorf("Either --from or --to should be set")
		return 1
	}
	if switchFrom != "" && switchTo != "" {
		app.logger.Errorf("Option --from and --to can't be used in the same time")
		return 1
	}

	cancel, err := app.cliInitApp()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer cancel()

	if len(app.cluster.HANodeHosts()) == 1 {
		app.logger.Info("switchover has not sense on single HA-node cluster")
		fmt.Println("switchover done")
		return 0
	}

	var fromHost, toHost string

	var currentMaster string
	if err := app.dcs.Get(pathMasterNode, &currentMaster); err != nil {
		app.logger.Errorf("failed to get current master: %v", err)
		return 1
	}
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}

	if switchTo != "" {
		// switch to particular host
		desired := util.SelectNodes(app.cluster.HANodeHosts(), switchTo)
		if len(desired) == 0 {
			app.logger.Errorf("no HA-nodes matching '%s'", switchTo)
			return 1
		}
		if len(desired) > 1 {
			app.logger.Errorf("two or more nodes matching '%s'", switchTo)
			return 1
		}
		toHost = desired[0]
		if toHost == currentMaster {
			app.logger.Infof("master is already on %s, skipping...", toHost)
			fmt.Println("switchover done")
			return 0
		}
		if !slices.Contains(activeNodes, toHost) {
			app.logger.Errorf("%s is not active, can't switch to it", toHost)
			return 1
		}
	} else {
		// switch away from specified host(s)
		notDesired := util.SelectNodes(app.cluster.HANodeHosts(), switchFrom)
		if len(notDesired) == 0 {
			app.logger.Errorf("no HA-nodes matches '%s', check --from param", switchFrom)
			return 1
		}
		if !slices.Contains(notDesired, currentMaster) {
			app.logger.Infof("master is already not on %v, skipping...", notDesired)
			fmt.Println("switchover done")
			return 0
		}
		var candidates []string
		for _, node := range activeNodes {
			if !slices.Contains(notDesired, node) {
				candidates = append(candidates, node)
			}
		}
		if len(candidates) == 0 {
			app.logger.Errorf("there are no active nodes, not matching '%s'", switchFrom)
			return 1
		}
		if len(notDesired) == 1 {
			fromHost = notDesired[0]
		} else {
			// there are multiple hosts matching --from pattern
			// to avoid switching from one to another, use switch to behavior
			positions, err := app.getNodePositions(candidates)
			if err != nil {
				app.logger.Error(err.Error())
				return 1
			}
			toHost, err = getMostDesirableNode(app.logger, positions, app.switchHelper.GetPriorityChoiceMaxLag())
			if err != nil {
				app.logger.Error(err.Error())
				return 1
			}
		}
	}

	var switchover Switchover
	err = app.dcs.Get(pathCurrentSwitch, &switchover)
	if err == nil {
		app.logger.Errorf("Another switchover in progress %v", switchover)
		return 2
	}
	if err != dcs.ErrNotFound {
		app.logger.Error(err.Error())
		return 2
	}

	switchover.From = fromHost
	switchover.To = toHost
	switchover.InitiatedBy = util.GuessWhoRunning() + "@" + app.config.Hostname
	switchover.InitiatedAt = time.Now()
	switchover.Cause = CauseManual
	if failover {
		switchover.MasterTransition = FailoverTransition
	} else {
		switchover.MasterTransition = SwitchoverTransition
	}

	err = app.dcs.Create(pathCurrentSwitch, switchover)
	if err == dcs.ErrExists {
		app.logger.Error("Another switchover in progress")
		return 2
	}
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	// wait for switchover to complete
	if waitTimeout > 0 {
		var lastSwitchover Switchover
		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()
		ticker := time.NewTicker(time.Second)
	Out:
		for {
			select {
			case <-ticker.C:
				lastSwitchover = app.GetLastSwitchover()
				if lastSwitchover.InitiatedBy == switchover.InitiatedBy && lastSwitchover.InitiatedAt.Unix() == switchover.InitiatedAt.Unix() {
					break Out
				} else {
					lastSwitchover = Switchover{}
				}
			case <-waitCtx.Done():
				break Out
			}
		}
		if lastSwitchover.Result == nil {
			app.logger.Error("could not wait for switchover to complete")
			return 1
		} else if !lastSwitchover.Result.Ok {
			app.logger.Error("could not wait for switchover to complete because of errors")
			return 1
		}
		fmt.Println("switchover done")
	} else {
		fmt.Println("switchover scheduled")
	}
	return 0
}

// CliAbort cleans switchover node from DCS
func (app *App) CliAbort() int {
	err := app.connectDCS()
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	defer app.dcs.Close()
	app.dcs.Initialize()

	err = app.dcs.Get(pathCurrentSwitch, new(Switchover))
	if err == dcs.ErrNotFound {
		fmt.Println("no active switchover")
		return 0
	}
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}

	const phrase = "yes, abort switch"
	fmt.Printf("please, confirm aborting switchover by typing '%s'\n", phrase)
	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}
	if strings.TrimSpace(response) != phrase {
		fmt.Printf("doesn't match, do nothing")
		return 1
	}

	err = app.dcs.Delete(pathCurrentSwitch)
	if err != nil {
		app.logger.Error(err.Error())
		return 1
	}

	fmt.Printf("switchover aborted\n")
	return 0
}
