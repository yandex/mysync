package app

import (
	"fmt"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type switchoverPhase int

const (
	switchoverOptimization switchoverPhase = iota
	switchoverFreeze
	switchoverCatchUp
	switchoverTurnReplicas
	switchoverPromote
	switchoverAdjustSemiSync
	switchoverMakeWritable
	switchoverReenableEvents
	switchoverSetExternalReplication
	switchoverSetMasterInDCS
	switchoverComplete
)

const (
	switchoverOptimizationName           = "optimization"
	switchoverFreezeName                 = "freeze"
	switchoverCatchUpName                = "catch up"
	switchoverTurnReplicasName           = "turn replicas"
	switchoverPromoteName                = "promote"
	switchoverAdjustSemiSyncName         = "adjust semi-sync"
	switchoverMakeWritableName           = "make writable"
	switchoverReenableEventsName         = "reenable events"
	switchoverSetExternalReplicationName = "set external replication"
	switchoverSetMasterInDCSName         = "set master in DCS"
	switchoverCompleteName               = "complete"
)

func (phase switchoverPhase) String() string {
	names := [...]string{
		switchoverOptimization:           switchoverOptimizationName,
		switchoverFreeze:                 switchoverFreezeName,
		switchoverCatchUp:                switchoverCatchUpName,
		switchoverTurnReplicas:           switchoverTurnReplicasName,
		switchoverPromote:                switchoverPromoteName,
		switchoverAdjustSemiSync:         switchoverAdjustSemiSyncName,
		switchoverMakeWritable:           switchoverMakeWritableName,
		switchoverReenableEvents:         switchoverReenableEventsName,
		switchoverSetExternalReplication: switchoverSetExternalReplicationName,
		switchoverSetMasterInDCS:         switchoverSetMasterInDCSName,
		switchoverComplete:               switchoverCompleteName,
	}
	if phase < 0 || phase >= switchoverPhase(len(names)) {
		return fmt.Sprintf("unknown (%d)", phase)
	}
	return names[phase]
}

// switchoverProgress belongs to the manager process only. It is deliberately
// separate from Switchover: no phase or topology is persisted in DCS.
type switchoverProgress struct {
	request                  Switchover
	phase                    switchoverPhase
	oldMaster                string
	newMaster                string
	mostRecent               string
	mostRecentGTIDSet        gtids.GTIDSet
	frozenActiveNodes        []string
	activeNodesWithOldMaster []string
}

func (p *switchoverProgress) matches(sw *Switchover) bool {
	return p != nil && sw != nil &&
		p.request.InitiatedAt.Equal(sw.InitiatedAt) &&
		p.request.InitiatedBy == sw.InitiatedBy &&
		p.request.From == sw.From && p.request.To == sw.To &&
		p.request.Cause == sw.Cause && p.request.MasterTransition == sw.MasterTransition
}

func (app *App) switchoverStarted(sw *Switchover) bool {
	if app.switchoverProgress.matches(sw) {
		return app.switchoverProgress.phase >= switchoverFreeze
	}
	// An older request has no local phase. Do not reject it when its previous
	// attempt could already have changed MySQL.
	return sw != nil && sw.RunCount > 0
}

func (app *App) getMasterForSwitchover(clusterState map[string]*nodestate.NodeState, sw *Switchover) (string, error) {
	if app.switchoverStarted(sw) {
		if app.switchoverProgress.matches(sw) {
			return app.switchoverProgress.oldMaster, nil
		}
		master, err := app.GetMasterHostFromDcs()
		if err != nil {
			return "", err
		}
		if master == "" {
			return "", fmt.Errorf("switchover: original master is unavailable")
		}
		return master, nil
	}
	return app.getCurrentMaster(clusterState)
}

// run advances only on success and skips actions before the current checkpoint.
func (p *switchoverProgress) run(phase switchoverPhase, action func() error) error {
	if p.phase > phase {
		return nil
	}
	if p.phase != phase {
		return fmt.Errorf("switchover: cannot run %s while at %s", phase, p.phase)
	}
	if err := action(); err != nil {
		return err
	}
	p.phase++
	return nil
}

func (app *App) switchoverTimedOut(sw *Switchover, now time.Time) bool {
	return !app.switchoverStarted(sw) && !sw.InitiatedAt.IsZero() &&
		now.Sub(sw.InitiatedAt) > app.config.SwitchoverTimeout
}
