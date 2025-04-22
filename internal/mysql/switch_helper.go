package mysql

import (
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/config"
)

type ISwitchHelper interface {
	GetPriorityChoiceMaxLag() time.Duration
	GetRequiredWaitSlaveCount([]string) int
	GetFailoverQuorum([]string) int
	CheckFailoverQuorum([]string, int) error
	IsOptimizationPhaseAllowed() bool
}

type SwitchHelper struct {
	priorityChoiceMaxLag               time.Duration
	rplSemiSyncMasterWaitForSlaveCount int
	SemiSync                           bool
}

func NewSwitchHelper(config *config.Config) ISwitchHelper {
	priorityChoiceMaxLag := config.PriorityChoiceMaxLag
	if config.ASync {
		if config.AsyncAllowedLag > config.PriorityChoiceMaxLag {
			priorityChoiceMaxLag = config.AsyncAllowedLag
		}
	}
	return &SwitchHelper{
		priorityChoiceMaxLag:               priorityChoiceMaxLag,
		rplSemiSyncMasterWaitForSlaveCount: config.RplSemiSyncMasterWaitForSlaveCount,
		SemiSync:                           config.SemiSync,
	}
}

func (sh *SwitchHelper) GetPriorityChoiceMaxLag() time.Duration {
	return sh.priorityChoiceMaxLag
}

// GetRequiredWaitSlaveCount Dynamically calculated version of RplSemiSyncMasterWaitForSlaveCount.
// This variable can be lower than hard-configured RplSemiSyncMasterWaitForSlaveCount
// when some semi-sync replicas are dead.
func (sh *SwitchHelper) GetRequiredWaitSlaveCount(activeNodes []string) int {
	wsc := min(len(activeNodes)/2, sh.rplSemiSyncMasterWaitForSlaveCount)
	return wsc
}

// GetFailoverQuorum Number of HA nodes to be alive to failover/switchover
func (sh *SwitchHelper) GetFailoverQuorum(activeNodes []string) int {
	fq := max(len(activeNodes)-sh.GetRequiredWaitSlaveCount(activeNodes), 1)
	return fq
}

func (sh *SwitchHelper) CheckFailoverQuorum(activeNodes []string, permissibleSlaves int) error {
	if sh.SemiSync {
		failoverQuorum := sh.GetFailoverQuorum(activeNodes)
		if permissibleSlaves < failoverQuorum {
			return fmt.Errorf("no quorum, have %d replicas while %d is required", permissibleSlaves, failoverQuorum)
		}
	} else {
		if permissibleSlaves == 0 {
			return fmt.Errorf("no alive active replica found")
		}
	}
	return nil
}

func (sh *SwitchHelper) IsOptimizationPhaseAllowed() bool {
	return sh.SemiSync
}
