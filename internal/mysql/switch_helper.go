package mysql

import (
	"time"

	"github.com/yandex/mysync/internal/config"
)

type ISwitchHelper interface {
	GetPriorityChoiceMaxLag() time.Duration
}

type SwitchHelper struct {
	priorityChoiceMaxLag time.Duration
}

func NewSwitchHelper(config *config.Config) ISwitchHelper {
	priorityChoiceMaxLag := config.PriorityChoiceMaxLag
	if config.ASync {
		AsyncAllowedLagTime := time.Duration(config.AsyncAllowedLag) * time.Second
		if AsyncAllowedLagTime > config.PriorityChoiceMaxLag {
			priorityChoiceMaxLag = AsyncAllowedLagTime
		}
	}
	return &SwitchHelper{
		priorityChoiceMaxLag: priorityChoiceMaxLag,
	}
}

func (sh *SwitchHelper) GetPriorityChoiceMaxLag() time.Duration {
	return sh.priorityChoiceMaxLag
}
