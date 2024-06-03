package mysql

import (
	"time"

	"github.com/yandex/mysync/internal/config"
)

type ISwitchHelper interface {
	GetPriorityChoiceMaxLag() time.Duration
}

type DefaultSwitchHelper struct {
	config *config.Config
}

type AsyncSwitchHelper struct {
	config *config.Config
}

func NewSwitchHelper(config *config.Config) ISwitchHelper {
	if config.ASync {
		return &AsyncSwitchHelper{
			config: config,
		}
	}
	return &DefaultSwitchHelper{
		config: config,
	}
}

func (sh *DefaultSwitchHelper) GetPriorityChoiceMaxLag() time.Duration {
	return sh.config.PriorityChoiceMaxLag
}

func (sh *AsyncSwitchHelper) GetPriorityChoiceMaxLag() time.Duration {
	AsyncAllowedLagTime := time.Duration(sh.config.AsyncAllowedLag) * time.Second
	if AsyncAllowedLagTime > sh.config.PriorityChoiceMaxLag {
		return AsyncAllowedLagTime
	}
	return sh.config.PriorityChoiceMaxLag
}
