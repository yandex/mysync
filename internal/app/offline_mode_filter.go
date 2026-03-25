package app

import (
	"math"
	"strings"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/log"
)

// Decide whether the node can go offline or not.
// Tracking already offline nodes on current iteration with pendingOfflineByAZ
type OfflineModeFilter interface {
	CanSetOffline(host string, clusterState map[string]*nodestate.NodeState, pendingOfflineByAZ map[string]int) bool
}

func NewOfflineModeFilter(cfg *config.Config, logger *log.Logger) OfflineModeFilter {
	if cfg.OfflineModeMaxOfflinePct <= 0 {
		logger.Infof("using neverAllowOfflineFilter, no replicas allowed to go offline")
		return &neverAllowOfflineFilter{logger: logger}
	}
	if cfg.OfflineModeMaxOfflinePct >= 100 {
		logger.Infof("using alwaysAllowOfflineFilter, all replicas allowed to go offline")
		return &alwaysAllowOfflineFilter{logger: logger}
	}
	logger.Infof(
		"offline mode filter: using azLimitedOfflineFilter (offline_mode_max_offline_pct=%d%%, offline_mode_az_separator=%q)",
		cfg.OfflineModeMaxOfflinePct, cfg.OfflineModeAZSeparator,
	)
	return &azLimitedOfflineFilter{
		maxOfflinePct: cfg.OfflineModeMaxOfflinePct,
		azSeparator:   cfg.OfflineModeAZSeparator,
		logger:        logger,
	}
}

// If offline_mode_max_offline_pct is set to 100, it means all replicas can go offline
type alwaysAllowOfflineFilter struct {
	logger *log.Logger
}

func (f *alwaysAllowOfflineFilter) CanSetOffline(host string, _ map[string]*nodestate.NodeState, _ map[string]int) bool {
	return true
}

// If offline_mode_max_offline_pct is set to 0, it means no replicas can go offline
type neverAllowOfflineFilter struct {
	logger *log.Logger
}

func (f *neverAllowOfflineFilter) CanSetOffline(host string, _ map[string]*nodestate.NodeState, _ map[string]int) bool {
	return false
}

// Handler for cases between 0 and 100 offline_mode_max_offline_pct
type azLimitedOfflineFilter struct {
	maxOfflinePct int
	azSeparator   string
	logger        *log.Logger
}

func (f *azLimitedOfflineFilter) CanSetOffline(host string, clusterState map[string]*nodestate.NodeState, pendingOfflineByAZ map[string]int) bool {
	az := getAvailabilityZone(host, f.azSeparator)

	totalInAZ := 0
	offlineInAZ := 0

	for h, state := range clusterState {
		if state.IsMaster || getAvailabilityZone(h, f.azSeparator) != az {
			continue
		}
		totalInAZ++
		if state.IsOffline {
			offlineInAZ++
		}
	}

	// Probably unreachable
	if totalInAZ == 0 {
		return false
	}

	// Add nodes already set offline in the current iteration
	pendingInAZ := pendingOfflineByAZ[az]
	offlineInAZ += pendingInAZ

	// If this replica will go offline and total percentage of offline replicas in az
	// will be less or equal to offline_mode_max_offline_pct, then it can go offline
	willBeOfflinePct := int(math.Floor(100 * float64(offlineInAZ+1) / float64(totalInAZ)))

	canGoOffline := willBeOfflinePct <= f.maxOfflinePct
	f.logger.Debugf(
		"offline mode filter: host %s (az=%q): total=%d, already_offline=%d, pending=%d, will_be_offline_pct=%d%%, max_offline_pct=%d%% => can_go_offline=%v",
		host, az, totalInAZ, offlineInAZ-pendingInAZ, pendingInAZ, willBeOfflinePct, f.maxOfflinePct, canGoOffline,
	)
	return canGoOffline
}

// Extract az name from hostname prefix
// Separator is configurable and set as '-' by default
// zone_123-mysql -> zone_123 availability zone
func getAvailabilityZone(fqdn, separator string) string {
	if separator == "" {
		return ""
	}
	if idx := strings.Index(fqdn, separator); idx != -1 {
		return fqdn[:idx]
	}
	return ""
}
