package optimization

import (
	"context"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

// WaitOptimization blocks until node is optimized
func WaitOptimization(
	ctx context.Context,
	config config.OptimizationConfig,
	logger Logger,
	node NodeReplicationController,
	checkInterval time.Duration,
	dcs DCS,
) error {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	maxConsequentErrors := 3
	consequentErrors := 0

	for {
		select {
		case <-ctx.Done():
			return errOptimizationWaitingDeadlineExceeded

		case <-ticker.C:
			isOptimized, err := isOptimizedDuringWaiting(config, logger, node, dcs)
			if err != nil {
				logger.Errorf("optimization: waiting; err %s", err)
				consequentErrors += 1
			}
			if isOptimized {
				logger.Infof("optimization: waiting is complete")
				return nil
			}
			if consequentErrors > maxConsequentErrors {
				return err
			}
		}
	}
}

func isOptimizedDuringWaiting(
	config config.OptimizationConfig,
	logger Logger,
	node NodeReplicationController,
	Dcs DCS,
) (bool, error) {
	optimizing, err := isNodeOptimizing(node, Dcs)
	if err != nil {
		return false, err
	}

	if optimizing {
		logger.Infof("optimization: waiting; node is optimizing")
	} else {
		return true, nil
	}

	optimized, lag, err := isOptimized(
		node,
		config.LowReplicationMark.Seconds(),
	)
	if err != nil {
		return false, err
	}
	if optimized {
		logger.Infof("optimization: waiting is complete, as replication lag is converged: %s", lag)
		return true, Dcs.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	}

	logger.Infof("optimization: waiting; replication lag is %f", lag)
	return false, nil
}

// EnableNodeOptimization activates optimization mode for the specified node.
// The change may not take effect immediately (e.g., pending retries or submissions).
// By default, only one optimized replica is allowed per setup to avoid data loss.
// Returns an error if enabling fails (e.g., due to existing optimizations or channel closures).
func EnableNodeOptimization(
	node NodeReplicationController,
	Dcs DCS,
) error {
	err := Dcs.Create(dcs.JoinPath(pathOptimizationNodes, node.Host()), DCSState{})
	if err == dcs.ErrExists {
		return nil
	}
	return err
}

// DisableNodeOptimization deactivates optimization mode for the specified node,
// using the master for context (e.g., resetting replication settings).
// Changes take effect immediately, as these options can be dangerous.
// Returns an error if disabling fails.
func DisableNodeOptimization(
	master, node NodeReplicationController,
	Dcs DCS,
) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		rs = mysql.SafeReplicationSettings
	}
	return disableHostWithDCS(node, rs, Dcs)
}

// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
// using the master for context. This is a bulk operation with immediate effects,
// and it carries risks similar to disabling a single node.
// Returns an error if disabling any node fails.
func DisableAllNodeOptimization(
	master NodeReplicationController,
	nodes []NodeReplicationController,
	Dcs DCS,
) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		rs = mysql.SafeReplicationSettings
	}

	errorArray := make([]error, 0, len(nodes)+1)

	hostnames, err := Dcs.GetChildren(pathOptimizationNodes)
	if err != nil {
		errorArray = append(errorArray, err)
		for _, node := range nodes {
			hostnames = append(hostnames, node.Host())
		}
	}

	m := makeHostToNodeMap(nodes...)
	for _, hostname := range hostnames {
		err := disableHostWithDCS(m[hostname], rs, Dcs)
		if err != nil {
			errorArray = append(errorArray, fmt.Errorf("%s:%w", hostname, err))
		}
	}

	if len(errorArray) > 0 {
		return fmt.Errorf(
			"got the following errors: %s",
			JoinErrors(errorArray, ","),
		)
	}
	return nil
}

func makeHostToNodeMap(nodes ...NodeReplicationController) map[string]NodeReplicationController {
	m := map[string]NodeReplicationController{}
	for _, node := range nodes {
		m[node.Host()] = node
	}
	return m
}
