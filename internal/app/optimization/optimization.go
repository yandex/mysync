package optimization

import (
	"context"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

// WaitOptimization blocks until node is optimized
func WaitOptimization(
	ctx context.Context,
	config config.OptimizationConfig,
	logger Logger,
	node Node,
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
	node Node,
	Dcs DCS,
) (bool, error) {
	dcsState, err := getHostDCSState(Dcs, node.Host())
	if err != nil {
		return false, err
	}
	if dcsState.Status == StatusEnabled {
		logger.Infof("optimization: waiting; node is optimizing")
	} else {
		return true, nil
	}

	replicationStatus, err := node.GetReplicaStatus()
	if err != nil {
		return false, err
	}

	lag := replicationStatus.GetReplicationLag()
	if lag.Valid && lag.Float64 < float64(config.LowReplicationMark) {
		logger.Infof("optimization: waiting is complete, as replication lag is converged: %s", lag.Float64)
		return true, Dcs.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	}

	logger.Infof("optimization: waiting; replication lag is %f", lag.Float64)
	return false, nil
}

// EnableNodeOptimization activates optimization mode for the specified node.
// The change may not take effect immediately (e.g., pending retries or submissions).
// By default, only one optimized replica is allowed per setup to avoid data loss.
// Returns an error if enabling fails (e.g., due to existing optimizations or channel closures).
func EnableNodeOptimization(
	node Node,
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
	master, node Node,
	Dcs DCS,
) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		rs = mysql.SafeReplicationSettings
	}

	err = node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}

	return Dcs.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
}

// DisableAllNodeOptimization deactivates optimization mode for all specified nodes,
// using the master for context. This is a bulk operation with immediate effects,
// and it carries risks similar to disabling a single node.
// Returns an error if disabling any node fails.
func DisableAllNodeOptimization(
	master Node,
	nodes []Node,
	Dcs DCS,
) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		rs = mysql.SafeReplicationSettings
	}

	hostnames := getDCSOptimizingHostnames(Dcs, nodes)

	err = disableParticularNodes(
		hostnames,
		rs,
		Dcs,
		nodes,
	)

	return err
}

func getDCSOptimizingHostnames(Dcs DCS, fallbackNodes []Node) []string {
	hostnames, err := Dcs.GetChildren(pathOptimizationNodes)
	if err != nil {
		for _, node := range fallbackNodes {
			hostnames = append(hostnames, node.Host())
		}
	}
	return hostnames
}

func disableParticularNodes(
	hostnames []string,
	masterRs mysql.ReplicationSettings,
	Dcs DCS,
	nodes []Node,
) error {
	errorArray := make([]error, 0, len(nodes))

	m := makeHostToNodeMap(nodes...)
	for _, hostname := range hostnames {
		node, ok := m[hostname]
		if !ok {
			continue
		}

		err := node.SetReplicationSettings(masterRs)
		if err != nil {
			return err
		}

		err = Dcs.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
		if err != nil {
			errorArray = append(errorArray, fmt.Errorf("%s:%w", hostname, err))
		}
	}

	if len(errorArray) > 0 {
		return fmt.Errorf(
			"got the following errors: %s",
			util.JoinErrors(errorArray, ","),
		)
	}

	return nil
}

func makeHostToNodeMap(nodes ...Node) map[string]Node {
	m := map[string]Node{}
	for _, node := range nodes {
		m[node.Host()] = node
	}
	return m
}
