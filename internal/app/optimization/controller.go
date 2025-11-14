package optimization

import (
	"context"
	"fmt"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func NewController(
	config config.OptimizationConfig,
	logger Logger,
	dcs DCS,
	waitingCheckInterval time.Duration,
) *controller {
	return &controller{
		config:               config,
		logger:               logger,
		dcs:                  dcs,
		waitingCheckInterval: waitingCheckInterval,
	}
}

type controller struct {
	config               config.OptimizationConfig
	logger               Logger
	dcs                  DCS
	waitingCheckInterval time.Duration
}

// Wait blocks until node is optimized
func (m *controller) Wait(ctx context.Context, node Node) error {
	ticker := time.NewTicker(m.waitingCheckInterval)
	defer ticker.Stop()

	maxConsequentErrors := 3
	consequentErrors := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("optimization waiting deadline exceeded")

		case <-ticker.C:
			isOptimized, err := m.isOptimizedDuringWaiting(node)
			if err != nil {
				m.logger.Errorf("optimization: waiting; err %s", err)
				consequentErrors += 1
			}
			if isOptimized {
				m.logger.Infof("optimization: waiting is complete")
				return nil
			}
			if consequentErrors > maxConsequentErrors {
				return err
			}
		}
	}
}

func (m *controller) isOptimizedDuringWaiting(node Node) (bool, error) {
	dcsState, err := m.dcs.GetState(node.Host())
	if err != nil {
		return false, err
	}
	if dcsState == nil {
		return true, nil
	}
	if dcsState.Status == StatusEnabled {
		m.logger.Infof("optimization: waiting; node is optimizing")
	} else {
		return true, nil
	}

	replicationStatus, err := node.GetReplicaStatus()
	if err != nil {
		return false, err
	}

	lag := replicationStatus.GetReplicationLag()
	if lag.Valid && lag.Float64 < float64(m.config.LowReplicationMark) {
		m.logger.Infof("optimization: waiting is complete, as replication lag is converged: %s", lag.Float64)
		return true, m.dcs.DeleteHosts(node.Host())
	}

	m.logger.Infof("optimization: waiting; replication lag is %f", lag.Float64)
	return false, nil
}

// Enable activates optimization mode for the specified node.
// The change may not take effect immediately (e.g., pending retries or submissions).
// By default, only one optimized replica is allowed per setup to avoid data loss.
// Returns an error if enabling fails (e.g., due to existing optimizations or channel closures).
func (m *controller) Enable(node Node) error {
	return m.dcs.CreateHosts(node.Host())
}

// Disable deactivates optimization mode for the specified node,
// Changes take effect immediately, as these options can be dangerous.
// Returns an error if disabling fails.
func (m *controller) Disable(master, node Node) error {
	rs, err := master.GetReplicationSettings()
	if err != nil {
		m.logger.Warnf("cannot get replication setting from the master: %s", err.Error())
		rs = mysql.SafeReplicationSettings
	}
	return m.disable(rs, node)
}

// DisableAll deactivates optimization mode for all specified nodes.
// This is a bulk operation with immediate effects,
// and it carries risks similar to disabling a single node.
// Returns an error if disabling any node fails.
func (m *controller) DisableAll(master Node, nodes []Node) error {
	hostnames := m.dcsHostnames(m.dcs, nodes)
	hostnameToNode := makeHostToNodeMap(nodes...)
	errors := make([]error, 0, len(nodes))
	rs, err := master.GetReplicationSettings()
	if err != nil {
		m.logger.Warnf("cannot get replication setting from the master: %s", err.Error())
		rs = mysql.SafeReplicationSettings
	}

	for _, hostname := range hostnames {
		node, ok := hostnameToNode[hostname]
		if !ok {
			m.logger.Warnf("host %s was not found", hostname)
			continue
		}

		err := m.disable(rs, node)
		if err != nil {
			errors = append(errors, fmt.Errorf("%s:%s", hostname, err))
		}
	}

	if len(errors) == 0 {
		return nil
	}
	return fmt.Errorf("got the following errors: %s", util.JoinErrors(errors, ","))
}

func (m *controller) disable(rs mysql.ReplicationSettings, node Node) error {
	err := node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}
	return m.dcs.DeleteHosts(node.Host())
}

func (m *controller) dcsHostnames(Dcs DCS, fallbackNodes []Node) []string {
	hostnames, err := Dcs.GetHosts()
	if err != nil {
		for _, node := range fallbackNodes {
			hostnames = append(hostnames, node.Host())
		}
	}
	return hostnames
}

func makeHostToNodeMap(nodes ...Node) map[string]Node {
	m := map[string]Node{}
	for _, node := range nodes {
		m[node.Host()] = node
	}
	return m
}
