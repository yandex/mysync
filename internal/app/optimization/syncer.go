package optimization

import (
	"errors"
	"fmt"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func NewSyncer(
	logger *log.Logger,
	config config.OptimizationConfig,
	relayLogMaxBytes int64,
	Dcs DCS,
) *Syncer {
	return &Syncer{
		logger:           logger,
		config:           config,
		relayLogMaxBytes: relayLogMaxBytes,
		dcs:              Dcs,
	}
}

type Syncer struct {
	logger           *log.Logger
	config           config.OptimizationConfig
	relayLogMaxBytes int64
	dcs              DCS
}

// Replica read errors leave a partial result so Sync can still disable completed
// optimizations. DCS errors invalidate the result and prevent all changes.
func (s *Syncer) getClusterHostsState(
	c Cluster,
	masterRs mysql.ReplicationSettings,
) (*hostsState, error) {
	hostnames, err := s.dcs.GetHosts()
	if err != nil {
		return nil, err
	}

	hostsState := new(hostsState)
	var errs []error
	lowReplMark := s.config.LowReplicationMark.Seconds()
	highReplMark := s.config.HighReplicationMark.Seconds()

	for _, hostname := range hostnames {
		dcsState, err := s.dcs.GetState(hostname)
		if err != nil {
			return nil, err
		}
		if dcsState == nil {
			continue
		}
		nodeState := c.GetState(hostname)

		isEnabled := dcsState.Status == StatusEnabled
		isMaster := nodeState.IsMaster
		isSlaveLost := nodeState.SlaveState == nil || nodeState.SlaveState.ReplicationLag == nil
		isNearConverged := !isSlaveLost && *nodeState.SlaveState.ReplicationLag < highReplMark
		isCompletelyConverged := !isSlaveLost && *nodeState.SlaveState.ReplicationLag < lowReplMark
		isRelayLogLarge := false
		if !isMaster && !isSlaveLost && dcsState.Reason == ReasonRelayLog {
			node := c.GetNode(hostname)
			if node == nil {
				hostsState.MalfunctioningHosts = append(hostsState.MalfunctioningHosts, hostname)
				continue
			}
			status, err := node.GetReplicaStatus()
			if err != nil {
				errs = append(errs, fmt.Errorf("get relay log space for %s: %w", hostname, err))
				continue
			}
			if status == nil {
				isSlaveLost = true
			} else {
				isRelayLogLarge = status.GetRelayLogSpace() > s.relayLogMaxBytes
			}
		}

		switch {
		case isMaster || isSlaveLost:
			hostsState.MalfunctioningHosts = append(hostsState.MalfunctioningHosts, hostname)

		case !isRelayLogLarge && (isNearConverged && !isEnabled ||
			isCompletelyConverged && isEnabled):
			hostsState.OptimizedHosts = append(hostsState.OptimizedHosts, hostname)

		case isEnabled || !nodeState.ReplicationSettings.Equal(&masterRs):
			hostsState.OptimizingHosts = append(hostsState.OptimizingHosts, hostname)

		default:
			hostsState.DisabledHosts = append(hostsState.DisabledHosts, hostname)
		}
	}

	return hostsState, errors.Join(errs...)
}

type hostsState struct {
	// DisabledHosts are hosts planned for optimization
	DisabledHosts []string
	// OptimizingHosts are optimizing hosts
	OptimizingHosts []string
	// OptimizedHosts are hosts whose lag and, when requested, relay-log size have converged.
	OptimizedHosts []string
	// MalfunctioningHosts are hosts that shouldn't have been optimized
	MalfunctioningHosts []string
}

func (hs *hostsState) String() string {
	return fmt.Sprintf(
		"<disabled: %v, optimizing: %v, optimized: %v, malfunc: %v>",
		hs.DisabledHosts, hs.OptimizingHosts, hs.OptimizedHosts, hs.MalfunctioningHosts,
	)
}

func (s *Syncer) Sync(c Cluster) error {
	masterRs, err := s.getMasterReplSettings(c)
	if err != nil {
		return err
	}

	hostsState, stateErr := s.getClusterHostsState(c, masterRs)
	if hostsState == nil {
		return stateErr
	}
	s.logger.Info().Msgf(
		"optimization: %s",
		hostsState.String(),
	)

	hostsToDisable := util.Union(
		hostsState.OptimizedHosts,
		hostsState.MalfunctioningHosts,
	)
	// Restore known completed replicas even if another replica could not be read.
	// Do not start or restart turbo while its state on any host is uncertain.
	err = errors.Join(stateErr, s.disableNodes(c, hostsToDisable, masterRs))
	if err != nil {
		return err
	}

	return s.balanceToSingleNode(c, masterRs, hostsState)
}

func (s *Syncer) balanceToSingleNode(
	c Cluster,
	masterRs mysql.ReplicationSettings,
	hostsState *hostsState,
) error {
	switch {
	case len(hostsState.OptimizingHosts) > 1:
		s.logger.Info().Msgf(
			"optimization: there are too many nodes: %d. Turn %d off",
			len(hostsState.OptimizingHosts),
			len(hostsState.OptimizingHosts)-1,
		)
		err := s.stopNodes(
			c,
			hostsState.OptimizingHosts[1:],
			masterRs,
		)
		if err != nil {
			return err
		}
		host := hostsState.OptimizingHosts[0]
		return s.syncNodeOptions(host, c.GetNode(host))

	case len(hostsState.OptimizingHosts) == 0 && len(hostsState.DisabledHosts) > 0:
		s.logger.Info().Msgf(
			"optimization: start optimizing new node %s",
			hostsState.DisabledHosts[0],
		)
		return s.startNodes(c, hostsState.DisabledHosts[:1])

	case len(hostsState.OptimizingHosts) == 1:
		host := hostsState.OptimizingHosts[0]
		return s.syncNodeOptions(host, c.GetNode(host))
	}
	return nil
}

func (s *Syncer) startNodes(
	c Cluster,
	hosts []string,
) error {
	for _, host := range hosts {
		state, err := s.dcs.GetState(host)
		if err != nil {
			return err
		}
		if state == nil {
			continue
		}
		err = c.GetNode(host).OptimizeReplication()
		if err != nil {
			return err
		}
		// Mark as actively optimizing so Wait() knows to start checking replication lag
		state.Status = StatusEnabled
		err = s.dcs.SetState(host, state)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) stopNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	for _, host := range hosts {
		node := c.GetNode(host)
		if node == nil {
			s.logger.Info().Msgf(
				"optimization: host %s was disabled, no need to turn off optimizations - skipping",
				host,
			)

			continue
		}

		err := node.SetReplicationSettings(rs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) disableNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	var errs []error
	for _, host := range hosts {
		if err := s.stopNodes(c, []string{host}, rs); err != nil {
			errs = append(errs, fmt.Errorf("stop optimization on %s: %w", host, err))
			continue
		}
		// Only delete requests after their settings have been restored.
		if err := s.dcs.DeleteHosts(host); err != nil {
			errs = append(errs, fmt.Errorf("delete optimization request for %s: %w", host, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) syncNodeOptions(
	host string,
	node Node,
) error {
	settings, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}
	if settings.CanBeOptimized() {
		s.logger.Warn().Msgf("Node %s should be optimizing but is not - restarting optimization", host)
		return node.OptimizeReplication()
	}
	return nil
}

func (s *Syncer) getMasterReplSettings(c Cluster) (mysql.ReplicationSettings, error) {
	master := c.GetState(c.GetMaster())
	if master.ReplicationSettings != nil {
		return *master.ReplicationSettings, nil
	}

	rs, err := c.GetNode(c.GetMaster()).GetReplicationSettings()
	if err != nil {
		return mysql.ReplicationSettings{}, err
	}

	return rs, nil
}
