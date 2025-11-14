package optimization

import (
	"fmt"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func NewSyncer(
	logger Logger,
	config config.OptimizationConfig,
	Dcs DCS,
) (*syncer, error) {
	return &syncer{
		logger: logger,
		config: config,
		dcs:    Dcs,
	}, nil
}

type syncer struct {
	logger Logger
	config config.OptimizationConfig
	dcs    DCS
}

func (s *syncer) getClusterHostsState(
	c Cluster,
	masterRs mysql.ReplicationSettings,
) (*hostsState, error) {
	hostnames, err := s.dcs.GetHosts()
	if err != nil {
		return nil, err
	}

	hostsState := new(hostsState)
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

		switch {
		case isMaster || isSlaveLost:
			hostsState.MalfunctioningHosts = append(hostsState.MalfunctioningHosts, hostname)

		case isNearConverged && !isEnabled ||
			isCompletelyConverged && isEnabled:
			hostsState.OptimizedHosts = append(hostsState.OptimizedHosts, hostname)

		case isEnabled || !nodeState.ReplicationSettings.Equal(&masterRs):
			hostsState.OptimizingHosts = append(hostsState.OptimizingHosts, hostname)

		default:
			hostsState.DisabledHosts = append(hostsState.DisabledHosts, hostname)
		}
	}

	return hostsState, nil
}

type hostsState struct {
	// DisabledHosts are hosts planned for optimization
	DisabledHosts []string
	// OptimizingHosts are optimizing hosts
	OptimizingHosts []string
	// OptimizedHosts are hosts with lag lower than LowReplicationMark
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

func (s *syncer) Sync(c Cluster) error {
	masterRs, err := s.getMasterReplSettings(c)
	if err != nil {
		return err
	}

	hostsState, err := s.getClusterHostsState(c, masterRs)
	if err != nil {
		return err
	}
	s.logger.Infof(
		"optimization: %s",
		hostsState.String(),
	)

	hostsToDisable := util.Union(
		hostsState.OptimizedHosts,
		hostsState.MalfunctioningHosts,
	)
	err = s.disableNodes(c, hostsToDisable, masterRs)
	if err != nil {
		return err
	}

	return s.balanceToSingleNode(c, masterRs, hostsState)
}

func (s *syncer) balanceToSingleNode(
	c Cluster,
	masterRs mysql.ReplicationSettings,
	hostsState *hostsState,
) error {
	switch {
	case len(hostsState.OptimizingHosts) > 1:
		s.logger.Infof(
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
		s.logger.Infof(
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

func (s *syncer) startNodes(
	c Cluster,
	hosts []string,
) error {
	for _, host := range hosts {
		err := c.GetNode(host).OptimizeReplication()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) stopNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	for _, host := range hosts {
		err := c.GetNode(host).SetReplicationSettings(rs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) disableNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	if len(hosts) == 0 {
		return nil
	}

	err := s.stopNodes(c, hosts, rs)
	if err != nil {
		return err
	}
	return s.dcs.DeleteHosts(hosts...)
}

func (s *syncer) syncNodeOptions(
	host string,
	node Node,
) error {
	settings, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}
	if settings.CanBeOptimized() {
		s.logger.Warnf("Node %s should be optimizing but is not - restarting optimization", host)
		return node.OptimizeReplication()
	}
	return nil
}

func (s *syncer) getMasterReplSettings(c Cluster) (mysql.ReplicationSettings, error) {
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
