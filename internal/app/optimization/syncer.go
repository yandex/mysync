package optimization

import (
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

type Syncer interface {
	// SyncState synchronizes optimization settings,
	// and applies replication adjustments if needed.
	// Returns an error if synchronization fails.
	Sync(c Cluster) error
}

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

func (s *syncer) getClusterHostsState(c Cluster) (*hostsState, error) {
	hostnames, err := s.dcs.GetHosts()
	if err != nil {
		return nil, err
	}

	hostsState := new(hostsState)
	masterRs := c.GetState(c.GetMaster()).ReplicationSettings

	lowReplMark := s.config.LowReplicationMark.Seconds()
	highReplMark := s.config.HighReplicationMark.Seconds()

	for _, hostname := range hostnames {
		dcsState, err := s.dcs.GetState(hostname)
		if err != nil {
			return nil, err
		}

		if err != nil {
			return nil, err
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

		case isEnabled || !nodeState.ReplicationSettings.Equal(masterRs):
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

func (opt *syncer) Sync(c Cluster) error {
	masterRs, err := opt.getMasterReplSettings(c)
	if err != nil {
		return err
	}

	hostsState, err := opt.getClusterHostsState(c)
	if err != nil {
		return err
	}

	hostsToDisable := util.Union(
		hostsState.OptimizedHosts,
		hostsState.MalfunctioningHosts,
	)
	err = opt.disableNodes(c, hostsToDisable, masterRs)
	if err != nil {
		return err
	}

	return opt.balanceToSingleNode(c, masterRs, hostsState)
}

func (opt *syncer) balanceToSingleNode(
	c Cluster,
	masterRs mysql.ReplicationSettings,
	hostsState *hostsState,
) error {
	switch {
	case len(hostsState.OptimizingHosts) > 1:
		opt.logger.Infof(
			"optimization: there are too many nodes: %d. Turn %d off",
			len(hostsState.OptimizingHosts),
			len(hostsState.OptimizingHosts)-1,
		)
		err := opt.stopNodes(
			c,
			hostsState.OptimizingHosts[1:],
			masterRs,
		)
		if err != nil {
			return err
		}
		host := hostsState.OptimizingHosts[0]
		return opt.syncNodeOptions(host, c.GetNode(host))

	case len(hostsState.OptimizingHosts) == 0 && len(hostsState.DisabledHosts) > 0:
		opt.logger.Infof(
			"optimization: start optimizing new node %s",
			hostsState.DisabledHosts[0],
		)
		return opt.startNodes(c, hostsState.DisabledHosts[:1])

	case len(hostsState.OptimizingHosts) == 1:
		host := hostsState.OptimizingHosts[0]
		return opt.syncNodeOptions(host, c.GetNode(host))
	}
	return nil
}

func (opt *syncer) startNodes(
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

func (opt *syncer) stopNodes(
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

func (opt *syncer) disableNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	if len(hosts) == 0 {
		return nil
	}

	err := opt.stopNodes(c, hosts, rs)
	if err != nil {
		return err
	}
	return opt.dcs.DeleteHosts(hosts...)
}

func (opt *syncer) syncNodeOptions(
	host string,
	node Node,
) error {
	settings, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}
	if settings.CanBeOptimized() {
		opt.logger.Warnf("Node %s should be optimizing but is not - restarting optimization", host)
		return node.OptimizeReplication()
	}
	return nil
}

func (opt *syncer) getMasterReplSettings(c Cluster) (mysql.ReplicationSettings, error) {
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
