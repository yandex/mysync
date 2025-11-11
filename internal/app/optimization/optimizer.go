package optimization

import (
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

type ReplicationOpitimizer interface {
	// Initialize initializes components
	// Must be called before any other method
	Initialize(dcs DCS) error

	// SyncState synchronizes optimization settings,
	// and applies replication adjustments if needed.
	// Returns an error if synchronization fails.
	SyncState(c Cluster) error
}

func NewOptimizer(
	logger Logger,
	config config.OptimizationConfig,
) *Optimizer {
	return &Optimizer{
		logger: logger,
		config: config,
	}
}

type Optimizer struct {
	logger Logger
	config config.OptimizationConfig
	dcs    DCS
}

type DCSState struct {
	Status Status `json:"status"`
}
type Status string

const (
	StatusNew     Status = ""
	StatusEnabled Status = "enabled"
)

func (opt *Optimizer) Initialize(DCS DCS) error {
	opt.dcs = DCS

	err := DCS.Create(pathOptimizationNodes, "")
	if err != nil && err != dcs.ErrExists {
		return err
	}

	opt.logger.Infof("Optimizer is initialized")
	return nil
}

func (opt *Optimizer) getClusterHostsState(c Cluster) (*HostsState, error) {
	hostnames, err := opt.dcs.GetChildren(pathOptimizationNodes)
	if err != nil {
		return nil, err
	}

	hostsState := new(HostsState)
	masterRs := c.GetState(c.GetMaster()).ReplicationSettings

	lowReplMark := opt.config.LowReplicationMark.Seconds()
	highReplMark := opt.config.HighReplicationMark.Seconds()

	for _, hostname := range hostnames {
		dcsState, err := getHostDCSState(opt.dcs, hostname)
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

func getHostDCSState(Dcs DCS, hostname string) (*DCSState, error) {
	path := dcs.JoinPath(pathOptimizationNodes, hostname)

	state := new(DCSState)
	err := Dcs.Get(path, state)
	if err != nil && err != dcs.ErrNotFound && err != dcs.ErrMalformed {
		return nil, err
	}

	return state, nil
}

type HostsState struct {
	// DisabledHosts are hosts planned for optimization
	DisabledHosts []string
	// OptimizingHosts are optimizing hosts
	OptimizingHosts []string
	// OptimizedHosts are hosts with lag lower than LowReplicationMark
	OptimizedHosts []string
	// MalfunctioningHosts are hosts that shouldn't have been optimized
	MalfunctioningHosts []string
}

func (opt *Optimizer) SyncState(c Cluster) error {
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

func (opt *Optimizer) balanceToSingleNode(
	c Cluster,
	masterRs mysql.ReplicationSettings,
	hostsState *HostsState,
) error {
	switch {
	case len(hostsState.OptimizingHosts) > 1:
		util.Shuffle(hostsState.OptimizingHosts)
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
		util.Shuffle(hostsState.DisabledHosts)
		return opt.startNodes(c, hostsState.DisabledHosts[:1])

	case len(hostsState.OptimizingHosts) == 1:
		host := hostsState.OptimizingHosts[0]
		return opt.syncNodeOptions(host, c.GetNode(host))
	}
	return nil
}

func (opt *Optimizer) startNodes(
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

func (opt *Optimizer) stopNodes(
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

func (opt *Optimizer) deleteNodes(
	hosts []string,
) error {
	for _, host := range hosts {
		err := opt.dcs.Delete(dcs.JoinPath(pathOptimizationNodes, host))
		if err != nil && err != dcs.ErrNotFound {
			return err
		}
	}
	return nil
}

func (opt *Optimizer) disableNodes(
	c Cluster,
	hosts []string,
	rs mysql.ReplicationSettings,
) error {
	err := opt.stopNodes(c, hosts, rs)
	if err != nil {
		return err
	}
	return opt.deleteNodes(hosts)
}

func (opt *Optimizer) syncNodeOptions(
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

func (opt *Optimizer) getMasterReplSettings(c Cluster) (mysql.ReplicationSettings, error) {
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
