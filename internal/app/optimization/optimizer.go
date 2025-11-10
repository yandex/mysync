package optimization

import (
	"fmt"
	"math/rand/v2"

	"github.com/go-zookeeper/zk"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
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

type Status string

type DCSState struct {
	Status Status `json:"status"`
}

const (
	StatusNew     Status = ""
	StatusEnabled Status = "enabled"
)

func (opt *Optimizer) Initialize(DCS DCS) error {
	opt.dcs = DCS

	err := DCS.Create(pathOptimizationNodes, "")
	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	opt.logger.Infof("Optimizer is initialized")
	return err
}

func (opt *Optimizer) getOptimizationState(c Cluster) (*OptimizationState, error) {
	hostnames, err := opt.dcs.GetChildren(pathOptimizationNodes)
	if err != nil {
		return nil, err
	}

	optState := new(OptimizationState)
	stopReplLowMark := opt.config.LowReplicationMark.Seconds()
	stopReplHighMark := opt.config.HighReplicationMark.Seconds()
	masterRs := c.GetState(c.GetMaster()).ReplicationSettings

	for _, hostname := range hostnames {
		dcsState, err := opt.getDCSState(hostname)
		if err != nil {
			return nil, err
		}
		nodeState := c.GetState(hostname)

		isEnabled := dcsState.Status == StatusEnabled
		isMaster := nodeState.IsMaster
		isSlaveLost := nodeState.SlaveState == nil || nodeState.SlaveState.ReplicationLag == nil
		isConverged := !isSlaveLost && *nodeState.SlaveState.ReplicationLag < stopReplHighMark
		isCompletelyConverged := !isSlaveLost && *nodeState.SlaveState.ReplicationLag < stopReplLowMark

		switch {
		case isMaster || isSlaveLost:
			optState.MalfunctioningHosts = append(optState.MalfunctioningHosts, hostname)

		case isConverged && !isEnabled ||
			isCompletelyConverged && isEnabled:
			optState.OptimizedHosts = append(optState.OptimizedHosts, hostname)

		case isEnabled || !nodeState.ReplicationSettings.Equal(masterRs):
			optState.OptimizingHosts = append(optState.OptimizingHosts, hostname)

		default:
			optState.DisabledHosts = append(optState.DisabledHosts, hostname)
		}
	}

	return optState, nil
}

func (opt *Optimizer) getDCSState(hostname string) (*DCSState, error) {
	path := dcs.JoinPath(pathOptimizationNodes, hostname)

	state := new(DCSState)
	err := opt.dcs.Get(path, state)
	if err != nil && err != dcs.ErrNotFound && err != dcs.ErrMalformed {
		return nil, err
	}

	return state, nil
}

type OptimizationState struct {
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
	master := c.GetState(c.GetMaster())
	if master.ReplicationSettings == nil {
		return fmt.Errorf("master replication settings are nil")
	}

	optState, err := opt.getOptimizationState(c)
	if err != nil {
		return err
	}

	nodesToDisable := Union(
		optState.OptimizedHosts,
		optState.MalfunctioningHosts,
	)
	err = opt.stopNodes(
		c,
		nodesToDisable,
		*master.ReplicationSettings,
	)
	if err != nil {
		return err
	}
	err = opt.deleteNodes(c, nodesToDisable)
	if err != nil {
		return err
	}

	switch {
	case len(optState.OptimizingHosts) > 1:
		opt.shuffle(optState.OptimizingHosts)
		err = opt.stopNodes(
			c,
			optState.OptimizingHosts[1:],
			*master.ReplicationSettings,
		)
		if err != nil {
			return err
		}

		host := optState.OptimizingHosts[0]
		err = opt.syncNodeOptions(host, c.GetNode(host))
		if err != nil {
			return err
		}

	case len(optState.OptimizingHosts) == 0 && len(optState.DisabledHosts) > 0:
		opt.shuffle(optState.DisabledHosts)
		err = opt.startNodes(c, optState.DisabledHosts[:1])
		if err != nil {
			return err
		}

	case len(optState.OptimizingHosts) == 1:
		host := optState.OptimizingHosts[0]
		err = opt.syncNodeOptions(host, c.GetNode(host))
		if err != nil {
			return err
		}
	}

	return nil
}

func (onp *Optimizer) startNodes(
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

func (onp *Optimizer) stopNodes(
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

func (onp *Optimizer) deleteNodes(
	c Cluster,
	hosts []string,
) error {
	for _, host := range hosts {
		err := onp.dcs.Delete(dcs.JoinPath(pathOptimizationNodes, host))
		if err != nil && err != dcs.ErrNotFound {
			return err
		}
	}
	return nil
}

func (onp *Optimizer) shuffle(hosts []string) {
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
}

func (onp *Optimizer) syncNodeOptions(
	host string,
	node NodeReplicationController,
) error {
	settings, err := node.GetReplicationSettings()
	if err != nil {
		return err
	}
	if settings.CanBeOptimized() {
		onp.logger.Warnf("Node %s should be optimizing but isn't - restarting optimization", host)
		return node.OptimizeReplication()
	}
	return nil
}
