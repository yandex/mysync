package dcs

import (
	"sync"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/app/optimization"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

func NewOptimizationClusterAdapter(
	cluster *mysql.Cluster,
	clusterState map[string]*nodestate.NodeState,
	master string,
) *OptimizationClusterAdapter {
	return &OptimizationClusterAdapter{
		cluster:      cluster,
		clusterState: clusterState,
		master:       master,
	}
}

type OptimizationClusterAdapter struct {
	clusterState map[string]*nodestate.NodeState
	cluster      *mysql.Cluster
	master       string
}

func (ocs *OptimizationClusterAdapter) GetNode(hostname string) optimization.Node {
	return ocs.cluster.Get(hostname)
}

func (ocs *OptimizationClusterAdapter) GetState(hostname string) nodestate.NodeState {
	ns, ok := ocs.clusterState[hostname]
	if !ok {
		return nodestate.NodeState{}
	}
	return *ns
}

func (ocs *OptimizationClusterAdapter) GetMaster() string {
	return ocs.master
}

func NewOptimizationDCSAdapter(
	Dcs dcs.DCS,
) *OptimizationDCSAdapter {
	return &OptimizationDCSAdapter{
		dcs:          Dcs,
		dcsInitiator: sync.Once{},
	}
}

type OptimizationDCSAdapter struct {
	dcs          dcs.DCS
	dcsInitiator sync.Once
}

func (oda *OptimizationDCSAdapter) initDcs() error {
	var err error

	oda.dcsInitiator.Do(func() {
		err = oda.dcs.Create(pathOptimizationNodes, "")
	})
	if err != nil {
		oda.dcsInitiator = sync.Once{}
	}

	return err
}

func (oda *OptimizationDCSAdapter) GetHosts() ([]string, error) {
	err := oda.initDcs()
	if err != nil {
		return nil, err
	}

	return oda.dcs.GetChildren(pathOptimizationNodes)
}

func (oda *OptimizationDCSAdapter) SetState(hostname string, value *optimization.DCSState) error {
	err := oda.initDcs()
	if err != nil {
		return err
	}

	path := dcs.JoinPath(pathOptimizationNodes, hostname)
	return oda.dcs.Set(path, value)
}

func (oda *OptimizationDCSAdapter) GetState(hostname string) (*optimization.DCSState, error) {
	err := oda.initDcs()
	if err != nil {
		return nil, err
	}

	state := new(optimization.DCSState)
	path := dcs.JoinPath(pathOptimizationNodes, hostname)

	err = oda.dcs.Get(path, state)
	if err != nil && err != dcs.ErrNotFound {
		return nil, err
	}
	if err == dcs.ErrNotFound {
		return nil, nil
	}
	return state, nil
}

func (oda *OptimizationDCSAdapter) DeleteHosts(hostnames ...string) error {
	err := oda.initDcs()
	if err != nil {
		return err
	}

	for _, hostname := range hostnames {
		path := dcs.JoinPath(pathOptimizationNodes, hostname)
		err := oda.dcs.Delete(path)
		if err != nil && err != dcs.ErrNotFound {
			return err
		}
	}
	return nil
}

func (oda *OptimizationDCSAdapter) CreateHosts(hostnames ...string) error {
	err := oda.initDcs()
	if err != nil {
		return err
	}

	state := new(optimization.DCSState)
	for _, hostname := range hostnames {
		path := dcs.JoinPath(pathOptimizationNodes, hostname)
		err := oda.dcs.Create(path, state)
		if err != nil && err != dcs.ErrExists {
			return err
		}
	}
	return nil
}
