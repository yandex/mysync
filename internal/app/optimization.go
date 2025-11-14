package app

import (
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/app/optimization"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

func NewOptimizationClusterAdapter(
	cluster *mysql.Cluster,
	clusterState map[string]*nodestate.NodeState,
	master string,
) optimization.Cluster {
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
) (optimization.DCS, error) {
	err := Dcs.Create(pathOptimizationNodes, "")
	if err != nil && err != dcs.ErrExists {
		return nil, err
	}
	return &OptimizationDCSAdapter{
		dcs: Dcs,
	}, nil
}

type OptimizationDCSAdapter struct {
	dcs dcs.DCS
}

func (oda *OptimizationDCSAdapter) GetHosts() ([]string, error) {
	return oda.dcs.GetChildren(pathOptimizationNodes)
}

func (oda *OptimizationDCSAdapter) SetState(hostname string, value *optimization.DCSState) error {
	path := dcs.JoinPath(pathOptimizationNodes, hostname)
	return oda.dcs.Set(path, value)
}

func (oda *OptimizationDCSAdapter) GetState(hostname string) (*optimization.DCSState, error) {
	state := new(optimization.DCSState)
	path := dcs.JoinPath(pathOptimizationNodes, hostname)

	err := oda.dcs.Get(path, state)
	if err != nil && err != dcs.ErrNotFound {
		return nil, err
	}
	if err == dcs.ErrNotFound {
		return nil, nil
	}
	return state, nil
}

func (oda *OptimizationDCSAdapter) DeleteHosts(hostnames ...string) error {
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
