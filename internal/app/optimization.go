package app

import (
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/app/optimization"
	"github.com/yandex/mysync/internal/mysql"
)

func NewOptimizationClusterAdapter(
	cluster *mysql.Cluster,
	clusterState map[string]*nodestate.NodeState,
) optimization.Cluster {
	return &OptimizationClusterAdapter{
		cluster:      cluster,
		clusterState: clusterState,
	}
}

type OptimizationClusterAdapter struct {
	clusterState map[string]*nodestate.NodeState
	cluster      *mysql.Cluster
}

func (ocs *OptimizationClusterAdapter) GetNode(hostname string) optimization.NodeReplicationController {
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
	return ocs.GetMaster()
}
