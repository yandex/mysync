//go:generate mockgen -source=deps.go -destination=mocks_test.go -package=optimization . DCS,Node,Cluster
package optimization

import (
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql"
)

type DCS interface {
	GetHosts() ([]string, error)

	SetState(hostname string, value *DCSState) error
	GetState(hostname string) (*DCSState, error)

	DeleteHosts(hostname ...string) error
	CreateHosts(hostname ...string) error
}

type DCSState struct {
	Status Status `json:"status"`
}

type Status string

const (
	StatusNew     Status = ""
	StatusEnabled Status = "enabled"
)

type Node interface {
	SetReplicationSettings(rs mysql.ReplicationSettings) error
	GetReplicationSettings() (mysql.ReplicationSettings, error)

	OptimizeReplication() error

	GetReplicaStatus() (mysql.ReplicaStatus, error)

	Host() string
}

type Cluster interface {
	GetNode(hostname string) Node
	GetState(hostname string) nodestate.NodeState
	GetMaster() string
}
