//go:generate mockgen -source=deps.go -destination=mocks_test.go -package=optimization . DCS,Node,Logger,Cluster
package optimization

import (
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/mysql"
)

type Logger interface {
	Debugf(msg string, args ...any)
	Infof(msg string, args ...any)
	Warnf(msg string, args ...any)
	Errorf(msg string, args ...any)
	Fatalf(msg string, args ...any)
}

type DCS interface {
	Create(path string, value any) error
	Set(path string, value any) error
	Get(path string, dest any) error
	Delete(path string) error
	GetChildren(path string) ([]string, error)
}

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
