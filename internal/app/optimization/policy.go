package optimization

import (
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/dcs"
)

type Policy interface {
	Apply(
		master NodeReplicationController,
		nodeStates map[string]*nodestate.NodeState,
		dcsStatuses map[string]Status,
		nodes map[string]NodeReplicationController,
	) error

	Initialize(DCS dcs.DCS) error
}
