package optimization

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func TestEnableForRelayLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zerolog.Nop()
	node := MakeNodeMock(ctrl, "replica1")

	Dcs := NewMockDCS(ctrl)
	Dcs.EXPECT().GetState("replica1").Return(nil, nil)
	Dcs.EXPECT().SetState("replica1", &DCSState{Reason: ReasonRelayLog})

	manager := NewController(config.OptimizationConfig{}, &logger, Dcs, time.Second)
	err := manager.EnableForRelayLog(node)
	require.NoError(t, err)
}

func TestRelayLogOptimization(t *testing.T) {
	config := config.OptimizationConfig{
		LowReplicationMark:  60 * time.Second,
		HighReplicationMark: 120 * time.Second,
	}
	logger := zerolog.Nop()

	t.Run("Large relay logs enable optimization despite low lag", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		replica := MakeNodeMock(ctrl, "replica1")
		replica.EXPECT().GetReplicaStatus().Return(&mysql.ReplicaStatusStruct{RelayLogSpace: 2 << 30}, nil)
		replica.EXPECT().OptimizeReplication()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().Return("master")
		cluster.EXPECT().GetState("master").Return(nodestate.NodeState{ReplicationSettings: &mysql.SafeReplicationSettings})
		cluster.EXPECT().GetNode("replica1").Return(replica).AnyTimes()
		cluster.EXPECT().GetState("replica1").Return(nodestate.NodeState{
			ReplicationSettings: &mysql.SafeReplicationSettings,
			SlaveState:          &nodestate.SlaveState{ReplicationLag: util.Ptr(30.0)},
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Reason: ReasonRelayLog}, nil).Times(2)
		Dcs.EXPECT().SetState("replica1", &DCSState{Status: StatusEnabled, Reason: ReasonRelayLog})

		opt := NewSyncer(&logger, config, 1<<30, Dcs)
		err := opt.Sync(cluster)
		require.NoError(t, err)
	})

	t.Run("Large relay logs keep optimization enabled despite low lag", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		settings := mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 2, SyncBinlog: 1000}
		replica := MakeNodeMock(ctrl, "replica1")
		replica.EXPECT().GetReplicaStatus().Return(&mysql.ReplicaStatusStruct{RelayLogSpace: 2 << 30}, nil)
		replica.EXPECT().GetReplicationSettings().Return(settings, nil)

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().Return("master")
		cluster.EXPECT().GetState("master").Return(nodestate.NodeState{ReplicationSettings: &mysql.SafeReplicationSettings})
		cluster.EXPECT().GetNode("replica1").Return(replica).AnyTimes()
		cluster.EXPECT().GetState("replica1").Return(nodestate.NodeState{
			ReplicationSettings: &settings,
			SlaveState:          &nodestate.SlaveState{ReplicationLag: util.Ptr(30.0)},
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled, Reason: ReasonRelayLog}, nil)

		opt := NewSyncer(&logger, config, 1<<30, Dcs)
		err := opt.Sync(cluster)
		require.NoError(t, err)
	})

	t.Run("Small relay logs and low lag disable optimization", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		replica := MakeNodeMock(ctrl, "replica1")
		replica.EXPECT().GetReplicaStatus().Return(&mysql.ReplicaStatusStruct{RelayLogSpace: 512 << 20}, nil)
		replica.WithSetReplicationSettings()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().Return("master")
		cluster.EXPECT().GetState("master").Return(nodestate.NodeState{ReplicationSettings: &mysql.SafeReplicationSettings})
		cluster.EXPECT().GetNode("replica1").Return(replica).AnyTimes()
		cluster.EXPECT().GetState("replica1").Return(nodestate.NodeState{
			ReplicationSettings: &mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 2, SyncBinlog: 1000},
			SlaveState:          &nodestate.SlaveState{ReplicationLag: util.Ptr(30.0)},
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled, Reason: ReasonRelayLog}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		opt := NewSyncer(&logger, config, 1<<30, Dcs)
		err := opt.Sync(cluster)
		require.NoError(t, err)
	})
}
