package optimization

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/util"
)

func TestBasicOptimization(t *testing.T) {
	t.Run("Sync on master does nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [], malfunc: []>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on master with 'optimizing' dcs status turns optimization off", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [], malfunc: [master]>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()
		master.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			})

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"master"}, nil)
		Dcs.EXPECT().GetState("master").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("master")

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})
}

//nolint:funlen
func TestHAClusterOptimization(t *testing.T) {
	t.Run("Sync works on hosts without optimization", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [], malfunc: []>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on hosts with optimization and without lag disables optimization", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [replica1], malfunc: []>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 2, SyncBinlog: 1000}, nil).AnyTimes()
		replica1.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			})

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on hosts with optimization and lag does nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [replica1], optimized: [], malfunc: []>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1000, SyncBinlog: 2}, nil).AnyTimes()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()
		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(1024.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})
}

func TestOneHostOptimizationPolicy(t *testing.T) {
	t.Run("Optimization can be enabled on just one host", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [replica1 replica2], optimized: [], malfunc: []>")
		logger.EXPECT().Infof("optimization: there are too many nodes: %d. Turn %d off", 2, 1)

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1000, SyncBinlog: 2}, nil).AnyTimes()
		replica1.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			})

		replica2 := NewMockNode(ctrl)
		replica2.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1000, SyncBinlog: 2}, nil).AnyTimes()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(1024.0),
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica2").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica2").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(1024.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().GetState("replica2").
			Return(&DCSState{Status: "enabled"}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})
}

func TestTurnBackOnOptimization(t *testing.T) {
	t.Run("Optimization will be enabled if the host options were changed manually", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [replica1], optimized: [], malfunc: []>")
		logger.EXPECT().Warnf("Node %s should be optimizing but is not - restarting optimization", "replica1")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()
		replica1.EXPECT().
			OptimizeReplication()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(1024.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})
}

func TestNetworkErrors(t *testing.T) {
	t.Run("Sync network error on the DCS side keeps cluster unchanged", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 2, SyncBinlog: 1000}, nil).AnyTimes()

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{}, fmt.Errorf("network-error"))

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Sync network error on the MySQL replicas side keeps cluster unchanged", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [replica1], malfunc: []>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			}).Return(fmt.Errorf("network-error"))

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: &nodestate.SlaveState{
						ReplicationLag: util.Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.EqualError(t, err, "network-error")
	})
}

func TestDeadReplica(t *testing.T) {
	t.Run("Sync an optimizing dead replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: %s", "<disabled: [], optimizing: [], optimized: [], malfunc: [replica1]>")

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNode(ctrl)

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			})

		cluster := NewMockCluster(ctrl)
		cluster.EXPECT().GetMaster().
			Return("master").AnyTimes()
		cluster.EXPECT().GetNode("master").
			Return(master).AnyTimes()
		cluster.EXPECT().GetState("master").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 1,
						SyncBinlog:                1,
					},
				}).AnyTimes()

		cluster.EXPECT().GetNode("replica1").
			Return(replica1).AnyTimes()
		cluster.EXPECT().GetState("replica1").
			Return(
				nodestate.NodeState{
					ReplicationSettings: &mysql.ReplicationSettings{
						InnodbFlushLogAtTrxCommit: 2,
						SyncBinlog:                1000,
					},
					SlaveState: nil,
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		opt, err := NewSyncer(logger, config, Dcs)
		require.NoError(t, err)

		err = opt.Sync(cluster)
		require.NoError(t, err)
	})
}
