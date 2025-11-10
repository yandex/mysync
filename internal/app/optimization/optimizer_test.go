package optimization

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
)

func TestBasicOptimization(t *testing.T) {
	t.Run("Initialization works", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{}
		Dcs := NewMockDCS(ctrl)

		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any()).
			Return(zk.ErrNodeExists)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any()).
			Return(fmt.Errorf("network-error"))

		logger.EXPECT().Infof("Optimizer is initialized")
		logger.EXPECT().Infof("Optimizer is initialized")

		opt := NewOptimizer(logger, config)
		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		opt = NewOptimizer(logger, config)
		err = opt.Initialize(Dcs)
		require.EqualError(t, err, zk.ErrNodeExists.Error())

		opt = NewOptimizer(logger, config)
		err = opt.Initialize(Dcs)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Sync on master does nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
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
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{}, nil)

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on master with 'optimizing' dcs status turns optimization off", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
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
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"master"}, nil)
		Dcs.EXPECT().Get("optimization_nodes/master", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})
		Dcs.EXPECT().Delete("optimization_nodes/master")

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.NoError(t, err)
	})
}

//nolint:funlen
func TestHAClusterOptimization(t *testing.T) {
	t.Run("Sync works on hosts without optimization", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNodeReplicationController(ctrl)
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
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{}, nil)

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on hosts with optimization and without lag disables optimization", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNodeReplicationController(ctrl)
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
						ReplicationLag: Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})
		Dcs.EXPECT().Delete("optimization_nodes/replica1")

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.NoError(t, err)
	})

	t.Run("Sync on hosts with optimization and lag does nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNodeReplicationController(ctrl)
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
						ReplicationLag: Ptr(1024.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.NoError(t, err)
	})
}

func TestOneHostOptimizationPolicy(t *testing.T) {
	t.Run("Optimization can be enabled on just one host", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNodeReplicationController(ctrl)
		replica1.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1000, SyncBinlog: 2}, nil).AnyTimes()
		replica1.EXPECT().
			SetReplicationSettings(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			})

		replica2 := NewMockNodeReplicationController(ctrl)
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
						ReplicationLag: Ptr(1024.0),
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
						ReplicationLag: Ptr(1024.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})
		Dcs.EXPECT().Get("optimization_nodes/replica2", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
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

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1}, nil).AnyTimes()

		replica1 := NewMockNodeReplicationController(ctrl)
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
						ReplicationLag: Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{}, fmt.Errorf("network-error"))

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Sync network error on the MySQL replicas side keeps cluster unchanged", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := NewMockLogger(ctrl)
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}

		master := NewMockNodeReplicationController(ctrl)

		replica1 := NewMockNodeReplicationController(ctrl)
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
						ReplicationLag: Ptr(0.0),
					},
				}).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes", gomock.Any())
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"replica1"}, nil)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})

		opt := NewOptimizer(logger, config)

		logger.EXPECT().Infof("Optimizer is initialized")

		err := opt.Initialize(Dcs)
		require.NoError(t, err)

		err = opt.SyncState(cluster)
		require.EqualError(t, err, "network-error")
	})
}
