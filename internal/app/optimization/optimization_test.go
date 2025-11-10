package optimization

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
)

func TestWaitOptimization(t *testing.T) {
	t.Run("Waiting for an optimized replica", func(t *testing.T) {
		ctx := context.Background()
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}
		checkInterval := time.Millisecond

		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting is complete, as replication lag is converged: %s", 1.0)
		logger.EXPECT().Infof("optimization: waiting is complete")

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 1.0},
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			})
		Dcs.EXPECT().Delete("optimization_nodes/replica1")

		err := WaitOptimization(ctx, config, logger, node, checkInterval, Dcs)
		require.NoError(t, err)
	})

	t.Run("Waiting for a replica absent in DCS", func(t *testing.T) {
		ctx := context.Background()
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}
		checkInterval := time.Millisecond

		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: waiting is complete")

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				return zk.ErrNoNode
			})

		err := WaitOptimization(ctx, config, logger, node, checkInterval, Dcs)
		require.NoError(t, err)
	})

	t.Run("Timeout exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}
		checkInterval := time.Millisecond

		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing").AnyTimes()
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 1.0).AnyTimes()

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 1.0},
			}, nil).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = ""
				return nil
			}).AnyTimes()

		err := WaitOptimization(ctx, config, logger, node, checkInterval, Dcs)
		require.EqualError(t, err, "optimization waiting deadline exceeded")
	})

	t.Run("Waiting works", func(t *testing.T) {
		ctx := context.Background()
		config := config.OptimizationConfig{
			LowReplicationMark:  5 * time.Second,
			HighReplicationMark: 120 * time.Second,
		}
		checkInterval := time.Millisecond

		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 800.0)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 200.0)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting is complete, as replication lag is converged: %s", 4.0)
		logger.EXPECT().Infof("optimization: waiting is complete")

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 800.0},
			}, nil)
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 200.0},
			}, nil)
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 4.0},
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Get("optimization_nodes/replica1", gomock.Any()).
			DoAndReturn(func(path string, dest any) error {
				ptr, _ := dest.(*DCSState)
				ptr.Status = "enabled"
				return nil
			}).AnyTimes()
		Dcs.EXPECT().Delete("optimization_nodes/replica1")

		err := WaitOptimization(ctx, config, logger, node, checkInterval, Dcs)
		require.NoError(t, err)
	})
}

func TestEnableNodeOptimization(t *testing.T) {
	t.Run("Enable on a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes/replica1", gomock.Any())

		err := EnableNodeOptimization(node, Dcs)
		require.NoError(t, err)
	})

	t.Run("Network error", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Create("optimization_nodes/replica1", gomock.Any()).
			Return(fmt.Errorf("network-error"))

		err := EnableNodeOptimization(node, Dcs)
		require.EqualError(t, err, "network-error")
	})
}

func TestDisableNodeOptimization(t *testing.T) {
	t.Run("Disable a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Delete("optimization_nodes/replica1")

		err := DisableNodeOptimization(master, node, Dcs)
		require.NoError(t, err)
	})

	t.Run("Network error on the side of DCS", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Delete("optimization_nodes/replica1").
			Return(fmt.Errorf("network-error"))

		err := DisableNodeOptimization(master, node, Dcs)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Network error on the side of MySQL master", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := NewMockNodeReplicationController(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{}, fmt.Errorf("network-error"))

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().Delete("optimization_nodes/replica1")

		err := DisableNodeOptimization(master, node, Dcs)
		require.NoError(t, err)
	})
}

func TestDisableAllNodeOptimization(t *testing.T) {
	t.Run("Disable all replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		replica1 := NewMockNodeReplicationController(ctrl)
		replica1.EXPECT().Host().Return("replica1").AnyTimes()
		replica1.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		replica2 := NewMockNodeReplicationController(ctrl)
		replica2.EXPECT().Host().Return("replica2").AnyTimes()
		replica2.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().Delete("optimization_nodes/replica1")
		Dcs.EXPECT().Delete("optimization_nodes/replica2")

		err := DisableAllNodeOptimization(
			master,
			[]NodeReplicationController{replica1, replica2},
			Dcs,
		)
		require.NoError(t, err)
	})

	t.Run("Dcs network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		replica1 := NewMockNodeReplicationController(ctrl)
		replica1.EXPECT().Host().Return("replica1").AnyTimes()
		replica1.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		replica2 := NewMockNodeReplicationController(ctrl)
		replica2.EXPECT().Host().Return("replica2").AnyTimes()
		replica2.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		master := NewMockNodeReplicationController(ctrl)
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{
				InnodbFlushLogAtTrxCommit: 1,
				SyncBinlog:                1,
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetChildren("optimization_nodes").
			Return([]string{}, fmt.Errorf("network-error"))
		Dcs.EXPECT().Delete("optimization_nodes/replica1").
			Return(fmt.Errorf("network-error"))
		Dcs.EXPECT().Delete("optimization_nodes/replica2").
			Return(fmt.Errorf("network-error"))

		err := DisableAllNodeOptimization(
			master,
			[]NodeReplicationController{replica1, replica2},
			Dcs,
		)
		require.EqualError(t, err, "got the following errors: network-error,replica1:network-error,replica2:network-error")
	})
}
