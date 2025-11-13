package optimization

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

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

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 1.0},
			}, nil)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Wait(ctx, node)
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

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(nil, nil)

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Wait(ctx, node)
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
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 1024.0).AnyTimes()

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().GetReplicaStatus().
			Return(&mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 1024.0},
			}, nil).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: ""}, nil).AnyTimes()

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Wait(ctx, node)
		require.EqualError(t, err, "optimization waiting deadline exceeded")
	})

	t.Run("Waiting works", func(t *testing.T) {
		ctx := context.Background()
		config := config.OptimizationConfig{
			LowReplicationMark:  5,
			HighReplicationMark: 120,
		}
		checkInterval := time.Nanosecond

		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 800.0)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting; replication lag is %f", 200.0)
		logger.EXPECT().Infof("optimization: waiting; node is optimizing")
		logger.EXPECT().Infof("optimization: waiting is complete, as replication lag is converged: %s", 4.0)
		logger.EXPECT().Infof("optimization: waiting is complete")

		node := NewMockNode(ctrl)
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
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil).AnyTimes()
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})
}

func TestEnableNodeOptimization(t *testing.T) {
	t.Run("Enable on a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Enable(node)
		require.NoError(t, err)
	})

	t.Run("Network error", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Enable(node)
		require.EqualError(t, err, "network-error")
	})
}

func TestDisableNodeOptimization(t *testing.T) {
	t.Run("Disable a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Disable(node)
		require.NoError(t, err)
	})

	t.Run("Network error on the side of DCS", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := NewMockNode(ctrl)
		node.EXPECT().Host().Return("replica1").AnyTimes()
		node.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.Disable(node)
		require.EqualError(t, err, "network-error")
	})
}

func TestDisableAllNodeOptimization(t *testing.T) {
	t.Run("Disable all replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().Host().Return("replica1").AnyTimes()
		replica1.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		replica2 := NewMockNode(ctrl)
		replica2.EXPECT().Host().Return("replica2").AnyTimes()
		replica2.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")
		Dcs.EXPECT().DeleteHosts("replica2")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.DisableAll(
			[]Node{replica1, replica2},
		)
		require.NoError(t, err)
	})

	t.Run("Disable only one replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Warnf("host %s was not found", "replica2")

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().Host().Return("replica1").AnyTimes()
		replica1.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		replica2 := NewMockNode(ctrl)
		replica2.EXPECT().Host().Return("replica2").AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.DisableAll(
			[]Node{replica1},
		)
		require.NoError(t, err)
	})

	t.Run("DCS network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		replica1 := NewMockNode(ctrl)
		replica1.EXPECT().Host().Return("replica1").AnyTimes()
		replica1.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		replica2 := NewMockNode(ctrl)
		replica2.EXPECT().Host().Return("replica2").AnyTimes()
		replica2.EXPECT().SetReplicationSettings(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{}, fmt.Errorf("network-error"))
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))
		Dcs.EXPECT().DeleteHosts("replica2").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
			mysql.ReplicationSettings{InnodbFlushLogAtTrxCommit: 1, SyncBinlog: 1},
		)

		err := manager.DisableAll(
			[]Node{replica1, replica2},
		)
		require.EqualError(t, err, "got the following errors: replica1:network-error,replica2:network-error")
	})
}
