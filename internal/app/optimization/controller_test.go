package optimization

import (
	"context"
	"fmt"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
)

//nolint:funlen
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

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(1.0)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
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

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(nil, nil)

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
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

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(1024.0).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: ""}, nil).AnyTimes()

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
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

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(800.0)
		node.WithGetReplicaStatus(200.0)
		node.WithGetReplicaStatus(4.0)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil).AnyTimes()
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config,
			logger,
			Dcs,
			checkInterval,
		)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})
}

func TestEnableNodeOptimization(t *testing.T) {
	t.Run("Enable on a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.Enable(node)
		require.NoError(t, err)
	})

	t.Run("Network error", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.Enable(node)
		require.EqualError(t, err, "network-error")
	})
}

func TestDisableNodeOptimization(t *testing.T) {
	t.Run("Disable a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.Disable(master, node)
		require.NoError(t, err)
	})

	t.Run("Network error on the side of DCS", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.Disable(master, node)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Network error on the side of MySQL", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Warnf("cannot get replication setting from the master: %s", "network-error")

		master := MakeNodeMock(ctrl, "master")
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{}, fmt.Errorf("network-error"))

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.Disable(master, node)
		require.EqualError(t, err, "network-error")
	})
}

//nolint:funlen
func TestDisableAllNodeOptimization(t *testing.T) {
	t.Run("Disable all replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		replica1 := MakeNodeMock(ctrl, "replica1")
		replica1.WithSetReplicationSettings()

		replica2 := MakeNodeMock(ctrl, "replica2")
		replica2.WithSetReplicationSettings()

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
		)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.NoError(t, err)
	})

	t.Run("Disable only one replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Warnf("host %s was not found", "replica2")

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		replica1 := MakeNodeMock(ctrl, "replica1")
		replica1.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.DisableAll(master, []Node{replica1})
		require.NoError(t, err)
	})

	t.Run("DCS network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		replica1 := MakeNodeMock(ctrl, "replica1")
		replica1.WithSetReplicationSettings()

		replica2 := MakeNodeMock(ctrl, "replica2")
		replica2.WithSetReplicationSettings()

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
		)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.EqualError(t, err, "got the following errors: replica1:network-error,replica2:network-error")
	})

	t.Run("MySQL network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logger := NewMockLogger(ctrl)
		logger.EXPECT().Warnf("cannot get replication setting from the master: %s", "network-error")

		master := MakeNodeMock(ctrl, "master")
		master.EXPECT().GetReplicationSettings().Return(mysql.ReplicationSettings{}, fmt.Errorf("network-error"))

		replica1 := MakeNodeMock(ctrl, "replica1")
		replica1.WithSetReplicationSettings()

		replica2 := MakeNodeMock(ctrl, "replica2")
		replica2.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{}, fmt.Errorf("network-error"))
		Dcs.EXPECT().DeleteHosts("replica1")
		Dcs.EXPECT().DeleteHosts("replica2")

		manager := NewController(
			config.OptimizationConfig{},
			logger,
			Dcs,
			time.Second,
		)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.NoError(t, err)
	})
}
