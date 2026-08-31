package optimization

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/mysql"
)

//nolint:funlen
func TestWaitOptimization(t *testing.T) {
	defaultConfig := config.OptimizationConfig{
		LowReplicationMark:  5 * time.Second,
		HighReplicationMark: 120 * time.Second,
	}
	logger := zerolog.Nop()
	checkInterval := time.Millisecond

	t.Run("Waiting for an optimized replica", func(t *testing.T) {
		ctx := context.Background()

		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(1.0)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(defaultConfig, &logger, Dcs, checkInterval)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})

	t.Run("Waiting for a replica absent in DCS", func(t *testing.T) {
		ctx := context.Background()

		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(nil, nil)

		manager := NewController(defaultConfig, &logger, Dcs, checkInterval)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})

	t.Run("Timeout exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(1024.0).AnyTimes()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: StatusEnabled}, nil).AnyTimes()

		manager := NewController(defaultConfig, &logger, Dcs, checkInterval)

		err := manager.Wait(ctx, node)
		require.True(t, errors.Is(err, ErrDeadlineExceeded), "expected ErrDeadlineExceeded, got: %v", err)
	})

	t.Run("StatusNew keeps waiting until Syncer transitions to StatusEnabled", func(t *testing.T) {
		ctx := context.Background()
		checkInterval := time.Nanosecond

		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(4.0)

		Dcs := NewMockDCS(ctrl)
		gomock.InOrder(
			// First tick: Syncer hasn't started yet — Status="" → keep waiting
			Dcs.EXPECT().GetState("replica1").
				Return(&DCSState{Status: StatusNew}, nil),
			// Second tick: Syncer has applied settings — Status="enabled" → check lag
			Dcs.EXPECT().GetState("replica1").
				Return(&DCSState{Status: StatusEnabled}, nil),
		)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(defaultConfig, &logger, Dcs, checkInterval)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})

	t.Run("Waiting works", func(t *testing.T) {
		ctx := context.Background()
		// LowReplicationMark=5s: lags 800s and 200s fail the check, 4s passes → 3 iterations.
		checkInterval := time.Nanosecond

		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")
		node.WithGetReplicaStatus(800.0)
		node.WithGetReplicaStatus(200.0)
		node.WithGetReplicaStatus(4.0)

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetState("replica1").
			Return(&DCSState{Status: "enabled"}, nil).AnyTimes()
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(defaultConfig, &logger, Dcs, checkInterval)

		err := manager.Wait(ctx, node)
		require.NoError(t, err)
	})
}

func TestWaitOptimizationConsecutiveErrorsReset(t *testing.T) {
	// Intermittent errors between successful polls must not accumulate to a premature exit.
	// maxConsequentErrors=3: Wait() aborts only when >3 errors occur without a success between them.
	// A single successful poll (StatusNew → no error) resets the counter to 0.
	ctx := context.Background()
	checkInterval := time.Nanosecond
	logger := zerolog.Nop()
	cfg := config.OptimizationConfig{
		LowReplicationMark:  5 * time.Second,
		HighReplicationMark: 120 * time.Second,
	}

	ctrl := gomock.NewController(t)

	networkErr := fmt.Errorf("network-error")
	node := MakeNodeMock(ctrl, "replica1")
	// 3 errors in first group + 3 errors in second group = 6 total error returns
	node.EXPECT().GetReplicaStatus().Return(nil, networkErr).Times(6)
	node.WithGetReplicaStatus(4.0) // final tick: lag 4s < 5s → converged

	Dcs := NewMockDCS(ctrl)
	gomock.InOrder(
		// ticks 1-3: StatusEnabled, GetReplicaStatus errors → counter reaches 3
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		// tick 4: StatusNew → isOptimizedDuringWaiting returns (false, nil) → counter resets to 0
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusNew}, nil),
		// ticks 5-7: StatusEnabled, GetReplicaStatus errors again → counter reaches 3 (not >3)
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
		// tick 8: StatusNew → counter resets to 0 again
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusNew}, nil),
		// tick 9: StatusEnabled, lag converges → done
		Dcs.EXPECT().GetState("replica1").Return(&DCSState{Status: StatusEnabled}, nil),
	)
	Dcs.EXPECT().DeleteHosts("replica1")

	manager := NewController(cfg, &logger, Dcs, checkInterval)

	err := manager.Wait(ctx, node)
	require.NoError(t, err, "intermittent errors with resets must not abort Wait()")
}

func TestEnableNodeOptimization(t *testing.T) {
	emptyConfig := config.OptimizationConfig{}
	checkInterval := time.Second
	logger := zerolog.Nop()

	t.Run("Enable on a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1")

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.Enable(node)
		require.NoError(t, err)
	})

	t.Run("Network error", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		node := MakeNodeMock(ctrl, "replica1")

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().CreateHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.Enable(node)
		require.EqualError(t, err, "network-error")
	})
}

func TestDisableNodeOptimization(t *testing.T) {
	emptyConfig := config.OptimizationConfig{}
	checkInterval := time.Second
	logger := zerolog.Nop()

	t.Run("Disable a replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.Disable(master, node)
		require.NoError(t, err)
	})

	t.Run("Network error on the side of DCS", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.Disable(master, node)
		require.EqualError(t, err, "network-error")
	})

	t.Run("Network error on the side of MySQL", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		master := MakeNodeMock(ctrl, "master")
		master.EXPECT().GetReplicationSettings().
			Return(mysql.ReplicationSettings{}, fmt.Errorf("network-error"))

		node := MakeNodeMock(ctrl, "replica1")
		node.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().DeleteHosts("replica1").
			Return(fmt.Errorf("network-error"))

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.Disable(master, node)
		require.EqualError(t, err, "network-error")
	})
}

//nolint:funlen
func TestDisableAllNodeOptimization(t *testing.T) {
	emptyConfig := config.OptimizationConfig{}
	checkInterval := time.Second
	logger := zerolog.Nop()

	t.Run("Disable all replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)

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

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.NoError(t, err)
	})

	t.Run("Disable only one replica", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		master := MakeNodeMock(ctrl, "master")
		master.WithGetReplicationSettings()

		replica1 := MakeNodeMock(ctrl, "replica1")
		replica1.WithSetReplicationSettings()

		Dcs := NewMockDCS(ctrl)
		Dcs.EXPECT().GetHosts().
			Return([]string{"replica1", "replica2"}, nil)
		Dcs.EXPECT().DeleteHosts("replica1")

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.DisableAll(master, []Node{replica1})
		require.NoError(t, err)
	})

	t.Run("DCS network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

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

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.EqualError(t, err, "got the following errors: replica1:network-error,replica2:network-error")
	})

	t.Run("MySQL network-errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

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

		manager := NewController(emptyConfig, &logger, Dcs, checkInterval)

		err := manager.DisableAll(master, []Node{replica1, replica2})
		require.NoError(t, err)
	})
}
