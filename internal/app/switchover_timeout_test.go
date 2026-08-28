package app

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
)

func TestNewSwitchoverAbortDeadline(t *testing.T) {
	initiatedAt := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		switchover Switchover
		want       bool
	}{
		{
			name:       "first attempt",
			switchover: Switchover{InitiatedAt: initiatedAt},
			want:       true,
		},
		{
			name: "legacy record started by a previous manager",
			switchover: Switchover{
				InitiatedAt: initiatedAt,
				StartedAt:   initiatedAt.Add(time.Minute),
			},
		},
		{
			name: "legacy retry",
			switchover: Switchover{
				InitiatedAt: initiatedAt,
				RunCount:    1,
			},
		},
		{
			name: "started by a previous manager while still abortable",
			switchover: Switchover{
				InitiatedAt: initiatedAt,
				StartedAt:   initiatedAt.Add(time.Minute),
				Abortable:   true,
			},
			want: true,
		},
		{
			name: "retry before topology change remains abortable",
			switchover: Switchover{
				InitiatedAt: initiatedAt,
				RunCount:    1,
				Abortable:   true,
			},
			want: true,
		},
		{
			name:       "legacy record without initiation time",
			switchover: Switchover{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := minConfig()
			cfg.SwitchoverTimeout = 10 * time.Minute
			app := newTestApp(t, cfg, nil)

			deadline := app.newSwitchoverAbortDeadline(&tt.switchover)
			if tt.switchover.StartedAt.IsZero() && tt.switchover.RunCount == 0 && !tt.switchover.InitiatedAt.IsZero() {
				require.True(t, tt.switchover.Abortable)
			}
			if !tt.want {
				require.Nil(t, deadline)
				return
			}
			require.NotNil(t, deadline)
			require.Equal(t, initiatedAt.Add(10*time.Minute), deadline.at)
			require.NoError(t, deadline.exceeded(deadline.at.Add(-time.Nanosecond)))
			err := deadline.exceeded(deadline.at)
			require.ErrorIs(t, err, ErrSwitchoverTimeout)
			require.EqualError(t, err, "switchover timed out after 10m0s")
		})
	}
}

func TestSwitchoverTimeoutDisabledAfterAbortBoundary(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 30, 0, 0, time.UTC)
	cfg := minConfig()
	cfg.SwitchoverTimeout = 10 * time.Minute
	app := newTestApp(t, cfg, nil)
	switchover := &Switchover{
		InitiatedAt: now.Add(-30 * time.Minute),
		StartedAt:   now.Add(-20 * time.Minute),
		RunCount:    1,
		Abortable:   false,
	}

	deadline := app.newSwitchoverAbortDeadline(switchover)
	require.Nil(t, deadline)
	require.NoError(t, deadline.exceeded(now))
	require.False(t, switchover.Abortable)
}

func TestMarkSwitchoverUnabortablePersistsBoundary(t *testing.T) {
	for _, abortable := range []bool{true, false} {
		t.Run(fmt.Sprintf("abortable_%t", abortable), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockDCS := NewMockIAppDCS(ctrl)
			mockDCS.EXPECT().SetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
				require.False(t, switchover.Abortable)
				require.Equal(t, int32(7), switchover.DCSVersion)
				switchover.DCSVersion = 8
				return nil
			})

			app := newTestApp(t, minConfig(), mockDCS)
			switchover := &Switchover{Abortable: abortable, DCSVersion: 7}
			require.NoError(t, app.markSwitchoverUnabortable(switchover))
			require.False(t, switchover.Abortable)
			require.Equal(t, int32(8), switchover.DCSVersion)
		})
	}
}

func TestWaitForCatchUpHonorsSwitchoverDeadline(t *testing.T) {
	app := newTestApp(t, minConfig(), nil)
	deadline := &switchoverAbortDeadline{
		at:      time.Now().Add(-time.Second),
		timeout: 10 * time.Minute,
	}

	caught, err := app.waitForCatchUp(nil, nil, time.Hour, time.Hour, deadline)
	require.False(t, caught)
	require.ErrorIs(t, err, ErrSwitchoverTimeout)
}

func TestRecordSwitchoverAttemptResultTimeoutIsTerminal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().DeleteCurrentSwitchoverVersion(int32(7)).Return(nil)
	mockDCS.EXPECT().SetLastRejectedSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		require.Equal(t, 0, switchover.RunCount)
		require.NotNil(t, switchover.Result)
		require.False(t, switchover.Result.Ok)
		require.Equal(t, "switchover timed out after 10m0s", switchover.Result.Error)
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	switchover := &Switchover{MasterTransition: FailoverTransition, DCSVersion: 7}
	err := app.recordSwitchoverAttemptResult(
		switchover,
		fmt.Errorf("%w after %s", ErrSwitchoverTimeout, 10*time.Minute),
	)
	require.NoError(t, err)
}

func TestRecordSwitchoverAttemptResultDoesNotDeleteNewManagerState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().DeleteCurrentSwitchoverVersion(int32(7)).Return(dcs.ErrVersionMismatch)

	app := newTestApp(t, minConfig(), mockDCS)
	err := app.recordSwitchoverAttemptResult(
		&Switchover{MasterTransition: FailoverTransition, DCSVersion: 7},
		fmt.Errorf("%w after %s", ErrSwitchoverTimeout, 10*time.Minute),
	)
	require.ErrorIs(t, err, dcs.ErrVersionMismatch)
}

func TestRecordSwitchoverAttemptResultRegularErrorIsRetried(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().SetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		require.Equal(t, 1, switchover.RunCount)
		require.NotNil(t, switchover.Result)
		require.False(t, switchover.Result.Ok)
		require.Equal(t, "temporary failure", switchover.Result.Error)
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	err := app.recordSwitchoverAttemptResult(&Switchover{}, errors.New("temporary failure"))
	require.NoError(t, err)
}
