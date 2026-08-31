package app

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
)

func TestIssueFailoverStartsTimingAfterCreatingSwitchover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	rawDCS := newTimingTestDCS()
	var created Switchover
	mockDCS.EXPECT().CreateCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		created = *switchover
		return nil
	})

	cfg := minConfig()
	cfg.Hostname = "manager"
	app := newTestApp(t, cfg, mockDCS)
	app.dcs = rawDCS

	require.NoError(t, app.IssueFailover("old-master"))
	require.Equal(t, "old-master", created.From)
	require.Equal(t, "manager", created.InitiatedBy)
	require.Equal(t, CauseAuto, created.Cause)
	require.Equal(t, FailoverTransition, created.MasterTransition)

	startedAt, ok := app.getTimingStart(timingFailover)
	require.True(t, ok)
	require.True(t, created.InitiatedAt.Equal(startedAt))
}

func TestIssueFailoverDoesNotStartTimingWhenCreationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	createErr := errors.New("failed to create switchover")
	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().CreateCurrentSwitchover(gomock.Any()).Return(createErr)

	app := newTestApp(t, minConfig(), mockDCS)
	app.dcs = newTimingTestDCS()

	require.ErrorIs(t, app.IssueFailover("old-master"), createErr)
	_, ok := app.getTimingStart(timingFailover)
	require.False(t, ok)
}

func TestFinishFailoverStopsTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().DeleteCurrentSwitchover().Return(nil)
	mockDCS.EXPECT().SetLastSwitchover(gomock.Any()).Return(nil)

	app := newTestApp(t, minConfig(), mockDCS)
	app.dcs = newTimingTestDCS()
	app.startTiming(timingFailover, time.Now())

	switchover := &Switchover{MasterTransition: FailoverTransition}
	require.NoError(t, app.FinishSwitchover(switchover, nil))
	_, ok := app.getTimingStart(timingFailover)
	require.False(t, ok)
}

func TestRejectedFailoverClearsTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().DeleteCurrentSwitchover().Return(nil)
	mockDCS.EXPECT().SetLastRejectedSwitchover(gomock.Any()).Return(nil)

	app := newTestApp(t, minConfig(), mockDCS)
	app.dcs = newTimingTestDCS()
	app.startTiming(timingFailover, time.Now())

	switchover := &Switchover{MasterTransition: FailoverTransition}
	require.NoError(t, app.FinishSwitchover(switchover, errors.New("failover rejected")))
	_, ok := app.getTimingStart(timingFailover)
	require.False(t, ok)
}

type timingTestDCS struct {
	values map[string][]byte
}

func newTimingTestDCS() *timingTestDCS {
	return &timingTestDCS{values: make(map[string][]byte)}
}

func (d *timingTestDCS) IsConnected() bool                    { return true }
func (d *timingTestDCS) WaitConnected(time.Duration) bool     { return true }
func (d *timingTestDCS) Initialize()                          {}
func (d *timingTestDCS) SetDisconnectCallback(func() error)   {}
func (d *timingTestDCS) AcquireLock(string) bool              { return true }
func (d *timingTestDCS) ReleaseLock(string)                   {}
func (d *timingTestDCS) Close()                               {}
func (d *timingTestDCS) GetTree(string) (any, error)          { return nil, dcs.ErrNotFound }
func (d *timingTestDCS) GetChildren(string) ([]string, error) { return nil, dcs.ErrNotFound }

func (d *timingTestDCS) Create(path string, value any) error {
	if _, ok := d.values[path]; ok {
		return dcs.ErrExists
	}
	return d.Set(path, value)
}

func (d *timingTestDCS) CreateEphemeral(path string, value any) error {
	return d.Create(path, value)
}

func (d *timingTestDCS) Set(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	d.values[path] = data
	return nil
}

func (d *timingTestDCS) SetEphemeral(path string, value any) error {
	return d.Set(path, value)
}

func (d *timingTestDCS) Get(path string, dest any) error {
	data, ok := d.values[path]
	if !ok {
		return dcs.ErrNotFound
	}
	return json.Unmarshal(data, dest)
}

func (d *timingTestDCS) Delete(path string) error {
	delete(d.values, path)
	return nil
}
