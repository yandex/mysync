package app

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
)

func TestRequestSafeAbortPersistsRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{OperationID: "op-a", Abortable: true, DCSVersion: 7}
		return nil
	})
	mockDCS.EXPECT().SetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		require.Equal(t, "op-a", switchover.OperationID)
		require.True(t, switchover.AbortRequested)
		require.Equal(t, "operator@test", switchover.AbortRequestedBy)
		require.NotNil(t, switchover.AbortRequestedAt)
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	require.NoError(t, app.requestSafeAbort("operator@test"))
}

func TestRequestSafeAbortRejectsUnabortableSwitch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{Abortable: false, DCSVersion: 7}
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.requestSafeAbort("operator@test"), ErrSwitchoverNotAbortable)
}

func TestRequestSafeAbortDoesNotOverwriteChangedSwitch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{OperationID: "op-a", Abortable: true, DCSVersion: 7}
		return nil
	})
	mockDCS.EXPECT().SetCurrentSwitchover(gomock.Any()).Return(dcs.ErrVersionMismatch)

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.requestSafeAbort("operator@test"), dcs.ErrVersionMismatch)
}

func TestRequestSafeAbortReturnsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).Return(dcs.ErrNotFound)

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.requestSafeAbort("operator@test"), dcs.ErrNotFound)
}

func TestRequestSafeAbortIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{OperationID: "op-a", Abortable: true, AbortRequested: true, DCSVersion: 8}
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	require.NoError(t, app.requestSafeAbort("operator@test"))
}
