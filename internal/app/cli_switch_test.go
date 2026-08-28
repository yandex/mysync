package app

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
)

func TestSafeAbortSwitchoverDeletesAbortableVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{Abortable: true, DCSVersion: 7}
		return nil
	})
	mockDCS.EXPECT().DeleteCurrentSwitchoverVersion(int32(7)).Return(nil)

	app := newTestApp(t, minConfig(), mockDCS)
	require.NoError(t, app.safeAbortSwitchover())
}

func TestSafeAbortSwitchoverRejectsUnabortableSwitch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{Abortable: false, DCSVersion: 7}
		return nil
	})

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.safeAbortSwitchover(), ErrSwitchoverNotAbortable)
}

func TestSafeAbortSwitchoverDoesNotDeleteChangedSwitch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).DoAndReturn(func(switchover *Switchover) error {
		*switchover = Switchover{Abortable: true, DCSVersion: 7}
		return nil
	})
	mockDCS.EXPECT().DeleteCurrentSwitchoverVersion(int32(7)).Return(dcs.ErrVersionMismatch)

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.safeAbortSwitchover(), dcs.ErrVersionMismatch)
}

func TestSafeAbortSwitchoverReturnsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDCS := NewMockIAppDCS(ctrl)
	mockDCS.EXPECT().GetCurrentSwitchover(gomock.Any()).Return(dcs.ErrNotFound)

	app := newTestApp(t, minConfig(), mockDCS)
	require.ErrorIs(t, app.safeAbortSwitchover(), dcs.ErrNotFound)
}
