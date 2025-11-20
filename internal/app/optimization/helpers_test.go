package optimization

import (
	"database/sql"

	gomock "github.com/golang/mock/gomock"
	"github.com/yandex/mysync/internal/mysql"
)

func MakeNodeMock(ctrl *gomock.Controller, name string) *MockNode {
	node := NewMockNode(ctrl)
	node.EXPECT().Host().Return(name).AnyTimes()

	return node
}

func (node *MockNode) WithGetReplicaStatus(lag float64) *gomock.Call {
	return node.EXPECT().GetReplicaStatus().
		Return(&mysql.ReplicaStatusStruct{
			Lag: sql.NullFloat64{Valid: true, Float64: lag},
		}, nil)
}

func (node *MockNode) WithGetReplicationSettings() *gomock.Call {
	return node.EXPECT().GetReplicationSettings().
		Return(mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		}, nil)
}

func (node *MockNode) WithSetReplicationSettings() *gomock.Call {
	return node.EXPECT().SetReplicationSettings(
		mysql.ReplicationSettings{
			InnodbFlushLogAtTrxCommit: 1,
			SyncBinlog:                1,
		})
}
