package optimization

import (
	"github.com/yandex/mysync/internal/mysql"
)

type MockNode struct {
	replicationSettings mysql.ReplicationSettings
	replicationStatus   mysql.ReplicaStatusStruct
	hostname            string
}

func (mn *MockNode) SetReplicationSettings(rs mysql.ReplicationSettings) error {
	mn.replicationSettings = rs
	return nil
}

func (mn *MockNode) GetReplicationSettings() (mysql.ReplicationSettings, error) {
	return mn.replicationSettings, nil
}

func (mn *MockNode) OptimizeReplication() error {
	mn.replicationSettings.InnodbFlushLogAtTrxCommit = mysql.OptimalInnodbFlushLogAtTrxCommitValue
	mn.replicationSettings.SyncBinlog = mysql.OptimalSyncBinlogValue
	return nil
}

func (mn *MockNode) GetReplicaStatus() (mysql.ReplicaStatus, error) {
	return &mn.replicationStatus, nil
}

func (mn *MockNode) Host() string {
	return mn.hostname
}
