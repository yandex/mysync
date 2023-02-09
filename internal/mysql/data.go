package mysql

import (
	"database/sql"
	"fmt"
)

const yes = "Yes" // suddenly

const (
	ReplicationRunning = "running"
	ReplicationStopped = "stopped"
	ReplicationError   = "error"
)

type pingResult struct {
	Ok int `db:"Ok"`
}

type readOnlyResult struct {
	ReadOnly      int `db:"ReadOnly"`
	SuperReadOnly int `db:"SuperReadOnly"`
}

// CascadeNodeConfiguration is a dcs node configuration for cascade mysql replica
type CascadeNodeConfiguration struct {
	// StreamFrom - is a host to stream from. Can be changed from CLI.
	StreamFrom string `json:"stream_from"`
}

// NodeConfiguration is a dcs node configuration for HA mysql replica
type NodeConfiguration struct {
	// Priority - is a host priority to become master. Can be changed from CLI.
	Priority int64 `json:"priority"`
}

// SlaveStatus contains SHOW SLAVE/REPLICA STATUS response
type SlaveStatus struct {
	MasterHost       string `db:"Master_Host"`
	MasterPort       int    `db:"Master_Port"`
	MasterLogFile    string `db:"Master_Log_File"`
	ReadMasterLogPos int64  `db:"Read_Master_Log_Pos"`
	SlaveIORunning   string `db:"Slave_IO_Running"`
	SlaveSQLRunning  string `db:"Slave_SQL_Running"`
	RetrievedGtidSet string `db:"Retrieved_Gtid_Set"`
	ExecutedGtidSet  string `db:"Executed_Gtid_Set"`
	LastIOErrno      int    `db:"Last_IO_Errno"`
	LastSQLErrno     int    `db:"Last_SQL_Errno"`
}

// SemiSyncStatus contains semi sync host settings
type SemiSyncStatus struct {
	MasterEnabled  int `db:"MasterEnabled"`
	SlaveEnabled   int `db:"SlaveEnabled"`
	WaitSlaveCount int `db:"WaitSlaveCount"`
}

// ReplicationIORunning ...
func (ss *SlaveStatus) ReplicationIORunning() bool {
	return ss.SlaveIORunning == yes
}

// ReplicationSQLRunning ...
func (ss *SlaveStatus) ReplicationSQLRunning() bool {
	return ss.SlaveSQLRunning == yes
}

// ReplicationRunning is true when both IO and SQL threads running
func (ss *SlaveStatus) ReplicationRunning() bool {
	return ss.ReplicationIORunning() && ss.ReplicationSQLRunning()
}

// ReplicationState ...
func (ss *SlaveStatus) ReplicationState() string {
	switch {
	case ss.SlaveIORunning == yes && ss.SlaveSQLRunning == yes:
		return ReplicationRunning
	case (ss.SlaveIORunning != yes || ss.SlaveSQLRunning != yes) && (ss.LastIOErrno == 0 && ss.LastSQLErrno == 0):
		return ReplicationStopped
	default:
		return ReplicationError
	}
}

// GTIDExecuted contains SHOW MASTER STATUS response
type GTIDExecuted struct {
	ExecutedGtidSet string `db:"Executed_Gtid_Set"`
}

// Binlog represents SHOW BINARY LOGS result item
type Binlog struct {
	Name string `db:"Log_name"`
	Size int64  `db:"File_size"`
}

type replicationLag struct {
	Lag sql.NullFloat64 `db:"Seconds_Behind_Master"`
}

type Event struct {
	Schema  string `db:"EVENT_SCHEMA"`
	Name    string `db:"EVENT_NAME"`
	Definer string `db:"DEFINER"`
}

// offlineModeStatus contains OfflineMode variable
type offlineModeStatus struct {
	OfflineMode int `db:"OfflineMode"`
}

func (ev Event) String() string {
	return fmt.Sprintf("`%s`.`%s`", ev.Schema, ev.Name)
}

type version struct {
	MajorVersion string `db:"MajorVersion"`
	FullVersion  string `db:"FullVersion"`
}

const (
	Version80              = "8.0"
	Version80ReplicaStatus = "8.0.22"
	Version57              = "5.7"
)

func (v *version) GetSlaveStatusQuery() string {
	switch v.MajorVersion {
	case Version80:
		if v.FullVersion >= Version80ReplicaStatus {
			return queryReplicaStatus
		}
		return querySlaveStatus
	case Version57:
		return querySlaveStatus
	default:
		return queryReplicaStatus
	}
}
