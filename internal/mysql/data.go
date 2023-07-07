package mysql

import (
	"database/sql"
	"fmt"
	"time"
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

type ResetupStatus struct {
	Status     bool
	UpdateTime time.Time
}

type replicationSettings struct {
	ChannelName    string `db:"ChannelName"`
	SourceHost     string `db:"SourceHost"`
	SourceUser     string `db:"SourceUser"`
	SourcePassword string `db:"SourcePassword"`
	SourcePort     int    `db:"SourcePort"`
	SourceSslCa    string `db:"SourceSslCa"`
	SourceDelay    int    `db:"SourceDelay"`
}

// SlaveStatusStruct contains SHOW SLAVE STATUS response
type SlaveStatusStruct struct {
	MasterHost       string          `db:"Master_Host"`
	MasterPort       int             `db:"Master_Port"`
	MasterLogFile    string          `db:"Master_Log_File"`
	ReadMasterLogPos int64           `db:"Read_Master_Log_Pos"`
	SlaveIORunning   string          `db:"Slave_IO_Running"`
	SlaveSQLRunning  string          `db:"Slave_SQL_Running"`
	RetrievedGtidSet string          `db:"Retrieved_Gtid_Set"`
	ExecutedGtidSet  string          `db:"Executed_Gtid_Set"`
	LastIOErrno      int             `db:"Last_IO_Errno"`
	LastSQLErrno     int             `db:"Last_SQL_Errno"`
	Lag              sql.NullFloat64 `db:"Seconds_Behind_Master"`
}

// ReplicaStatusStruct contains SHOW REPLICA STATUS response
type ReplicaStatusStruct struct {
	SourceHost        string          `db:"Source_Host"`
	SourcePort        int             `db:"Source_Port"`
	SourceLogFile     string          `db:"Source_Log_File"`
	ReadSourceLogPos  int64           `db:"Read_Source_Log_Pos"`
	ReplicaIORunning  string          `db:"Replica_IO_Running"`
	ReplicaSQLRunning string          `db:"Replica_SQL_Running"`
	RetrievedGtidSet  string          `db:"Retrieved_Gtid_Set"`
	ExecutedGtidSet   string          `db:"Executed_Gtid_Set"`
	LastIOErrno       int             `db:"Last_IO_Errno"`
	LastSQLErrno      int             `db:"Last_SQL_Errno"`
	Lag               sql.NullFloat64 `db:"Seconds_Behind_Source"`
}

type ReplicaStatus interface {
	ReplicationIORunning() bool
	ReplicationSQLRunning() bool
	ReplicationRunning() bool
	ReplicationState() string
	GetMasterHost() string
	GetMasterLogFile() string
	GetReadMasterLogPos() int64
	GetExecutedGtidSet() string
	GetRetrievedGtidSet() string
	GetLastIOErrno() int
	GetLastSQLErrno() int
	GetReplicationLag() sql.NullFloat64
}

// SemiSyncStatus contains semi sync host settings
type SemiSyncStatus struct {
	MasterEnabled  int `db:"MasterEnabled"`
	SlaveEnabled   int `db:"SlaveEnabled"`
	WaitSlaveCount int `db:"WaitSlaveCount"`
}

func (ss *SlaveStatusStruct) GetMasterHost() string {
	return ss.MasterHost
}

func (ss *SlaveStatusStruct) GetMasterLogFile() string {
	return ss.MasterLogFile
}

func (ss *SlaveStatusStruct) GetReadMasterLogPos() int64 {
	return ss.ReadMasterLogPos
}

func (ss *SlaveStatusStruct) GetExecutedGtidSet() string {
	return ss.ExecutedGtidSet
}

func (ss *SlaveStatusStruct) GetRetrievedGtidSet() string {
	return ss.RetrievedGtidSet
}

func (ss *SlaveStatusStruct) GetLastIOErrno() int {
	return ss.LastIOErrno
}

func (ss *SlaveStatusStruct) GetLastSQLErrno() int {
	return ss.LastSQLErrno
}

func (ss *ReplicaStatusStruct) GetMasterHost() string {
	return ss.SourceHost
}

func (ss *ReplicaStatusStruct) GetMasterLogFile() string {
	return ss.SourceLogFile
}

func (ss *ReplicaStatusStruct) GetReadMasterLogPos() int64 {
	return ss.ReadSourceLogPos
}

func (ss *ReplicaStatusStruct) GetExecutedGtidSet() string {
	return ss.ExecutedGtidSet
}

func (ss *ReplicaStatusStruct) GetRetrievedGtidSet() string {
	return ss.RetrievedGtidSet
}

func (ss *ReplicaStatusStruct) GetLastIOErrno() int {
	return ss.LastIOErrno
}

func (ss *ReplicaStatusStruct) GetLastSQLErrno() int {
	return ss.LastSQLErrno
}

// ReplicationIORunning ...
func (ss *SlaveStatusStruct) ReplicationIORunning() bool {
	return ss.SlaveIORunning == yes
}

func (ss *ReplicaStatusStruct) ReplicationIORunning() bool {
	return ss.ReplicaIORunning == yes
}

// ReplicationSQLRunning ...
func (ss *SlaveStatusStruct) ReplicationSQLRunning() bool {
	return ss.SlaveSQLRunning == yes
}

func (ss *ReplicaStatusStruct) ReplicationSQLRunning() bool {
	return ss.ReplicaSQLRunning == yes
}

// ReplicationRunning is true when both IO and SQL threads running
func (ss *SlaveStatusStruct) ReplicationRunning() bool {
	return ss.ReplicationIORunning() && ss.ReplicationSQLRunning()
}

func (ss *ReplicaStatusStruct) ReplicationRunning() bool {
	return ss.ReplicationIORunning() && ss.ReplicationSQLRunning()
}

func (ss *SlaveStatusStruct) GetReplicationLag() sql.NullFloat64 {
	return ss.Lag
}

func (ss *ReplicaStatusStruct) GetReplicationLag() sql.NullFloat64 {
	return ss.Lag
}

//GetReplicationLag

// ReplicationState ...
func (ss *SlaveStatusStruct) ReplicationState() string {
	switch {
	case ss.SlaveIORunning == yes && ss.SlaveSQLRunning == yes:
		return ReplicationRunning
	case (ss.SlaveIORunning != yes || ss.SlaveSQLRunning != yes) && (ss.LastIOErrno == 0 && ss.LastSQLErrno == 0):
		return ReplicationStopped
	default:
		return ReplicationError
	}
}

func (ss *ReplicaStatusStruct) ReplicationState() string {
	switch {
	case ss.ReplicaIORunning == yes && ss.ReplicaSQLRunning == yes:
		return ReplicationRunning
	case (ss.ReplicaIORunning != yes || ss.ReplicaSQLRunning != yes) && (ss.LastIOErrno == 0 && ss.LastSQLErrno == 0):
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

type Version struct {
	MajorVersion int `db:"MajorVersion"`
	MinorVersion int `db:"MinorVersion"`
	PatchVersion int `db:"PatchVersion"`
}

const (
	Version80Major              = 8
	Version80Minor              = 0
	Version80PatchReplicaStatus = 22
	Version57Major              = 5
)

func (v *Version) CheckIfVersionReplicaStatus() bool {
	switch v.MajorVersion {
	case Version80Major:
		if v.MinorVersion > Version80Minor || v.PatchVersion >= Version80PatchReplicaStatus {
			return true
		}
		return false
	case Version57Major:
		return false
	default:
		return true
	}
}

func (v *Version) CheckIfExternalReplicationSupported() bool {
	switch v.MajorVersion {
	case Version80Major:
		if v.MinorVersion > Version80Minor || v.PatchVersion >= Version80PatchReplicaStatus {
			return true
		}
		return false
	case Version57Major:
		return false
	default:
		return true
	}
}

func (v *Version) GetSlaveStatusQuery() string {
	if v.CheckIfVersionReplicaStatus() {
		return queryReplicaStatus
	} else {
		return querySlaveStatus
	}
}

func (v *Version) GetSlaveOrReplicaStruct() ReplicaStatus {
	if v.CheckIfVersionReplicaStatus() {
		return new(ReplicaStatusStruct)
	} else {
		return new(SlaveStatusStruct)
	}
}
