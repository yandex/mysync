package nodestate

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

// NodeState contains status check performed by some mysync process
type NodeState struct {
	CheckBy              string                     `json:"check_by"`
	CheckAt              time.Time                  `json:"check_at"`
	PingOk               bool                       `json:"ping_ok"`
	PingDubious          bool                       `json:"ping_dubious"`
	IsMaster             bool                       `json:"is_master"`
	IsReadOnly           bool                       `json:"is_readonly"`
	IsSuperReadOnly      bool                       `json:"is_super_readonly"`
	IsOffline            bool                       `json:"is_offline"`
	IsCascade            bool                       `json:"is_cascade"`
	IsFileSystemReadonly bool                       `json:"is_file_system_readonly"`
	IsLoadingBinlog      bool                       `json:"is_loading_binlog"`
	Error                string                     `json:"error"`
	DiskState            *DiskState                 `json:"disk_state"`
	DaemonState          *DaemonState               `json:"daemon_state"`
	MasterState          *MasterState               `json:"master_state"`
	SlaveState           *SlaveState                `json:"slave_state"`
	SemiSyncState        *SemiSyncState             `json:"semi_sync_state"`
	ReplicationSettings  *mysql.ReplicationSettings `json:"replication_settings"`

	ShowOnlyGTIDDiff bool
}

func (ns *NodeState) IsReplicationPermanentlyBroken() (bool, int) {
	slaveState := ns.SlaveState
	if slaveState != nil {
		if _, ok := permanentReplicationLostSQLErrorCodes[slaveState.LastSQLErrno]; ok {
			return true, slaveState.LastSQLErrno
		}

		if _, ok := permanentReplicationLostIOErrorCodes[slaveState.LastIOErrno]; ok {
			return true, slaveState.LastIOErrno
		}
	}
	return false, 0
}

// Last_SQL_Errno codes, that disallow to restart replication
var permanentReplicationLostSQLErrorCodes = map[int]any{
	// Table 'db1.test_table' doesn't exist
	1146: nil,
	// Row size too large
	1118: nil,
}

// Last_IO_Errno codes, that disallow to restart replication
var permanentReplicationLostIOErrorCodes = map[int]any{
	// 5.7
	1236: nil,
	// 8.0
	13114: nil,
}

func (ns *NodeState) CalcGTIDDiffWithMaster() (string, error) {
	if ns.SlaveState == nil {
		return "", fmt.Errorf("slave state not defined")
	}
	replicaGTID := gtids.ParseGtidSet(ns.SlaveState.ExecutedGtidSet)
	if ns.MasterState == nil {
		return "", fmt.Errorf("master state not defined")
	}
	sourceGTID := gtids.ParseGtidSet(ns.MasterState.ExecutedGtidSet)

	return gtids.GTIDDiff(replicaGTID, sourceGTID)
}

func (ss *SlaveState) GetCurrentBinlogPosition() string {
	return fmt.Sprintf("%s%019d", ss.MasterLogFile, ss.MasterLogPos)
}

func (ns *NodeState) UpdateBinlogStatus(oldLogFile string, maxLogPos int64) (string, int64) {
	if ns.SlaveState == nil {
		return "", 0
	}
	newLogFile := ns.SlaveState.MasterLogFile
	newLogPos := ns.SlaveState.MasterLogPos

	// IsLoadingBinlog is reset to false when starting to load a new binlog file.
	// Example:
	//   - Current position: MasterLogFile = "mysql-bin.000016", MasterLogPos = 784
	//   - When switching to "mysql-bin.000017", IsLoadingBinlog becomes false
	//     until the new file begins processing.
	if newLogFile != oldLogFile {
		ns.IsLoadingBinlog = false
		return newLogFile, newLogPos
	}

	ns.IsLoadingBinlog = newLogPos > maxLogPos

	if ns.IsLoadingBinlog {
		return oldLogFile, newLogPos
	} else {
		return oldLogFile, maxLogPos
	}
}

func (ns *NodeState) String() string {
	ping := "ok"
	if !ns.PingOk {
		ping = "ERR"
		if ns.PingDubious {
			ping += "?"
		}
	}
	const unknown = "???"
	repl := unknown
	gtid := unknown
	lag := 0.0
	if ns.SlaveState != nil {
		repl = ns.SlaveState.ReplicationState

		if ns.ShowOnlyGTIDDiff {
			var err error
			gtid, err = ns.CalcGTIDDiffWithMaster()
			if err != nil {
				gtid = fmt.Sprintf("%s", err)
			}
		} else {
			gtid = strings.ReplaceAll(ns.SlaveState.ExecutedGtidSet, "\n", "")
		}

		if ns.SlaveState.ReplicationLag != nil {
			lag = *ns.SlaveState.ReplicationLag
		} else {
			lag = math.NaN()
		}
	} else if ns.MasterState != nil {
		repl = "master"
		gtid = strings.ReplaceAll(ns.MasterState.ExecutedGtidSet, "\n", "")
	}
	sync := unknown
	if ns.SemiSyncState != nil {
		sync = ""
		if ns.SemiSyncState.MasterEnabled {
			sync += "m" + strconv.Itoa(ns.SemiSyncState.WaitSlaveCount)
		} else {
			sync += "--"
		}
		if ns.SemiSyncState.SlaveEnabled {
			sync += "s"
		} else {
			sync += "-"
		}
	}
	du := unknown
	if ns.DiskState != nil {
		du = fmt.Sprintf("%0.2f%%", ns.DiskState.Usage())
	}
	cr := unknown
	if ns.DaemonState != nil {
		if ns.DaemonState.CrashRecovery {
			cr = "yes"
		} else {
			cr = "no"
		}
	}

	// NodeState of replica doesn't know master gtid and always writes "master state not defined" when we calculate gtid diff
	if ns.ShowOnlyGTIDDiff && ns.MasterState == nil {
		return fmt.Sprintf("<ping=%s repl=%s sync=%s ro=%v offline=%v lag=%.02f du=%s cr=%s>",
			ping, repl, sync, ns.IsReadOnly, ns.IsOffline, lag, du, cr)
		// BLUEPRINT: do something here
	}

	return fmt.Sprintf("<ping=%s repl=%s sync=%s ro=%v offline=%v lag=%.02f du=%s cr=%s gtid=%s>",
		ping, repl, sync, ns.IsReadOnly, ns.IsOffline, lag, du, cr, gtid)
}

// DiskState contains information about disk space on the node
type DiskState struct {
	Used  uint64
	Total uint64
}

func (ds DiskState) Usage() float64 {
	if ds.Total == 0 {
		return 0
	}
	if ds.Used > ds.Total {
		return 100
	}
	return 100.0 * float64(ds.Used) / float64(ds.Total)
}

type DaemonState struct {
	StartTime     time.Time `json:"start_time"`
	RecoveryTime  time.Time `json:"recovery_time"`
	CrashRecovery bool      `json:"crash_recovery"`
}

// MasterState contains master specific info
type MasterState struct {
	ExecutedGtidSet string `json:"executed_gtid_set"`
}

// SlaveState contains slave specific info.
// Master always has this state empty
type SlaveState struct {
	MasterHost       string   `json:"master_host"`
	RetrievedGtidSet string   `json:"retrieved_gtid_get"`
	ExecutedGtidSet  string   `json:"executed_gtid_set"`
	ReplicationLag   *float64 `json:"replication_lag"`
	ReplicationState string   `json:"replication_state"`
	MasterLogFile    string   `json:"master_log_file"`
	MasterLogPos     int64    `json:"master_log_pos"`
	LastIOErrno      int      `json:"last_io_errno"`
	LastSQLErrno     int      `json:"last_sql_errno"`
}

func (ss *SlaveState) FromReplicaStatus(replStatus mysql.ReplicaStatus) {
	ss.ExecutedGtidSet = replStatus.GetExecutedGtidSet()
	ss.RetrievedGtidSet = replStatus.GetRetrievedGtidSet()
	ss.MasterHost = replStatus.GetMasterHost()
	ss.ReplicationState = replStatus.ReplicationState()
	ss.MasterLogFile = replStatus.GetMasterLogFile()
	ss.MasterLogPos = replStatus.GetReadMasterLogPos()
	ss.LastIOErrno = replStatus.GetLastIOErrno()
	ss.LastSQLErrno = replStatus.GetLastSQLErrno()
}

// SemiSyncState contains semi sync host settings
type SemiSyncState struct {
	MasterEnabled  bool `json:"master_enabled"`
	SlaveEnabled   bool `json:"slave_enabled"`
	WaitSlaveCount int  `json:"wait_slave_count"`
}
