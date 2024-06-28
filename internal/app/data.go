package app

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

type appState string

const (
	stateFirstRun    = "FirstRun"
	stateManager     = "Manager"
	stateCandidate   = "Candidate"
	stateLost        = "Lost"
	stateMaintenance = "Maintenance"
)

const (
	// manager's lock
	pathManagerLock = "manager"

	pathMasterNode = "master"

	// def 1: activeNodes are master + alive running HA replicas ???
	// def 2: active nodes is last iteration surely ok ones
	// structure: list of hosts(strings)
	pathActiveNodes = "active_nodes"

	// structure: pathHealthPrefix/hostname -> NodeState
	pathHealthPrefix = "health"

	// structure: single Switchover
	pathCurrentSwitch = "switch"

	// structure: single Switchover
	pathLastSwitch = "last_switch"

	// structure: single Switchover
	pathLastRejectedSwitch = "last_rejected_switch"

	// structure: single Maintenance
	pathMaintenance = "maintenance"

	// structure: pathRecovery/hostname -> nil
	pathRecovery = "recovery"

	// List of HA nodes. May be modified by external tools (e.g. remove node from HA-cluster)
	// list of strings
	pathHANodes = "ha_nodes"

	// List of Cascade nodes. May be modified by external tools (e.g. on node removal)
	// structure: pathCascadeNodesPrefix/hostname -> CascadeNodeConfiguration
	pathCascadeNodesPrefix = "cascade_nodes"

	// low space flag
	// structure: single value boolean
	pathLowSpace = "low_space"

	// resetup status
	// structure: pathResetupStatus/hostname -> ResetupStatus
	pathResetupStatus = "resetup_status"

	pathLastShutdownNodeTime = "last_shutdown_node_time"

	// last known timestamp from repl_mon table
	pathMasterReplMonTS = "master_repl_mon_ts"
)

var (
	ErrNoMaster      = errors.New("no alive master found")
	ErrManyMasters   = errors.New("more than one master found")
	ErrNoActiveNodes = errors.New("no active nodes found")
)

// NodeState contains status check performed by some mysync process
type NodeState struct {
	CheckBy              string         `json:"check_by"`
	CheckAt              time.Time      `json:"check_at"`
	PingOk               bool           `json:"ping_ok"`
	PingDubious          bool           `json:"ping_dubious"`
	IsMaster             bool           `json:"is_master"`
	IsReadOnly           bool           `json:"is_readonly"`
	IsSuperReadOnly      bool           `json:"is_super_readonly"`
	IsOffline            bool           `json:"is_offline"`
	IsCascade            bool           `json:"is_cascade"`
	IsFileSystemReadonly bool           `json:"is_file_system_readonly"`
	Error                string         `json:"error"`
	DiskState            *DiskState     `json:"disk_state"`
	DaemonState          *DaemonState   `json:"daemon_state"`
	MasterState          *MasterState   `json:"master_state"`
	SlaveState           *SlaveState    `json:"slave_state"`
	SemiSyncState        *SemiSyncState `json:"semi_sync_state"`
}

// Last_SQL_Errno codes, that disallow to restart replication
var permanentReplicationLostSQLErrorCodes = map[int]interface{}{
	// Table 'db1.test_table' doesn't exist
	1146: nil,
	// Row size too large
	1118: nil,
}

// Last_IO_Errno codes, that disallow to restart replication
var permanentReplicationLostIOErrorCodes = map[int]interface{}{
	// 5.7
	1236: nil,
	// 8.0
	13114: nil,
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
		var err error
		gtid, err = ns.CalcGTIDDiffWithMaster()
		if err != nil {
			gtid = fmt.Sprintf("%s", err)
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

func (ns *SlaveState) FromReplicaStatus(replStatus mysql.ReplicaStatus) {
	ns.ExecutedGtidSet = replStatus.GetExecutedGtidSet()
	ns.RetrievedGtidSet = replStatus.GetRetrievedGtidSet()
	ns.MasterHost = replStatus.GetMasterHost()
	ns.ReplicationState = replStatus.ReplicationState()
	ns.MasterLogFile = replStatus.GetMasterLogFile()
	ns.MasterLogPos = replStatus.GetReadMasterLogPos()
	ns.LastIOErrno = replStatus.GetLastIOErrno()
	ns.LastSQLErrno = replStatus.GetLastSQLErrno()
}

// SemiSyncState contains semi sync host settings
type SemiSyncState struct {
	MasterEnabled  bool `json:"master_enabled"`
	SlaveEnabled   bool `json:"slave_enabled"`
	WaitSlaveCount int  `json:"wait_slave_count"`
}

const (
	// CauseManual means switchover was issued via command line
	CauseManual = "manual"
	// CauseWorker means switchover was initated via MDB worker (set directly to dcs)
	CauseWorker = "worker"
	// CauseAuto  means failover was started automatically by failure detection process
	CauseAuto = "auto"
)

// Switchover contains info about currently running or scheduled switchover/failover process
type Switchover struct {
	From        string            `json:"from"`
	To          string            `json:"to"`
	Cause       string            `json:"cause"`
	InitiatedBy string            `json:"initiated_by"`
	InitiatedAt time.Time         `json:"initiated_at"`
	StartedBy   string            `json:"started_by"`
	StartedAt   time.Time         `json:"started_at"`
	Result      *SwitchoverResult `json:"result"`
	RunCount    int               `json:"run_count,omitempty"`
}

func (sw *Switchover) String() string {
	var state string
	if sw.Result != nil {
		if sw.Result.Ok {
			state = "done"
		} else {
			state = "ERROR"
		}
	} else if !sw.StartedAt.IsZero() {
		state = "RUNNING"
	} else {
		state = "SCHEDULED"
	}
	swFrom := "*"
	if sw.From != "" {
		swFrom = sw.From
	}
	swTo := "*"
	if sw.To != "" {
		swTo = sw.To
	}
	return fmt.Sprintf("<%s %s=>%s %s by %s at %s>", state, swFrom, swTo, sw.Cause, sw.InitiatedBy, sw.InitiatedAt)
}

// SwitchoverResult contains results of finished/failed switchover
type SwitchoverResult struct {
	Ok         bool      `json:"ok"`
	Error      string    `json:"error"`
	FinishedAt time.Time `json:"finished_at"`
}

// Maintenance struct presence means that cluster under manual control
type Maintenance struct {
	InitiatedBy  string    `json:"initiated_by"`
	InitiatedAt  time.Time `json:"initiated_at"`
	MySyncPaused bool      `json:"mysync_paused"`
	ShouldLeave  bool      `json:"should_leave"`
}

func (m *Maintenance) String() string {
	ms := "entering"
	if m.MySyncPaused {
		ms = "ON"
	}
	if m.ShouldLeave {
		ms = "leaving"
	}
	return fmt.Sprintf("<%s by %s at %s>", ms, m.InitiatedBy, m.InitiatedAt)
}
