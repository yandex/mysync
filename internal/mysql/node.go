package mysql

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql/gtids"
	"github.com/yandex/mysync/internal/util"
)

// Node represents API to query/manipulate single MySQL node
type Node struct {
	config  *config.Config
	logger  *log.Logger
	host    string
	db      *sqlx.DB
	version *Version
}

var queryOnliner = regexp.MustCompile(`\r?\n\s*`)
var mogrifyRegex = regexp.MustCompile(`:\w+`)
var ErrNotLocalNode = errors.New("this method should be run on local node only")

// NewNode returns new Node
func NewNode(config *config.Config, logger *log.Logger, host string) (*Node, error) {
	addr := util.JoinHostPort(host, config.MySQL.Port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/mysql", config.MySQL.User, config.MySQL.Password, addr)
	if config.MySQL.SslCA != "" {
		dsn += "?tls=custom"
	}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// Unsafe option allow us to use queries containing fields missing in structs
	// eg. when we running "SHOW SLAVE STATUS", but need only few columns
	db = db.Unsafe()
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(3)
	db.SetConnMaxLifetime(3 * config.TickInterval)
	return &Node{
		config:  config,
		logger:  logger,
		db:      db,
		host:    host,
		version: nil,
	}, nil
}

// RegisterTLSConfig loads and register CA file for TLS encryption
func RegisterTLSConfig(config *config.Config) error {
	if config.MySQL.SslCA != "" {
		var pem []byte
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(config.MySQL.SslCA)
		if err != nil {
			return err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return fmt.Errorf("failed to parse PEM certificate")
		}
		return mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool})
	}
	return nil
}

// Host returns Node host name
func (n *Node) Host() string {
	return n.host
}

// IsLocal returns true if MySQL Node running on the same host as calling mysync process
func (n *Node) IsLocal() bool {
	return n.host == n.config.Hostname
}

func (n *Node) String() string {
	return n.host
}

// Close closes underlying SQL connection
func (n *Node) Close() error {
	return n.db.Close()
}

func (n *Node) getCommand(name string) string {
	command, ok := n.config.Commands[name]
	if !ok {
		command, ok = defaultCommands[name]
	}
	if !ok {
		panic(fmt.Sprintf("Failed to find command with name '%s'", name))
	}
	return command
}

func (n *Node) runCommand(name string) (int, error) {
	command := n.getCommand(name)
	if !n.IsLocal() {
		panic(fmt.Sprintf("Remote command execution is not supported (%s on %s)", command, n.host))
	}
	shell := util.GetEnvVariable("SHELL", "sh")
	cmd := exec.Command(shell, "-c", command)
	err := cmd.Run()
	ret := cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
	n.logger.Debugf("running command '%s', retcode: %d, error %s", command, ret, err)
	return ret, err
}

func (n *Node) getQuery(name string) string {
	query, ok := n.config.Queries[name]
	if !ok {
		query, ok = DefaultQueries[name]
	}
	if !ok {
		panic(fmt.Sprintf("Failed to find query with name '%s'", name))
	}
	return query
}

func (n *Node) traceQuery(query string, arg interface{}, result interface{}, err error) {
	query = queryOnliner.ReplaceAllString(query, " ")
	msg := fmt.Sprintf("node %s running query '%s' with args %#v, result: %#v, error: %v", n.host, query, arg, result, err)
	msg = strings.Replace(msg, n.config.MySQL.Password, "********", -1)
	msg = strings.Replace(msg, n.config.MySQL.ReplicationPassword, "********", -1)
	n.logger.Debug(msg)
}

//nolint:unparam
func (n *Node) queryRow(queryName string, arg interface{}, result interface{}) error {
	return n.queryRowWithTimeout(queryName, arg, result, n.config.DBTimeout)
}

func (n *Node) queryRowWithTimeout(queryName string, arg interface{}, result interface{}, timeout time.Duration) error {
	if arg == nil {
		arg = struct{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	if err == nil {
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			err = rows.StructScan(result)
		} else {
			err = rows.Err()
			if err == nil {
				err = sql.ErrNoRows
			}
		}
	}
	n.traceQuery(query, arg, result, err)
	return err
}

// nolint: unparam
func (n *Node) queryRows(queryName string, arg interface{}, scanner func(*sqlx.Rows) error) error {
	if arg == nil {
		arg = struct{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), n.config.DBTimeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	n.traceQuery(query, arg, rows, err)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = scanner(rows)
		if err != nil {
			break
		}
	}
	return err
}

// nolint: unparam
func (n *Node) execWithTimeout(queryName string, arg map[string]interface{}, timeout time.Duration) error {
	if arg == nil {
		arg = map[string]interface{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// avoid connection leak on long lock timeouts
	lockTimeout := int64(math.Floor(0.8 * float64(timeout/time.Second)))
	if _, err := n.db.ExecContext(ctx, n.getQuery(querySetLockTimeout), lockTimeout); err != nil {
		n.traceQuery(query, arg, nil, err)
		return err
	}

	_, err := n.db.NamedExecContext(ctx, query, arg)
	n.traceQuery(query, arg, nil, err)
	return err
}

// nolint: unparam
func (n *Node) exec(queryName string, arg map[string]interface{}) error {
	return n.execWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *Node) getRunningQueryIds(excludeUsers []string, timeout time.Duration) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := DefaultQueries[queryGetProcessIds]

	bquery, args, err := sqlx.In(query, excludeUsers)
	if err != nil {
		n.traceQuery(bquery, args, nil, err)
		return nil, err
	}
	rows, err := n.db.QueryxContext(ctx, bquery, args...)
	if err != nil {
		n.traceQuery(bquery, args, nil, err)
		return nil, err
	}
	defer rows.Close()
	var ret []int

	for rows.Next() {
		var currid int
		err := rows.Scan(&currid)

		if err != nil {
			n.traceQuery(bquery, nil, ret, err)
			return nil, err
		}
		ret = append(ret, currid)
	}

	n.traceQuery(bquery, args, ret, nil)

	return ret, nil
}

type schemaname string

func escape(s string) string {
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `'`, `\'`, -1)
	return s
}

// Poorman's sql templating with value quotation
// Because go built-in placeholders don't work for queries like CHANGE MASTER
func Mogrify(query string, arg map[string]interface{}) string {
	return mogrifyRegex.ReplaceAllStringFunc(query, func(n string) string {
		n = n[1:]
		if v, ok := arg[n]; ok {
			switch vt := v.(type) {
			case schemaname:
				return "`" + string(vt) + "`"
			case string:
				return "'" + escape(vt) + "'"
			case int:
				return strconv.Itoa(vt)
			default:
				return "'" + escape(fmt.Sprintf("%s", vt)) + "'"
			}
		} else {
			return n
		}
	})
}

// not all queries may be parametrized with placeholders
func (n *Node) execMogrifyWithTimeout(queryName string, arg map[string]interface{}, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := n.db.ExecContext(ctx, query)
	n.traceQuery(query, nil, nil, err)
	return err
}

func (n *Node) execMogrify(queryName string, arg map[string]interface{}) error {
	return n.execMogrifyWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *Node) queryRowMogrifyWithTimeout(queryName string, arg map[string]interface{}, result interface{}, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	if err == nil {
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			err = rows.StructScan(result)
		} else {
			err = rows.Err()
			if err == nil {
				err = sql.ErrNoRows
			}
		}
	}
	n.traceQuery(query, arg, result, err)
	return err
}

// IsRunning checks if daemon process is running
func (n *Node) IsRunning() (bool, error) {
	if !n.IsLocal() {
		return false, ErrNotLocalNode
	}
	ret, err := n.runCommand(commandStatus)
	return ret == 0, err
}

func (n *Node) getTestDiskUsage(f string) (used uint64, total uint64, err error) {
	data, err := os.ReadFile(f)
	if err != nil {
		return 0, 0, err
	}
	percent, err := strconv.Atoi(strings.TrimSpace(string((data))))
	if err != nil {
		return 0, 0, err
	}
	someSize := 10 * 1024 * 1024 * 1024
	return uint64(float64(percent) / float64(100) * float64(someSize)), uint64(someSize), nil
}

// GetDiskUsage returns datadir usage statistics
func (n *Node) GetDiskUsage() (used uint64, total uint64, err error) {
	if n.config.TestDiskUsageFile != "" {
		return n.getTestDiskUsage(n.config.TestDiskUsageFile)
	}
	if !n.IsLocal() {
		err = ErrNotLocalNode
		return
	}
	var stat syscall.Statfs_t
	err = syscall.Statfs(n.config.MySQL.DataDir, &stat)
	total = uint64(stat.Bsize) * stat.Blocks
	// on FreeBSD stat.Bavail may be negative
	var bavail = stat.Bavail
	// nolint: staticcheck
	if bavail < 0 {
		bavail = 0
	}
	used = total - uint64(stat.Bsize)*uint64(bavail) // nolint: unconvert
	return
}

func (n *Node) isTestFileSystemReadonly(f string) (bool, error) {
	data, err := os.ReadFile(f)
	if err != nil {
		return false, err
	}
	value, err := strconv.ParseBool(strings.TrimSpace(string(data)))
	if err != nil {
		return false, fmt.Errorf("error while parce test file: %s", err)
	}
	return value, nil
}

func getFlagsFromProcMounts(file, filesystem string) (string, error) {
	for _, line := range strings.Split(file, "\n") {
		components := strings.Split(line, " ")
		if len(components) < 3 {
			continue
		}

		if components[1] == filesystem {
			flags := strings.Split(components[3], ",")
			for _, flag := range flags {
				if flag == "ro" || flag == "rw" {
					return flag, nil
				}
			}
			return "", fmt.Errorf("not found expected flag in line %s", components[3])
		}
	}

	return "", fmt.Errorf("not found %s filesystem in /proc/mounts", filesystem)
}

func (n *Node) IsFileSystemReadonly() (bool, error) {
	if n.config.TestFilesystemReadonlyFile != "" {
		return n.isTestFileSystemReadonly(n.config.TestFilesystemReadonlyFile)
	}
	if !n.IsLocal() {
		return false, ErrNotLocalNode
	}

	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false, err
	}
	file := string(data)

	flag, err := getFlagsFromProcMounts(file, n.config.MySQL.DataDir)
	if err != nil {
		return false, err
	}

	if flag == "ro" {
		return true, nil
	} else {
		return false, nil
	}
}

func (n *Node) GetDaemonStartTime() (time.Time, error) {
	if !n.IsLocal() {
		return time.Time{}, ErrNotLocalNode
	}
	pidB, err := os.ReadFile(n.config.MySQL.PidFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return time.Time{}, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidB)))
	if err != nil {
		return time.Time{}, err
	}
	ps, err := process.NewProcess(int32(pid))
	if err != nil {
		return time.Time{}, err
	}
	createTS, err := ps.CreateTime()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(createTS/1000, (createTS%1000)*1000000), nil
}

func (n *Node) GetCrashRecoveryTime() (time.Time, error) {
	if !n.IsLocal() {
		return time.Time{}, ErrNotLocalNode
	}
	fh, err := os.Open(n.config.MySQL.ErrorLog)
	if err != nil {
		return time.Time{}, err
	}
	defer fh.Close()
	recoveryTimeText := ""
	s := bufio.NewScanner(fh)
	for s.Scan() {
		line := s.Text()
		if strings.HasSuffix(line, "[Note] Starting crash recovery...") || // For 5.7 version
			strings.HasSuffix(line, "[InnoDB] Starting crash recovery.") { // For 8.0 version
			recoveryTimeText = strings.Split(line, " ")[0]
		}
	}
	if recoveryTimeText == "" {
		return time.Time{}, nil
	}
	return time.Parse("2006-01-02T15:04:05.000000-07:00", recoveryTimeText)
}

// Ping checks node health status by executing simple query
func (n *Node) Ping() (bool, error) {
	result := new(pingResult)
	err := n.queryRow(queryPing, nil, result)
	return result.Ok > 0, err
}

// GetReplicaStatus returns slave/replica status or nil if node is master
func (n *Node) GetReplicaStatus() (ReplicaStatus, error) {
	return n.ReplicaStatusWithTimeout(n.config.DBTimeout)
}

func (n *Node) ReplicaStatusWithTimeout(timeout time.Duration) (ReplicaStatus, error) {
	query, status, err := n.GetVersionSlaveStatusQuery()
	if err != nil {
		return nil, nil
	}
	err = n.queryRowMogrifyWithTimeout(query, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	}, status, timeout)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return status, err
}

// GetExternalReplicaStatus returns slave/replica status or nil if node is master for external channel
func (n *Node) GetExternalReplicaStatus() (ReplicaStatus, error) {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return nil, err
	}
	if !(checked) {
		return nil, nil
	}
	status := new(ReplicaStatusStruct)
	err = n.queryRowMogrifyWithTimeout(queryReplicaStatus, map[string]interface{}{
		"channel": n.config.ExternalReplicationChannel,
	}, status, n.config.DBTimeout)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return status, err
}

func (n *Node) GetVersionSlaveStatusQuery() (string, ReplicaStatus, error) {
	version, err := n.GetVersion()
	if err != nil {
		return "", nil, err
	}
	return version.GetSlaveStatusQuery(), version.GetSlaveOrReplicaStruct(), nil
}

func (n *Node) GetVersion() (*Version, error) {
	if n.version != nil {
		return n.version, nil
	}
	v := new(Version)
	err := n.queryRow(queryGetVersion, nil, v)
	if err != nil {
		return nil, err
	}
	n.version = v
	return n.version, nil
}

// ReplicationLag returns slave replication lag in seconds
// ReplicationLag may return nil without error if lag is unknown (replication not running)
func (n *Node) ReplicationLag(sstatus ReplicaStatus) (*float64, error) {
	var err error
	if n.getQuery(queryReplicationLag) != "" {
		lag := new(replicationLag)
		err = n.queryRow(queryReplicationLag, nil, lag)
		if err == sql.ErrNoRows {
			// looks like master
			return new(float64), nil
		}
		if lag.Lag.Valid {
			return &lag.Lag.Float64, nil
		}
	} else if sstatus != nil {
		l := sstatus.GetReplicationLag()
		if l.Valid {
			return &l.Float64, nil
		}
	}
	// replication not running, assume lag is huge
	return nil, err
}

// GTIDExecuted returns global transaction id executed
func (n *Node) GTIDExecuted() (*GTIDExecuted, error) {
	status := new(GTIDExecuted)
	err := n.queryRow(queryGTIDExecuted, nil, status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return status, err
}

// GTIDExecuted returns global transaction id executed
func (n *Node) GTIDExecutedParsed() (gtids.GTIDSet, error) {
	gtid, err := n.GTIDExecuted()
	if err != nil {
		return nil, err
	}

	return gtids.ParseGtidSet(gtid.ExecutedGtidSet), nil
}

// GetBinlogs returns SHOW BINARY LOGS output
func (n *Node) GetBinlogs() ([]Binlog, error) {
	var binlogs []Binlog
	err := n.queryRows(queryShowBinaryLogs, nil, func(rows *sqlx.Rows) error {
		var binlog Binlog
		err := rows.StructScan(&binlog)
		if err != nil {
			return err
		}
		binlogs = append(binlogs, binlog)
		return nil
	})
	return binlogs, err
}

// IsReadOnly returns (true, true) if MySQL Node in (read-only, super-read-only) mode
func (n *Node) IsReadOnly() (bool, bool, error) {
	var ror readOnlyResult
	err := n.queryRow(queryIsReadOnly, nil, &ror)
	return ror.ReadOnly > 0, ror.SuperReadOnly > 0, err
}

// SetReadOnly sets MySQL Node to be read-only
// SetReadOnlyNoSuper sets MySQL Node to read_only=1, but super_read_only=0
// Setting server read-only may take a while
// as server waits all running commits (not transactions) to be finished
func (n *Node) SetReadOnly(superReadOnly bool) error {
	return n.setReadonlyWithTimeout(superReadOnly, n.config.DBSetRoTimeout)
}

func (n *Node) setReadonlyWithTimeout(superReadOnly bool, timeout time.Duration) error {
	query := querySetReadonly
	if !superReadOnly {
		query = querySetReadonlyNoSuper
	}

	err := n.execWithTimeout(query, nil, timeout)
	if err != nil {
		return err
	}

	// making sure that the node has switched to read_only mode
	isReadOnly, isSuperReadOnly, err := n.IsReadOnly()
	if err != nil {
		return err
	}
	if isSuperReadOnly != superReadOnly {
		return fmt.Errorf("the node has not switched to super_read_only mode")
	}
	if !isReadOnly {
		return fmt.Errorf("the node has not switched to read_only mode")
	}
	return nil
}

// SetReadOnlyWithForce sets MySQL Node to be read-only
// Setting server read-only with force kill all running queries trying to kill problematic ones
// this may take a while as client may start new queries
func (n *Node) SetReadOnlyWithForce(excludeUsers []string, superReadOnly bool) error {
	// first, we will try to gracefully set host read_only
	timeouts := []int{2, 4, 8}
	for i, t := range timeouts {
		n.logger.Infof("trying set host %s read-only, attempt %d", n.host, i+1)
		err := n.setReadonlyWithTimeout(superReadOnly, time.Duration(t)*time.Second)
		if err == nil {
			return nil
		}
	}

	n.logger.Infof("host %s was not set read-only gracefully, so now we'll kill all user processes", n.host)

	quit := make(chan bool)
	ticker := time.NewTicker(time.Second)

	go func() {
		for {
			ids, err := n.getRunningQueryIds(excludeUsers, time.Second)
			if err == nil {
				for _, id := range ids {
					_ = n.exec(queryKillQuery, map[string]interface{}{"kill_id": strconv.Itoa(id)})
				}
			}

			select {
			case <-quit:
				return
			case <-ticker.C:
				continue
			}
		}
	}()

	defer func() { quit <- true }()

	return n.setReadonlyWithTimeout(superReadOnly, n.config.DBSetRoForceTimeout)
}

// SetWritable sets MySQL Node to be writable, eg. disables read-only
func (n *Node) SetWritable() error {
	return n.exec(querySetWritable, nil)
}

// StopSlave stops replication (both IO and SQL threads)
func (n *Node) StopSlave() error {
	return n.execMogrifyWithTimeout(queryStopSlave, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	}, n.config.DBStopSlaveSQLThreadTimeout)
}

// StartSlave starts replication (both IO and SQL threads)
func (n *Node) StartSlave() error {
	return n.execMogrify(queryStartSlave, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	})
}

// StopSlaveIOThread stops IO replication thread
func (n *Node) StopSlaveIOThread() error {
	return n.execMogrify(queryStopSlaveIOThread, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	})
}

// StartSlaveIOThread starts IO replication thread
func (n *Node) StartSlaveIOThread() error {
	return n.execMogrify(queryStartSlaveIOThread, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	})
}

// RestartSlaveIOThread stops IO replication thread
func (n *Node) RestartSlaveIOThread() error {
	err := n.StopSlaveIOThread()
	if err != nil {
		return err
	}
	return n.StartSlaveIOThread()
}

// StopSlaveSQLThread stops SQL replication thread
func (n *Node) StopSlaveSQLThread() error {
	return n.execMogrifyWithTimeout(queryStopSlaveSQLThread, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	}, n.config.DBStopSlaveSQLThreadTimeout)
}

// StartSlaveSQLThread starts SQL replication thread
func (n *Node) StartSlaveSQLThread() error {
	return n.execMogrify(queryStartSlaveSQLThread, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	})
}

// ResetSlaveAll promotes MySQL Node to be master
func (n *Node) ResetSlaveAll() error {
	return n.execMogrify(queryResetSlaveAll, map[string]interface{}{
		"channel": n.config.ReplicationChannel,
	})
}

// StartExternalReplication starts external replication
func (n *Node) StartExternalReplication() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStartReplica, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// StopExternalReplication stops external replication
func (n *Node) StopExternalReplication() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStopReplica, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}

// ResetExternalReplicationAll resets external replication
func (n *Node) ResetExternalReplicationAll() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryResetReplicaAll, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}

// SemiSyncStatus returns semi sync status
func (n *Node) SemiSyncStatus() (*SemiSyncStatus, error) {
	status := new(SemiSyncStatus)
	err := n.queryRow(querySemiSyncStatus, nil, status)
	if err != nil {
		if err2, ok := err.(*mysql.MySQLError); ok && err2.Number == 1193 {
			// Error: Unknown system variable
			// means semisync plugin is not loaded
			return status, nil
		}
	}
	return status, err
}

// SemiSyncSetMaster set host as semisync master
func (n *Node) SemiSyncSetMaster() error {
	return n.exec(querySemiSyncSetMaster, nil)
}

// SemiSyncSetSlave set host as semisync master
func (n *Node) SemiSyncSetSlave() error {
	return n.exec(querySemiSyncSetSlave, nil)
}

// SemiSyncDisable disables semi_sync_master and semi_sync_slave
func (n *Node) SemiSyncDisable() error {
	return n.exec(querySemiSyncDisable, nil)
}

// SemiSyncSetWaitSlaveCount changes rpl_semi_sync_master_wait_for_slave_count
func (n *Node) SetSemiSyncWaitSlaveCount(c int) error {
	return n.exec(querySetSemiSyncWaitSlaveCount, map[string]interface{}{"wait_slave_count": c})
}

// IsOffline returns current 'offline_mode' variable value
func (n *Node) IsOffline() (bool, error) {
	status := new(offlineModeStatus)
	err := n.queryRow(queryGetOfflineMode, nil, status)
	if err != nil {
		return false, err
	}
	return status.OfflineMode == 1, err
}

// SetOffline turns on 'offline_mode'
func (n *Node) SetOffline() error {
	return n.exec(queryEnableOfflineMode, nil)
}

// SetOnline turns off 'offline_mode'
func (n *Node) SetOnline() error {
	return n.exec(queryDisableOfflineMode, nil)
}

// ChangeMaster changes master of MySQL Node, demoting it to slave
func (n *Node) ChangeMaster(host string) error {
	useSsl := 0
	if n.config.MySQL.ReplicationSslCA != "" {
		useSsl = 1
	}
	return n.execMogrify(queryChangeMaster, map[string]interface{}{
		"host":            host,
		"port":            n.config.MySQL.ReplicationPort,
		"user":            n.config.MySQL.ReplicationUser,
		"password":        n.config.MySQL.ReplicationPassword,
		"ssl":             useSsl,
		"sslCa":           n.config.MySQL.ReplicationSslCA,
		"retryCount":      n.config.MySQL.ReplicationRetryCount,
		"connectRetry":    n.config.MySQL.ReplicationConnectRetry,
		"heartbeatPeriod": n.config.MySQL.ReplicationHeartbeatPeriod,
		"channel":         n.config.ReplicationChannel,
	})
}

func (n *Node) ReenableEvents() ([]Event, error) {
	var events []Event
	err := n.queryRows(queryListSlavesideDisabledEvents, nil, func(rows *sqlx.Rows) error {
		var event Event
		err := rows.StructScan(&event)
		if err != nil {
			return err
		}
		events = append(events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		definer := strings.Split(event.Definer, "@")
		user := definer[0]
		host := ""
		/*
			In case of incorrect Definer field in event. Though I wasn't able find the way to get it, I'm not sure
			that there is no way to create mysql event with definer field without '@' symbol
			At least there is possible to event with definer like definer=abc@'' (but symbol @ will still be present)
		*/
		if len(definer) > 1 {
			host = definer[1]
		}
		err = n.execMogrify(queryEnableEvent, map[string]interface{}{
			"schema": schemaname(event.Schema),
			"name":   schemaname(event.Name),
			"user":   user,
			"host":   host,
		})
		if err != nil {
			return nil, err
		}
	}
	return events, nil
}

// IsWaitingSemiSyncAck returns true when Master is stuck in 'Waiting for semi-sync ACK from slave' state
func (n *Node) IsWaitingSemiSyncAck() (bool, error) {
	type waitingSemiSyncStatus struct {
		IsWaiting bool `db:"IsWaiting"`
	}
	var status waitingSemiSyncStatus
	err := n.queryRow(queryHasWaitingSemiSyncAck, nil, &status)
	return status.IsWaiting, err
}

func (n *Node) GetStartupTime() (time.Time, error) {
	type lastStartupTime struct {
		LastStartup float64 `db:"LastStartup"`
	}
	var startupTime lastStartupTime
	err := n.queryRow(queryGetLastStartupTime, nil, &startupTime)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(startupTime.LastStartup), 0), nil
}

func (n *Node) IsExternalReplicationSupported() (bool, error) {
	version, err := n.GetVersion()
	if err != nil {
		return false, err
	}
	return version.CheckIfExternalReplicationSupported(), nil
}

func (n *Node) SetExternalReplication() error {
	var replSettings replicationSettings
	err := n.queryRow(queryGetExternalReplicationSettings, nil, &replSettings)
	if err != nil {
		// If no table in scheme then we consider external replication not existing so we do nothing
		if IsErrorTableDoesNotExists(err) {
			return nil
		}
		// If there is no rows in table for external replication - do nothing
		if err == sql.ErrNoRows {
			n.logger.Infof("no external replication records found in replication table on host %s", n.host)
			return nil
		}
		return err
	}
	useSsl := 0
	sslCa := ""
	if replSettings.SourceSslCa != "" && n.config.MySQL.ExternalReplicationSslCA != "" {
		useSsl = 1
		sslCa = n.config.MySQL.ExternalReplicationSslCA
	}
	err = n.StopExternalReplication()
	if err != nil {
		return err
	}
	err = n.ResetExternalReplicationAll()
	if err != nil {
		return err
	}
	err = n.execMogrify(queryChangeSource, map[string]interface{}{
		"host":            replSettings.SourceHost,
		"port":            replSettings.SourcePort,
		"user":            replSettings.SourceUser,
		"password":        replSettings.SourcePassword,
		"ssl":             useSsl,
		"sslCa":           sslCa,
		"sourceDelay":     replSettings.SourceDelay,
		"retryCount":      n.config.MySQL.ReplicationRetryCount,
		"connectRetry":    n.config.MySQL.ReplicationConnectRetry,
		"heartbeatPeriod": n.config.MySQL.ReplicationHeartbeatPeriod,
		"channel":         "external",
	})
	if err != nil {
		return err
	}
	return n.StartExternalReplication()
}

func (n *Node) UpdateExternalCAFile() error {
	var replSettings replicationSettings
	err := n.queryRow(queryGetExternalReplicationSettings, nil, &replSettings)
	if err != nil {
		return nil
	}
	data := replSettings.SourceSslCa
	fileName := n.config.MySQL.ExternalReplicationSslCA
	if data != "" && fileName != "" {
		err = util.TouchFile(fileName)
		if err != nil {
			return err
		}
		oldDataByte, err := os.ReadFile(fileName)
		if err != nil {
			return err
		}
		if data != string(oldDataByte) {
			n.logger.Infof("saving new CA file to %s", fileName)
			err := n.SaveCAFile(data, fileName)
			if err != nil {
				return err
			}
		}
	}
	if data == "" && fileName != "" {
		_, err := os.Stat(fileName)
		if os.IsNotExist(err) {
			return nil
		}
		n.logger.Infof("deleting CA file %s", fileName)
		err = os.Remove(fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) SaveCAFile(data string, path string) error {
	rootCertPool := x509.NewCertPool()
	byteData := []byte(data)
	if ok := rootCertPool.AppendCertsFromPEM(byteData); !ok {
		return fmt.Errorf("got error validating PEM certificate")
	}
	err := os.WriteFile(path, byteData, 0644)
	if err != nil {
		err2 := fmt.Errorf("got error while writing CA file: %s", err)
		return err2
	}
	return nil
}
