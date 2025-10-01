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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
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
	db      *sqlx.DB
	version *Version
	host    string
	uuid    uuid.UUID

	done atomic.Uint32
	mu   sync.Mutex
}

var (
	queryOnliner            = regexp.MustCompile(`\r?\n\s*`)
	mogrifyRegex            = regexp.MustCompile(`:\w+`)
	ErrNotLocalNode         = errors.New("this method should be run on local node only")
	SafeReplicationSettings = ReplicationSettings{
		InnodbFlushLogAtTrxCommit: DefaultInnodbFlushLogAtTrxCommitValue,
		SyncBinlog:                DefaultSyncBinlogValue,
	}
)

const (
	OptimalSyncBinlogValue                = 1000
	OptimalInnodbFlushLogAtTrxCommitValue = 2
	DefaultInnodbFlushLogAtTrxCommitValue = 1
	DefaultSyncBinlogValue                = 1
)

// NewNode returns new Node
func NewNode(config *config.Config, logger *log.Logger, host string) (*Node, error) {
	return &Node{
		config:  config,
		logger:  logger,
		db:      nil,
		host:    host,
		version: nil,

		done: atomic.Uint32{},
		mu:   sync.Mutex{},
	}, nil
}

// Lazy initialization of db connection
func (n *Node) GetDB() (*sqlx.DB, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var err error

	// First initialization
	if n.done.Load() == 0 {
		defer n.done.Store(1)
		addr := util.JoinHostPort(n.host, n.config.MySQL.Port)
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/mysql", n.config.MySQL.User, n.config.MySQL.Password, addr) + n.config.DSNSettings
		if n.config.MySQL.SslCA != "" {
			dsn += "&tls=custom"
		}
		n.db, err = sqlx.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}

		// Unsafe option allow us to use queries containing fields missing in structs
		// eg. when we running "SHOW SLAVE STATUS", but need only few columns
		n.db = n.db.Unsafe()
		n.db.SetMaxIdleConns(1)
		n.db.SetMaxOpenConns(3)
		n.db.SetConnMaxLifetime(3 * n.config.TickInterval)
	}

	// Return old value
	return n.db, nil
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
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.done.Load() != 0 {
		return n.db.Close()
	} else {
		return nil
	}
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

func (n *Node) traceQuery(query string, arg any, result any, err error) {
	query = queryOnliner.ReplaceAllString(query, " ")
	if n.config.ShowOnlyGTIDDiff && IsGtidQuery(query) {
		n.logger.Debug("<gtid query was ignored>")
		return
	}
	msg := fmt.Sprintf("node %s running query '%s' with args %#v, result: %#v, error: %v", n.host, query, arg, result, err)
	msg = strings.ReplaceAll(msg, n.config.MySQL.Password, "********")
	msg = strings.ReplaceAll(msg, n.config.MySQL.ReplicationPassword, "********")
	n.logger.Debug(msg)
}

func IsGtidQuery(query string) bool {
	for _, gtidQuery := range GtidQueries {
		if strings.HasPrefix(query, gtidQuery) {
			return true
		}
	}

	return false
}

var GtidQueries = []string{
	DefaultQueries[queryGTIDExecuted],
	strings.ReplaceAll(DefaultQueries[querySlaveStatus], ":channel", "''"),
	strings.ReplaceAll(DefaultQueries[queryReplicaStatus], ":channel", "''"),
}

//nolint:unparam
func (n *Node) queryRow(queryName string, arg any, result any) error {
	return n.queryRowWithTimeout(queryName, arg, result, n.config.DBTimeout)
}

func (n *Node) queryRowWithTimeout(queryName string, arg any, result any, timeout time.Duration) error {
	if arg == nil {
		arg = struct{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	db, err := n.GetDB()
	if err != nil {
		return err
	}
	rows, err := db.NamedQueryContext(ctx, query, arg)
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
func (n *Node) queryRows(queryName string, arg any, scanner func(*sqlx.Rows) error) error {
	// TODO we need to rewrite processQuery, to make traceQuery work properly
	// traceQuery should be called with result, not *Rows
	return n.processQuery(queryName, arg, func(rows *sqlx.Rows) error {
		var err error

		for rows.Next() {
			err = scanner(rows)
			if err != nil {
				break
			}
		}

		return err
	}, n.config.DBTimeout)
}

func (n *Node) processQuery(queryName string, arg any, rowsProcessor func(*sqlx.Rows) error, timeout time.Duration) error {
	if arg == nil {
		arg = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := n.getQuery(queryName)
	db, err := n.GetDB()
	if err != nil {
		return err
	}
	rows, err := db.NamedQueryContext(ctx, query, arg)
	n.traceQuery(query, arg, rows, err)
	if err != nil {
		return err
	}

	defer func() { _ = rows.Close() }()

	return rowsProcessor(rows)
}

// nolint: unparam
func (n *Node) execWithTimeout(queryName string, arg map[string]any, timeout time.Duration) error {
	if arg == nil {
		arg = map[string]any{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// avoid connection leak on long lock timeouts
	lockTimeout := int64(math.Floor(0.8 * float64(timeout/time.Second)))
	db, err := n.GetDB()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, n.getQuery(querySetLockTimeout), lockTimeout); err != nil {
		n.traceQuery(query, arg, nil, err)
		return err
	}
	_, err = db.NamedExecContext(ctx, query, arg)
	n.traceQuery(query, arg, nil, err)
	return err
}

// nolint: unparam
func (n *Node) exec(queryName string, arg map[string]any) error {
	return n.execWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *Node) getRunningQueryIDs(excludeUsers []string, timeout time.Duration) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := DefaultQueries[queryGetProcessIDs]

	bquery, args, err := sqlx.In(query, excludeUsers)
	if err != nil {
		n.traceQuery(bquery, args, nil, err)
		return nil, err
	}
	db, err := n.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryxContext(ctx, bquery, args...)
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

type inlinestr string

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

// Poorman's sql templating with value quotation
// Because go built-in placeholders don't work for queries like CHANGE MASTER
func Mogrify(query string, arg map[string]any) string {
	return mogrifyRegex.ReplaceAllStringFunc(query, func(n string) string {
		n = n[1:]
		if v, ok := arg[n]; ok {
			switch vt := v.(type) {
			case schemaname:
				return "`" + string(vt) + "`"
			case inlinestr:
				return string(vt)
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

// not all queries may be parameterized with placeholders
func (n *Node) execMogrifyWithTimeout(queryName string, arg map[string]any, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	db, err := n.GetDB()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, query)
	n.traceQuery(query, nil, nil, err)
	return err
}

func (n *Node) execMogrify(queryName string, arg map[string]any) error {
	return n.execMogrifyWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *Node) queryRowMogrifyWithTimeout(queryName string, arg map[string]any, result any, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	db, err := n.GetDB()
	if err != nil {
		return err
	}
	rows, err := db.NamedQueryContext(ctx, query, arg)
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

func (n *Node) queryRowMogrify(queryName string, arg map[string]any, result any) error {
	return n.queryRowMogrifyWithTimeout(queryName, arg, result, n.config.DBTimeout)
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
	bavail := stat.Bavail
	bavail = max(0, bavail)
	used = total - uint64(stat.Bsize)*uint64(bavail) // nolint: unconvert
	return
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
		return isTestFileSystemReadonly(n.config.TestFilesystemReadonlyFile)
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

func isTestFileSystemReadonly(f string) (bool, error) {
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
	pid, err := strconv.ParseInt(strings.TrimSpace(string(pidB)), 10, 32)
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
	return n.ReplicaStatusWithTimeout(n.config.DBTimeout, n.config.ReplicationChannel)
}

func (n *Node) ReplicaStatusWithTimeout(timeout time.Duration, channel string) (ReplicaStatus, error) {
	query, err := n.GetSlaveStatusQuery()
	if err != nil {
		return nil, err
	}
	status, err := n.GetSlaveOrReplicaStruct()
	if err != nil {
		return nil, err
	}
	err = n.queryRowMogrifyWithTimeout(query, map[string]any{
		"channel": channel,
	}, status, timeout)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return status, err
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

// UUID returns server_uuid
func (n *Node) UUID() (uuid.UUID, error) {
	if n.uuid.ID() != 0 {
		return n.uuid, nil
	}
	var r ServerUUIDResult
	err := n.queryRow(queryGetUUID, nil, &r)
	if err != nil {
		return uuid.UUID{}, err
	}
	v, err := uuid.Parse(r.ServerUUID)
	if err != nil {
		return uuid.UUID{}, err
	}
	n.uuid = v
	return v, err
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
			ids, err := n.getRunningQueryIDs(excludeUsers, time.Second)
			if err == nil {
				for _, id := range ids {
					_ = n.exec(queryKillQuery, map[string]any{"kill_id": strconv.Itoa(id)})
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
	q, err := n.GetStopSlaveQuery()
	if err != nil {
		return err
	}
	return n.execMogrifyWithTimeout(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	}, n.config.DBStopSlaveSQLThreadTimeout)
}

// StartSlave starts replication (both IO and SQL threads)
func (n *Node) StartSlave() error {
	q, err := n.GetStartSlaveQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	})
}

// RestartReplica restart replication
func (n *Node) RestartReplica() error {
	err := n.StopSlave()
	if err != nil {
		return err
	}
	return n.StartSlave()
}

// StopSlaveIOThread stops IO replication thread
func (n *Node) StopSlaveIOThread() error {
	q, err := n.GetStopSlaveIOThreadQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	})
}

// StartSlaveIOThread starts IO replication thread
func (n *Node) StartSlaveIOThread() error {
	q, err := n.GetStartSlaveIOThreadQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	})
}

// RestartSlaveIOThread restart IO replication thread
func (n *Node) RestartSlaveIOThread() error {
	err := n.StopSlaveIOThread()
	if err != nil {
		return err
	}
	return n.StartSlaveIOThread()
}

// StopSlaveSQLThread stops SQL replication thread
func (n *Node) StopSlaveSQLThread() error {
	q, err := n.GetStopSlaveSQLThreadQuery()
	if err != nil {
		return err
	}
	return n.execMogrifyWithTimeout(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	}, n.config.DBStopSlaveSQLThreadTimeout)
}

// StartSlaveSQLThread starts SQL replication thread
func (n *Node) StartSlaveSQLThread() error {
	q, err := n.GetStartSlaveSQLThreadQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	})
}

// ResetSlaveAll promotes MySQL Node to be master
func (n *Node) ResetSlaveAll() error {
	q, err := n.GetResetSlaveQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
		"channel": n.config.ReplicationChannel,
	})
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
	return n.exec(querySetSemiSyncWaitSlaveCount, map[string]any{"wait_slave_count": c})
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

func (n *Node) SetOfflineForce() error {
	err := n.SemiSyncDisable()
	if err != nil {
		return err
	}
	return n.SetOffline()
}

// ChangeMaster changes master of MySQL Node, demoting it to slave
func (n *Node) ChangeMaster(host string) error {
	useSsl := 0
	if n.config.MySQL.ReplicationSslCA != "" {
		useSsl = 1
	}
	q, err := n.GetChangeMasterQuery()
	if err != nil {
		return err
	}
	return n.execMogrify(q, map[string]any{
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

const ReenableEventsRetryCount = 3

func (n *Node) ReenableEventsRetry() ([]Event, error) {
	var err error
	for range ReenableEventsRetryCount {
		events, err := n.ReenableEvents()
		if err == nil {
			return events, nil
		}
		n.logger.Errorf("failed to enable events: %s", err)
	}
	return nil, err
}

func (n *Node) ReenableEvents() ([]Event, error) {
	var events []Event
	q, err := n.GetListSlaveSideDisabledEventsQuery()
	if err != nil {
		return nil, err
	}
	err = n.queryRows(q, nil, func(rows *sqlx.Rows) error {
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
		err = n.execMogrify(queryEnableEvent, map[string]any{
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
			err := SaveCAFile(data, fileName)
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

func SaveCAFile(data string, path string) error {
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

// OptimizeReplication sets the optimal settings for replication, which reduces the replication lag.
// Cannot be used permanently due to the instability of the replica in this state
func (n *Node) OptimizeReplication() error {
	err := n.exec(querySetInnodbFlushLogAtTrxCommit, map[string]any{"level": OptimalInnodbFlushLogAtTrxCommitValue})
	if err != nil {
		return err
	}
	err = n.exec(querySetSyncBinlog, map[string]any{"sync_binlog": OptimalSyncBinlogValue})
	if err != nil {
		return err
	}
	return nil
}

type ReplicationSettings struct {
	InnodbFlushLogAtTrxCommit int `db:"InnodbFlushLogAtTrxCommit"`
	SyncBinlog                int `db:"SyncBinlog"`
}

func (rs *ReplicationSettings) Equal(anotherRs *ReplicationSettings) bool {
	if rs.SyncBinlog == anotherRs.SyncBinlog &&
		rs.InnodbFlushLogAtTrxCommit == anotherRs.InnodbFlushLogAtTrxCommit {
		return true
	}
	return false
}

// There may be cases where the user specified master settings that are higher than our "optimal" ones or equal to them.
func (rs *ReplicationSettings) CanBeOptimized() bool {
	if rs.SyncBinlog >= OptimalSyncBinlogValue {
		return false
	}

	if rs.InnodbFlushLogAtTrxCommit != OptimalInnodbFlushLogAtTrxCommitValue &&
		rs.InnodbFlushLogAtTrxCommit != DefaultInnodbFlushLogAtTrxCommitValue {
		return false
	}

	return true
}

// GetReplicationSettings retrieves replication settings from the host
func (n *Node) GetReplicationSettings() (ReplicationSettings, error) {
	var rs ReplicationSettings
	err := n.queryRow(queryGetReplicationSettings, nil, &rs)
	return rs, err
}

// SetReplicationSettings sets values for replication for the host
func (n *Node) SetReplicationSettings(rs ReplicationSettings) error {
	err := n.exec(querySetInnodbFlushLogAtTrxCommit, map[string]any{"level": rs.InnodbFlushLogAtTrxCommit})
	if err != nil {
		return err
	}
	err = n.exec(querySetSyncBinlog, map[string]any{"sync_binlog": rs.SyncBinlog})
	if err != nil {
		return err
	}
	return nil
}

// SetDefaultReplicationSettings sets default values for replication based on the value on the master
func (n *Node) SetDefaultReplicationSettings(masterNode *Node) error {
	var rs ReplicationSettings
	err := masterNode.queryRow(queryGetReplicationSettings, nil, &rs)
	if err != nil {
		return err
	}
	err = n.exec(querySetInnodbFlushLogAtTrxCommit, map[string]any{"level": rs.InnodbFlushLogAtTrxCommit})
	if err != nil {
		return err
	}
	err = n.exec(querySetSyncBinlog, map[string]any{"sync_binlog": rs.SyncBinlog})
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) GetReplMonTS(replMonSchemeName string, replMonTable string) (string, error) {
	result := new(ReplMonTS)
	err := n.queryRowMogrify(queryGetReplMonTS,
		map[string]any{
			"replMonSchemeName": schemaname(replMonSchemeName),
			"replMonTable":      schemaname(replMonTable),
		},
		result)
	return result.Timestamp, err
}

func (n *Node) CalcReplMonTSDelay(replMonSchemeName string, replMonTable string, ts string) (int64, error) {
	result := new(ReplMonTSDelay)
	err := n.queryRowMogrify(queryCalcReplMonTSDelay,
		map[string]any{
			"ts":                ts,
			"replMonSchemeName": schemaname(replMonSchemeName),
			"replMonTable":      schemaname(replMonTable),
		},
		result)
	return result.Delay, err
}

func (n *Node) CreateReplMonTable(replMonSchemeName string, replMonTable string) error {
	return n.execMogrify(queryCreateReplMonTable, map[string]any{
		"replMonSchemeName": schemaname(replMonSchemeName),
		"replMonTable":      schemaname(replMonTable),
	})
}

func (n *Node) UpdateReplMonTable(replMonSchemeName string, replMonTable string) error {
	return n.execMogrify(queryUpdateReplMon, map[string]any{
		"replMonSchemeName": schemaname(replMonSchemeName),
		"replMonTable":      schemaname(replMonTable),
	})
}

func (n *Node) GetSlaveStatusQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryReplicaStatus, nil
	} else {
		return querySlaveStatus, nil
	}
}

func (n *Node) GetSlaveOrReplicaStruct() (ReplicaStatus, error) {
	v, err := n.GetVersion()
	if err != nil {
		return nil, err
	}
	if v.CheckIfVersionReplicaStatus() {
		return new(ReplicaStatusStruct), nil
	} else {
		return new(SlaveStatusStruct), nil
	}
}

func (n *Node) GetStopSlaveQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStopReplica, nil
	} else {
		return queryStopSlave, nil
	}
}

func (n *Node) GetStartSlaveQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStartReplica, nil
	} else {
		return queryStartSlave, nil
	}
}

func (n *Node) GetStopSlaveIOThreadQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStopReplicaIOThread, nil
	} else {
		return queryStopSlaveIOThread, nil
	}
}

func (n *Node) GetStartSlaveIOThreadQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStartReplicaIOThread, nil
	} else {
		return queryStartSlaveIOThread, nil
	}
}

func (n *Node) GetStopSlaveSQLThreadQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStopReplicaSQLThread, nil
	} else {
		return queryStopSlaveSQLThread, nil
	}
}

func (n *Node) GetStartSlaveSQLThreadQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryStartReplicaSQLThread, nil
	} else {
		return queryStartSlaveSQLThread, nil
	}
}

func (n *Node) GetResetSlaveQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryResetReplicaAll, nil
	} else {
		return queryResetSlaveAll, nil
	}
}

func (n *Node) GetChangeMasterQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryChangeSource, nil
	} else {
		return queryChangeMaster, nil
	}
}

// This queries were made intentionally identical: in 8.0 status is still SLAVESIDE_DISABLED
// And even in 8.4 doc it said that "MySQL 8.4 normally displays REPLICA_SIDE_DISABLED rather than SLAVESIDE_DISABLED"
func (n *Node) GetListSlaveSideDisabledEventsQuery() (string, error) {
	v, err := n.GetVersion()
	if err != nil {
		return "", err
	}
	if v.CheckIfVersionReplicaStatus() {
		return queryListReplicaSideDisabledEvents, nil
	} else {
		return queryListSlavesideDisabledEvents, nil
	}
}
