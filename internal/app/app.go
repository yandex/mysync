package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	mysql_driver "github.com/go-sql-driver/mysql"
	"github.com/gofrs/flock"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
	"github.com/yandex/mysync/internal/util"
)

// App is main application structure
type App struct {
	state               appState
	logger              *log.Logger
	config              *config.Config
	dcs                 dcs.DCS
	cluster             *mysql.Cluster
	filelock            *flock.Flock
	nodeFailedAt        map[string]time.Time
	streamFromFailedAt  map[string]time.Time
	daemonState         *DaemonState
	daemonMutex         sync.Mutex
	replRepairState     map[string]*ReplicationRepairState
	externalReplication mysql.IExternalReplication
	switchHelper        mysql.ISwitchHelper
}

// NewApp returns new App. Suddenly.
func NewApp(configFile, logLevel string, interactive bool) (*App, error) {
	config, err := config.ReadFromFile(configFile)
	if err != nil {
		return nil, err
	}
	logPath := ""
	if !interactive {
		logLevel = config.LogLevel
		logPath = config.Log
	}
	logger, err := log.Open(logPath, logLevel)
	if err != nil {
		return nil, err
	}
	if logPath != "" {
		logger.ReOpenOnSignal(syscall.SIGUSR2)
	}
	externalReplication, err := mysql.NewExternalReplication(config.ExternalReplicationType, logger)
	if err != nil {
		return nil, err
	}
	switchHelper := mysql.NewSwitchHelper(config)
	app := &App{
		state:               stateFirstRun,
		config:              config,
		logger:              logger,
		nodeFailedAt:        make(map[string]time.Time),
		streamFromFailedAt:  make(map[string]time.Time),
		replRepairState:     make(map[string]*ReplicationRepairState),
		externalReplication: externalReplication,
		switchHelper:        switchHelper,
	}
	return app, nil
}

func (app *App) lockFile() error {
	app.filelock = flock.New(app.config.Lockfile)
	if locked, err := app.filelock.TryLock(); !locked {
		msg := "Possibly another instance is running."
		if err != nil {
			msg = err.Error()
		}
		return fmt.Errorf("failed to acquire lock on %s: %s", app.config.Lockfile, msg)
	}
	return nil
}

func (app *App) unlockFile() {
	_ = app.filelock.Unlock()
}

func (app *App) baseContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()
	return ctx
}

func (app *App) connectDCS() error {
	var err error
	// TODO: support other DCS systems
	app.dcs, err = dcs.NewZookeeper(&app.config.Zookeeper, app.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to zkDCS: %s", err.Error())
	}
	return nil
}

func (app *App) newDBCluster() error {
	var err error
	app.cluster, err = mysql.NewCluster(app.config, app.logger, app.dcs)
	if err != nil {
		return fmt.Errorf("failed to create database cluster %s", err.Error())
	}
	return nil
}

func (app *App) writeEmergeFile(msg string) {
	err := os.WriteFile(app.config.Emergefile, []byte(msg), 0644)
	if err != nil {
		app.logger.Errorf("failed to write emerge file: %v", err)
	}
}

func (app *App) writeResetupFile(msg string) {
	err := os.WriteFile(app.config.Resetupfile, []byte(msg), 0644)
	if err != nil {
		app.logger.Errorf("failed to write resetup file: %v", err)
	}
}

func (app *App) doesResetupFileExist() bool {
	_, err := os.Stat(app.config.Resetupfile)
	return err == nil
}

func (app *App) writeMaintenanceFile() {
	err := os.WriteFile(app.config.Maintenancefile, []byte(""), 0644)
	if err != nil {
		app.logger.Errorf("failed to write maintenance file: %v", err)
	}
}

func (app *App) doesMaintenanceFileExist() bool {
	_, err := os.Stat(app.config.Maintenancefile)
	return err == nil
}

func (app *App) removeMaintenanceFile() {
	err := os.Remove(app.config.Maintenancefile)
	if err != nil && !os.IsNotExist(err) {
		app.logger.Errorf("failed to remove maintenance file: %v", err)
	}
}

// Dynamically calculated version of RplSemiSyncMasterWaitForSlaveCount.
// This variable can be lower than hard-configured RplSemiSyncMasterWaitForSlaveCount
// when some semi-sync replicas are dead.
func (app *App) getRequiredWaitSlaveCount(activeNodes []string) int {
	wsc := len(activeNodes) / 2
	if wsc > app.config.RplSemiSyncMasterWaitForSlaveCount {
		wsc = app.config.RplSemiSyncMasterWaitForSlaveCount
	}
	return wsc
}

// Number of HA nodes to be alive to failover/switchover
func (app *App) getFailoverQuorum(activeNodes []string) int {
	fq := len(activeNodes) - app.getRequiredWaitSlaveCount(activeNodes)
	if fq < 1 {
		fq = 1
	}
	return fq
}

// separate goroutine performing health checks
func (app *App) healthChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.HealthCheckInterval)
	for {
		select {
		case <-ticker.C:
			hc := app.getLocalNodeState()
			app.logger.Infof("healthcheck: %v", hc)
			err := app.dcs.SetEphemeral(dcs.JoinPath(pathHealthPrefix, app.config.Hostname), hc)
			if err != nil {
				app.logger.Errorf("healthcheck: failed to set status to dcs: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// separate gorutine performing info file management
func (app *App) stateFileHandler(ctx context.Context) {
	ticker := time.NewTicker(app.config.InfoFileHandlerInterval)
	for {
		select {
		case <-ticker.C:

			tree, err := app.dcs.GetTree("")
			if err != nil {
				app.logger.Errorf("stateFileHandler: failed to get current zk tree: %v", err)
				_ = os.Remove(app.config.InfoFile)
				continue
			}
			data, err := json.Marshal(tree)
			if err != nil {
				app.logger.Errorf("stateFileHandler: failed to marshal zk node data: %v", err)
				_ = os.Remove(app.config.InfoFile)
				continue
			}
			err = os.WriteFile(app.config.InfoFile, data, 0666)
			if err != nil {
				app.logger.Errorf("stateFileHandler: failed to write info file %v", err)
				_ = os.Remove(app.config.InfoFile)
				continue
			}

		case <-ctx.Done():
			return
		}
	}
}

// checks if update of external CA file required
func (app *App) externalCAFileChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.ExternalCAFileCheckInterval)
	for {
		select {
		case <-ticker.C:
			localNode := app.cluster.Local()
			replicaStatus, err := app.externalReplication.GetReplicaStatus(localNode)
			if err != nil {
				if !mysql.IsErrorChannelDoesNotExists(err) {
					app.logger.Errorf("external CA file checker: host %s failed to get external replica status %v", localNode.Host(), err)
				}
				continue
			}
			if replicaStatus == nil {
				app.logger.Infof("external CA file checker: no external replication found on host %v", localNode.Host())
				continue
			}
			err = localNode.UpdateExternalCAFile()
			if err != nil {
				app.logger.Errorf("external CA file checker: failed check and update CA file: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) replMonWriter(ctx context.Context) {
	ticker := time.NewTicker(app.config.ReplMonWriteInterval)
	for {
		select {
		case <-ticker.C:
			localNode := app.cluster.Local()
			sstatus, err := localNode.GetReplicaStatus()
			if err != nil {
				app.logger.Errorf("repl mon writer: got error %v while checking replica status", err)
				time.Sleep(app.config.ReplMonErrorWaitInterval)
				continue
			}
			if sstatus != nil {
				app.logger.Infof("repl mon writer: host is replica")
				time.Sleep(app.config.ReplMonSlaveWaitInterval)
				continue
			}
			readOnly, _, err := localNode.IsReadOnly()
			if err != nil {
				app.logger.Errorf("repl mon writer: got error %v while checking read only status", err)
				time.Sleep(app.config.ReplMonErrorWaitInterval)
				continue
			}
			if readOnly {
				app.logger.Infof("repl mon writer: host is read only")
				time.Sleep(app.config.ReplMonSlaveWaitInterval)
				continue
			}
			err = localNode.UpdateReplMonTable(app.config.ReplMonSchemeName, app.config.ReplMonTableName)
			if err != nil {
				if mysql.IsErrorTableDoesNotExists(err) {
					err = localNode.CreateReplMonTable(app.config.ReplMonSchemeName, app.config.ReplMonTableName)
					if err != nil {
						app.logger.Errorf("repl mon writer: got error %v while creating repl mon table", err)
					}
					continue
				}
				app.logger.Errorf("repl mon writer: got error %v while writing in repl mon table", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) SetResetupStatus() {
	err := app.setResetupStatus(app.cluster.Local().Host(), app.doesResetupFileExist())
	if err != nil {
		app.logger.Errorf("recovery: failed to set resetup status: %v", err)
	}
}

// separated gorutine for resetuping local mysql
func (app *App) recoveryChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.RecoveryCheckInterval)
	for {
		select {
		case <-ticker.C:
			app.checkRecovery()
			app.checkCrashRecovery()
			app.SetResetupStatus()
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) checkRecovery() {
	if !app.IsRecoveryNeeded(app.config.Hostname) {
		return
	}
	if app.doesResetupFileExist() {
		app.logger.Infof("recovery: resetup file exists, waiting for resetup to complete")
		return
	}

	localNode := app.cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get slave status %v", localNode.Host(), err)
		return
	}
	if sstatus == nil {
		app.logger.Info("recovery: waiting for manager to turn us to a new master")
		return
	}

	var master string
	err = app.dcs.Get(pathMasterNode, &master)
	if err != nil {
		app.logger.Errorf("recovery: failed to get current master from dcs: %v", err)
		return
	}
	err = app.cluster.UpdateHostsInfo()
	if err != nil {
		app.logger.Errorf("recovery: updating hosts info failed due: %s", err)
		return
	}
	masterNode := app.cluster.Get(master)
	mgtids, err := masterNode.GTIDExecutedParsed()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get master status %v", masterNode, err)
		return
	}

	app.logger.Infof("recovery: master %s has GTIDs %s", master, mgtids)
	app.logger.Infof("recovery: local node %s has GTIDs %s", localNode.Host(), sstatus.GetExecutedGtidSet())

	if isSlavePermanentlyLost(sstatus, mgtids) {
		rp, err := localNode.GetReplicaStatus()
		if err == nil {
			if rp.GetLastError() != "" {
				app.logger.Errorf("recovery: local node %s has error: %s", localNode.Host(), rp.GetLastError())
			}
			if rp.GetLastIOError() != "" {
				app.logger.Errorf("recovery: local node %s has IO error: %s", localNode.Host(), rp.GetLastIOError())
			}
		} else {
			app.logger.Errorf("recovery: local node %s is NOT behind the master %s, need RESETUP", localNode.Host(), masterNode)
		}
		app.writeResetupFile("")
	} else {
		readOnly, _, err := localNode.IsReadOnly()
		if err != nil {
			app.logger.Errorf("recovery: failed to check if host is read-only: %v", err)
			return
		}

		if !readOnly {
			app.logger.Errorf("recovery: host is not read-only, we should wait for it...")
			return
		}

		app.logger.Infof("recovery: local node %s is not ahead of master, recovery finished", localNode.Host())
		err = app.ClearRecovery(app.config.Hostname)
		if err != nil {
			app.logger.Errorf("recovery: failed to clear recovery flag in zk: %v", err)
		}
	}
}

func (app *App) checkCrashRecovery() {
	if !app.config.ResetupCrashedHosts {
		return
	}
	if app.doesResetupFileExist() {
		app.logger.Infof("recovery: resetup file exists, waiting for resetup to complete")
		return
	}
	ds, err := app.getLocalDaemonState()
	if err != nil {
		app.logger.Errorf("recovery: failed to get local daemon state: %v", err)
		return
	}
	if !ds.StartTime.IsZero() {
		app.logger.Debugf("recovery: daemon state: start time: %s", ds.StartTime)
	}
	if !ds.RecoveryTime.IsZero() {
		app.logger.Debugf("recovery: daemon state: crash recovery time: %s", ds.RecoveryTime)
	}
	if !ds.CrashRecovery {
		return
	}
	localNode := app.cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		app.logger.Errorf("recovery: host %s failed to get slave status %v", localNode.Host(), err)
		return
	}
	if sstatus == nil {
		app.logger.Info("recovery: resetup after crash recovery may happen only on replicas")
		return
	}
	app.logger.Errorf("recovery: local node %s is running after crash recovery %v, need RESETUP", localNode.Host(), ds.RecoveryTime)
	app.writeResetupFile("")
}

func (app *App) checkHAReplicasRunning(local *mysql.Node) bool {
	checker := func(host string) error {
		node := app.cluster.Get(host)
		status, err := node.ReplicaStatusWithTimeout(app.config.DBLostCheckTimeout, app.config.ReplicationChannel)
		if err != nil {
			return err
		}
		if status == nil {
			return fmt.Errorf("%s is master", host)
		}
		if !status.ReplicationRunning() {
			return fmt.Errorf("replication on host %s is not running", host)
		}
		if status.GetMasterHost() != local.Host() {
			return fmt.Errorf("replication on host %s doesn't streaming from master %s", host, local.Host())
		}
		if !app.config.SemiSync {
			return nil // count all replicas in async-only schema
		}
		ssstatus, err := node.SemiSyncStatus()
		if err != nil {
			return fmt.Errorf("%s %v", host, err)
		}
		if ssstatus.SlaveEnabled == 0 {
			return fmt.Errorf("replica %s is not semi-sync", host)
		}
		return nil
	}

	results := util.RunParallel(checker, app.cluster.HANodeHosts())

	availableReplicas := 0
	for hostname, err := range results {
		if err == nil {
			availableReplicas++
			app.logger.Debugf("mysync HA Replicas check: %s good replica", hostname)
		} else {
			app.logger.Debugf("mysync HA Replicas check: %v", err.Error())
		}
	}

	app.logger.Infof("mysync HA Replicas check: live replicas %d, number of hosts %d ", availableReplicas, len(app.cluster.HANodeHosts()))

	if app.config.SemiSync {
		status, err := local.SemiSyncStatus()
		if err != nil {
			app.logger.Errorf("failed to get semisync status: %v", err)
			return false
		}
		return availableReplicas >= status.WaitSlaveCount
	} else {
		// check connectivity to all replicas:
		return availableReplicas >= len(app.cluster.HANodeHosts())-1
	}
}

func (app *App) stateFirstRun() appState {
	if !app.dcs.WaitConnected(app.config.DcsWaitTimeout) {
		if app.doesMaintenanceFileExist() {
			return stateMaintenance
		}
		return stateFirstRun
	}
	app.dcs.Initialize()
	if app.dcs.AcquireLock(pathManagerLock) {
		return stateManager
	}
	return stateCandidate
}

func (app *App) stateLost() appState {
	if app.dcs.IsConnected() {
		return stateCandidate
	}
	if len(app.cluster.HANodeHosts()) == 1 || !app.cluster.IsHAHost(app.cluster.Local().Host()) {
		// do nothing for 1-node clusters or not ha hosts
		return stateLost
	}

	if app.config.DisableSetReadonlyOnLost {
		app.logger.Infof("mysync have lost connection to ZK. MySQL HA cluster is not reachable. However switching to RO is prohibited by mysync configuration. Do nothing")
		return stateLost
	}

	node := app.cluster.Local()
	localNodeState := app.getLocalNodeState()
	if localNodeState.IsMaster && app.checkHAReplicasRunning(node) {
		app.logger.Infof("mysync have lost connection to ZK. However HA cluster is live. Do nothing")
		return stateLost
	}

	app.logger.Infof("mysync have lost connection to ZK. MySQL HA cluster is not reachable. Switching to RO")
	var err error
	if localNodeState.IsMaster {
		err = node.SetReadOnlyWithForce(app.config.ExcludeUsers, true)

		merr, ok := err.(*mysql_driver.MySQLError)
		if !(errors.Is(err, context.DeadlineExceeded) || ok && merr.Number == 1205) { // Error 1205: Lock wait timeout exceeded; try restarting transaction
			app.logger.Errorf("failed to set node %s read-only: %v", node.Host(), err)
			return stateLost
		}

		isBlocked, err := node.IsWaitingSemiSyncAck()
		if err != nil {
			app.logger.Errorf("Failed to fetch processlist on %s: %v", node.Host(), err)
			return stateLost
		}
		if isBlocked {
			app.logger.Errorf("Master %s is waiting for semi-sync ack, trying to take it offline", node.Host())
			err = app.stopReplicationOnMaster(node)
			if err != nil {
				app.logger.Errorf("node %s: %v", node.Host(), err)
				return stateLost
			}
			err = node.SetReadOnlyWithForce(app.config.ExcludeUsers, true)
			if err != nil {
				app.logger.Errorf("failed to set master %s read-only: %v", node.Host(), err)
				return stateLost
			}
		}
	} else {
		err = node.SetReadOnly(true)
	}
	if err != nil {
		app.logger.Errorf("lost: failed to set local node %s read-only: %v", node.Host(), err)
	}
	gtidExecuted, err := node.GTIDExecutedParsed()
	if err != nil {
		app.logger.Errorf("lost: failed get master status: %v", err)
	}
	app.logger.Infof("lost: gtid executed is %s", gtidExecuted)

	return stateLost
}

func (app *App) stateMaintenance() appState {
	if !app.doesMaintenanceFileExist() {
		app.writeMaintenanceFile()
	}
	maintenance, err := app.GetMaintenance()
	if err != nil && err != dcs.ErrNotFound {
		return stateMaintenance
	}
	if err == dcs.ErrNotFound || maintenance.ShouldLeave {
		if app.dcs.AcquireLock(pathManagerLock) {
			app.logger.Info("leaving maintenance")
			err := app.leaveMaintenance()
			if err != nil {
				app.logger.Errorf("maintenance: failed to leave: %v", err)
				return stateMaintenance
			}
			app.removeMaintenanceFile()
			return stateManager
		} else {
			app.removeMaintenanceFile()
			return stateCandidate
		}
	}
	return stateMaintenance
}

// nolint: gocyclo, funlen
func (app *App) stateManager() appState {
	if !app.dcs.IsConnected() {
		return stateLost
	}
	if !app.dcs.AcquireLock(pathManagerLock) {
		return stateCandidate
	}

	err := app.cluster.UpdateHostsInfo()
	if err != nil {
		// we are connected to dcs, but updating hosts info failed - lets log and ignore
		app.logger.Errorf("updating hosts info failed due: %s", err)
	}

	// clusterState is manager-view of cluster state, up-to-date
	clusterState := app.getClusterStateFromDB()

	// clusterStateDcs is dcs-based view of cluster, may be stale (with healthChecker interval)
	// but contains additional info such as DiskState
	clusterStateDcs, err := app.getClusterStateFromDcs()
	if err != nil {
		app.logger.Errorf("failed to get cluster state from DCS: %s", err)
		return stateManager
	}

	// master is master host that should be on cluster
	master, err := app.getCurrentMaster(clusterState)
	if err != nil {
		app.logger.Errorf("failed to get or identify master: %s", err)
		if errors.Is(err, ErrManyMasters) {
			app.writeEmergeFile(err.Error())
		}
		return stateManager
	}

	// activeNodes are master + alive running replicas
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		app.logger.Errorf(err.Error())
		return stateManager
	}
	app.logger.Infof("active: %v", activeNodes)
	app.logger.Infof("master: %s", master)
	app.logger.Infof("cs: %v", clusterState)
	app.logger.Infof("dcs cs: %v", clusterStateDcs)

	// check if we are in maintenance
	maintenance, err := app.GetMaintenance()
	if err != nil && err != dcs.ErrNotFound {
		app.logger.Errorf("failed to get maintenance from zk %v", err)
		return stateManager
	}
	if maintenance != nil {
		if !maintenance.MySyncPaused {
			app.logger.Info("entering maintenance")
			err := app.enterMaintenance(maintenance, master)
			if err != nil {
				app.logger.Errorf("failed to enter maintenance: %v", err)
				return stateManager
			}
		}
		return stateMaintenance
	}

	// check if switchover required or in progress
	switchover := new(Switchover)
	if err := app.dcs.Get(pathCurrentSwitch, switchover); err == nil {
		err = app.approveSwitchover(switchover, activeNodes, clusterState)
		if err != nil {
			app.logger.Errorf("cannot perform switchover: %s", err)
			err = app.FinishSwitchover(switchover, err)
			if err != nil {
				app.logger.Errorf("failed to reject switchover: %s", err)
			}
			return stateManager
		}

		err = app.StartSwitchover(switchover)
		if err != nil {
			app.logger.Errorf("failed to start switchover: %s", err)
			return stateManager
		}
		err = app.performSwitchover(clusterState, activeNodes, switchover, master)
		if app.dcs.Get(pathCurrentSwitch, new(Switchover)) == dcs.ErrNotFound {
			app.logger.Errorf("switchover was aborted")
		} else {
			if err != nil {
				err = app.FailSwitchover(switchover, err)
				if err != nil {
					app.logger.Errorf("failed to report switchover failure: %s", err)
				}
			} else {
				err = app.FinishSwitchover(switchover, nil)
				if err != nil {
					// we failed to update status in DCS, it's highly possible
					// that current process lost DCS connection
					// and another process will take managerLock
					app.logger.Errorf("failed to report switchover finish: %s", err)
				}
			}
		}
		return stateManager
	} else if err != dcs.ErrNotFound {
		app.logger.Errorf(err.Error())
		return stateManager
	}

	// perform failover if needed
	if !clusterStateDcs[master].PingOk || clusterStateDcs[master].IsFileSystemReadonly {
		app.logger.Errorf("MASTER FAILURE")
		if app.nodeFailedAt[master].IsZero() {
			app.nodeFailedAt[master] = time.Now()
		}
		err = app.approveFailover(clusterState, clusterStateDcs, activeNodes, master)
		if err == nil {
			app.logger.Infof("failover approved")
			err = app.IssueFailover(master)
			if err != nil {
				app.logger.Error(err.Error())
			}
		} else {
			app.logger.Errorf("failover was not approved: %v", err)
		}
		return stateManager
	} else {
		delete(app.nodeFailedAt, master)
	}
	if !clusterState[master].PingOk {
		app.logger.Errorf("MASTER SUSPICIOUS, do not perform any kind of repair")
		return stateManager
	}

	// set hosts online or offline depending on replication lag
	app.repairOfflineMode(clusterState, master)

	// analyze and repair cluster
	app.repairCluster(clusterState, clusterStateDcs, master)

	// perform after-crash failover if needed
	if app.config.ResetupCrashedHosts && countHANodes(clusterState) > 1 && clusterStateDcs[master].DaemonState != nil && clusterStateDcs[master].DaemonState.CrashRecovery {
		app.logger.Errorf("MASTER FAILURE (CRASH RECOVERY)")
		err = app.approveFailover(clusterState, clusterStateDcs, activeNodes, master)
		if err == nil {
			app.logger.Infof("failover approved")
			err = app.IssueFailover(master)
			if err != nil {
				app.logger.Error(err.Error())
			}
			return stateManager
		} else {
			app.logger.Errorf("failover was not approved: %v", err)
		}
	}

	// if everything is fine - update active nodes
	err = app.updateActiveNodes(clusterState, clusterStateDcs, activeNodes, master)
	if err != nil {
		app.logger.Errorf("failed to update active nodes in dcs: %v", err)
	}

	if app.config.ReplMon {
		err = app.updateReplMonTS(master)
		if err != nil {
			app.logger.Errorf("failed to update repl_mon timestamp: %v", err)
		}
	}

	return stateManager
}

func (app *App) approveFailover(clusterState, clusterStateDcs map[string]*NodeState, activeNodes []string, master string) error {
	if !app.config.Failover {
		return fmt.Errorf("auto_failover is disabled in config")
	}
	afterCrashRecovery := false
	if clusterStateDcs[master].DaemonState != nil && clusterStateDcs[master].DaemonState.CrashRecovery && app.config.ResetupCrashedHosts {
		afterCrashRecovery = true
	}
	if afterCrashRecovery {
		app.logger.Infof("approve failover: after crash recovery, skip replication and delay checks")
	} else if clusterStateDcs[master].IsFileSystemReadonly {
		app.logger.Infof("approve failover: filesystem is readonly, skip replication and delay checks")
	} else {
		if countRunningHASlaves(clusterState) == countHANodes(clusterState)-1 {
			return fmt.Errorf("all replics are alive and running replication, seems zk problems")
		}
		if app.config.FailoverDelay > 0 {
			failingTime := time.Since(app.nodeFailedAt[master])
			if failingTime < app.config.FailoverDelay {
				return fmt.Errorf("failover delay is not yet elapsed: remaining %v", app.config.FailoverDelay-failingTime)
			}
		}
	}

	app.logger.Infof("approve failover: active nodes are %v", activeNodes)
	// number of active slaves that we can use to perform switchover
	permissibleSlaves := countAliveHASlavesWithinNodes(activeNodes, clusterState)
	if app.config.SemiSync {
		failoverQuorum := app.getFailoverQuorum(activeNodes)
		if permissibleSlaves < failoverQuorum {
			return fmt.Errorf("no quorum, have %d replics while %d is required", permissibleSlaves, failoverQuorum)
		}
	} else {
		if permissibleSlaves == 0 {
			return fmt.Errorf("no alive active replica found")
		}
	}

	var lastSwitchover Switchover
	err := app.dcs.Get(pathLastSwitch, &lastSwitchover)
	if err != dcs.ErrNotFound {
		if err != nil {
			return err
		}
		if lastSwitchover.Result == nil {
			return fmt.Errorf("another switchover in progress. this should never happen")
		}
		timeAfterLastSwitchover := time.Since(lastSwitchover.Result.FinishedAt)
		if timeAfterLastSwitchover < app.config.FailoverCooldown && lastSwitchover.Cause == CauseAuto {
			return fmt.Errorf("not enough time from last failover %s (cooldown %s)", lastSwitchover.Result.FinishedAt, app.config.FailoverCooldown)
		}
	}
	return nil
}

func (app *App) stateCandidate() appState {
	if !app.dcs.IsConnected() {
		return stateLost
	}
	err := app.cluster.UpdateHostsInfo()
	if err != nil {
		app.logger.Errorf("candidate: failed to update host info zk %v", err)
		return stateCandidate
	}
	maintenance, err := app.GetMaintenance()
	if err != nil && err != dcs.ErrNotFound {
		app.logger.Errorf("candidate: failed to get maintenance from zk %v", err)
		return stateCandidate
	}
	// candidate enters maintenane only after manager, when mysync already paused
	if maintenance != nil && maintenance.MySyncPaused {
		return stateMaintenance
	}
	if app.dcs.AcquireLock(pathManagerLock) {
		return stateManager
	}
	return stateCandidate
}

func (app *App) emulateError(pos string) bool {
	if !app.config.DevMode {
		return false
	}
	ee := util.GetEnvVariable("MYSYNC_EMULATE_ERROR", "")
	for _, emulatedError := range strings.Split(ee, ",") {
		if pos == emulatedError {
			app.logger.Debugf("devmode: error emulated: %s", pos)
			return true
		}
	}
	return false
}

func (app *App) approveSwitchover(switchover *Switchover, activeNodes []string, clusterState map[string]*NodeState) error {
	if switchover.RunCount > 0 {
		return nil
	}
	if app.config.SemiSync {
		// number of active slaves that we can use to perform switchover
		permissibleSlaves := countAliveHASlavesWithinNodes(activeNodes, clusterState)
		failoverQuorum := app.getFailoverQuorum(activeNodes)
		if permissibleSlaves < failoverQuorum {
			return fmt.Errorf("no quorum, have %d replics while %d is required", permissibleSlaves, failoverQuorum)
		}
	}
	return nil
}

/*
Returns list of hosts that should be active at the moment
Typically it's master + list of alive, replicating, not split-brained replicas
*/
func (app *App) calcActiveNodes(clusterState, clusterStateDcs map[string]*NodeState, oldActiveNodes []string, master string) (activeNodes []string, err error) {
	masterNode := app.cluster.Get(master)
	hostsOnRecovery, err := app.GetHostsOnRecovery()
	if err != nil {
		app.logger.Errorf("failed to get hosts on recovery: %v", err)
		return nil, err
	}
	mgtids, err := masterNode.GTIDExecutedParsed()
	if err != nil {
		app.logger.Warnf("failed to get master status %v", err)
		return nil, err
	}
	muuid, err := masterNode.UUID()
	if err != nil {
		app.logger.Warnf("failed to get master uuid %v", err)
		return nil, err
	}

	for host, node := range clusterState {
		if host == master {
			activeNodes = append(activeNodes, master)
			continue
		}
		if node.IsCascade {
			continue
		}
		if hostsOnRecovery != nil && util.ContainsString(hostsOnRecovery, host) {
			continue
		}
		if !node.PingOk {
			if node.PingDubious || clusterStateDcs[host].PingOk {
				// we can't rely on ping and slave status if ping was dubios
				if util.ContainsString(oldActiveNodes, host) {
					app.logger.Warnf("calc active nodes: %s is dubious or keep heath lock in dcs, keeping active...", host)
					activeNodes = append(activeNodes, host)
				}
				continue
			}
			if app.nodeFailedAt[host].IsZero() {
				app.nodeFailedAt[host] = time.Now()
			}
			failingTime := time.Since(app.nodeFailedAt[host])
			if failingTime < app.config.InactivationDelay {
				if util.ContainsString(oldActiveNodes, host) {
					app.logger.Warnf("calc active nodes: %s is failing: remaining %v", host, app.config.InactivationDelay-failingTime)
					activeNodes = append(activeNodes, host)
				}
			} else {
				app.logger.Errorf("calc active nodes: %s is down, deleting from active...", host)
			}
			continue
		} else {
			delete(app.nodeFailedAt, host)
		}
		sstatus := node.SlaveState
		if sstatus == nil {
			app.logger.Warnf("calc active nodes: lost master %s", host)
			continue
		}
		sgtids := gtids.ParseGtidSet(sstatus.ExecutedGtidSet)
		if sstatus.ReplicationState != mysql.ReplicationRunning || isSplitBrained(sgtids, mgtids, muuid) {
			app.logger.Errorf("calc active nodes: %s is not replicating or splitbrained, deleting from active...", host)
			continue
		}
		activeNodes = append(activeNodes, host)
	}

	sort.Strings(activeNodes)
	return activeNodes, nil
}

func (app *App) calcActiveNodesChanges(clusterState map[string]*NodeState, activeNodes []string, oldActiveNodes []string, master string) (becomeActive, becomeInactive []string, err error) {
	masterNode := app.cluster.Get(master)
	var syncReplicas []string
	var deadReplicas []string

	for host, state := range clusterState {
		if state.SemiSyncState != nil && state.SemiSyncState.SlaveEnabled {
			syncReplicas = append(syncReplicas, host)
		}
		if !state.PingOk || state.SlaveState == nil {
			deadReplicas = append(deadReplicas, host)
		}
	}
	becomeActive = filterOut(filterOut(activeNodes, syncReplicas), deadReplicas)
	becomeInactive = filterOut(syncReplicas, activeNodes)

	if len(oldActiveNodes) == 1 && oldActiveNodes[0] == master && len(becomeActive) == 0 {
		// When replicas with semi-sync enabled returns to lone master,
		// we should mark all of them as "become active" to restart replication
		// because master may become stuck otherwise
		for _, host := range activeNodes {
			if host != master {
				becomeActive = append(becomeActive, host)
			}
		}
	}

	if len(becomeActive) > 0 {
		// Some replicas are going to become semi-sync.
		// We need to check that they downloaded (not replayed) almost all binary logs,
		// in order to prevent master freezing.
		// We can't check all the replicas on each iteration, because SHOW BINARY LOGS is pretty heavy request
		var dataLagging []string
		masterBinlogs, err := masterNode.GetBinlogs()
		if err != nil {
			app.logger.Errorf("calc active nodes: failed to list master binlogs on %s: %v", master, err)
			return nil, nil, err
		}
		for _, host := range becomeActive {
			slaveState := clusterState[host].SlaveState
			dataLag := calcLagBytes(masterBinlogs, slaveState.MasterLogFile, slaveState.MasterLogPos)
			if dataLag > app.config.SemiSyncEnableLag {
				app.logger.Warnf("calc active nodes: %v should become active, but it has data lag %d, delaying...", host, dataLag)
				dataLagging = append(dataLagging, host)
				becomeInactive = append(becomeInactive, host)
			}
		}
		becomeActive = filterOut(becomeActive, dataLagging)
	}
	return becomeActive, becomeInactive, nil
}

/*
Active nodes are used to compute quorum for failover.

List of active nodes should be more or the same nodes as [ actual semisync replicas + master ]
Ideally, these list should be equal, but we can't get the list of replicas and put it into DCS atomically.
So there may be the moments when active nodes contains nodes, that are not alive semisync replicas.

If active nodes contains more node than actually alive and sync at the moment - mysync will not failover.
If there are alive sync replicas, that are not in active list - mysync will loose data during failover.

So, it's better to have dead sync replica in active list, than alive sync replica outside of it.
*/
func (app *App) updateActiveNodes(clusterState, clusterStateDcs map[string]*NodeState, oldActiveNodes []string, master string) error {
	masterNode := app.cluster.Get(master)
	masterState := clusterState[master]
	activeNodes, err := app.calcActiveNodes(clusterState, clusterStateDcs, oldActiveNodes, master)
	if err != nil {
		app.logger.Errorf("update active nodes: failed to calc new active nodes: %v", err)
		return err
	}

	if !app.config.SemiSync {
		// disable semi-sync on hosts
		for host, state := range clusterState {
			node := app.cluster.Get(host)
			app.disableSemiSyncIfNonNeeded(node, state)
		}
		// than update DCS
		err = app.dcs.Set(pathActiveNodes, activeNodes)
		if err != nil {
			app.logger.Errorf("update active nodes: failed to update active nodes in dcs %v", err)
			return err
		}
		return nil
	}

	becomeActive, becomeInactive, err := app.calcActiveNodesChanges(clusterState, activeNodes, oldActiveNodes, master)
	if err != nil {
		app.logger.Errorf("update active nodes: failed to calc active nodes changes: %v", err)
		return err
	}
	oldWaitSlaveCount := 0
	if masterState.SemiSyncState != nil && masterState.SemiSyncState.MasterEnabled {
		oldWaitSlaveCount = masterState.SemiSyncState.WaitSlaveCount
	}
	waitSlaveCount := app.getRequiredWaitSlaveCount(activeNodes)

	app.logger.Infof("update active nodes: active nodes are: %v, wait_slave_count %d", activeNodes, waitSlaveCount)
	if len(becomeActive) > 0 {
		app.logger.Infof("update active nodes: slave enter HA-group: %v", becomeActive)
	}
	if len(becomeInactive) > 0 {
		app.logger.Infof("update active nodes: slave leave HA-group: %v", becomeInactive)
	}
	if oldWaitSlaveCount != waitSlaveCount {
		app.logger.Infof("update active nodes: master change wait_slave_count: %d => %d", oldWaitSlaveCount, waitSlaveCount)
	}

	// double check master status before inactivating nodes
	if ok, err := masterNode.Ping(); !ok {
		app.logger.Errorf("update active nodes: MASTER SUSPICIOUS, do not update active nodes: %v", err)
		return err
	}

	// first, shrink HA-group, if needed
	if waitSlaveCount > oldWaitSlaveCount {
		err := app.adjustSemiSyncOnMaster(masterNode, clusterState[master], waitSlaveCount)
		if err != nil {
			app.logger.Errorf("failed to adjust semi-sync on master %s to %d: %v", masterNode.Host(), waitSlaveCount, err)
			return err
		}
	}
	for _, host := range becomeInactive {
		err = app.disableSemiSyncOnSlave(host)
		if err != nil {
			app.logger.Warnf("failed to disable semi-sync on slave %s: %v", host, err)
			return err
		}
	}

	// than update DCS
	err = app.dcs.Set(pathActiveNodes, activeNodes)
	if err != nil {
		app.logger.Errorf("update active nodes: failed to update active nodes in dcs %v", err)
		return err
	}

	// and finally enlarge HA-group, if needed
	for _, host := range becomeActive {
		err := app.enableSemiSyncOnSlave(host)
		if err != nil {
			app.logger.Errorf("failed to enable semi-sync on slave %s: %v", host, err)
		}
	}
	if waitSlaveCount < oldWaitSlaveCount {
		err := app.adjustSemiSyncOnMaster(masterNode, clusterState[master], waitSlaveCount)
		if err != nil {
			app.logger.Errorf("failed to adjust semi-sync on master %s to %d: %v", masterNode.Host(), waitSlaveCount, err)
		}
	}

	return nil
}

func (app *App) adjustSemiSyncOnMaster(node *mysql.Node, state *NodeState, waitSlaveCount int) error {
	if state.SemiSyncState == nil {
		return fmt.Errorf("semi-sync state is empty for %s", node.Host())
	}
	if waitSlaveCount == 0 {
		// technically we can't set wait_slave_count to zero, so we disable master plugin instead
		if state.SemiSyncState.MasterEnabled {
			err := node.SemiSyncDisable()
			if err != nil {
				return err
			}
		}
	} else {
		if state.SemiSyncState.WaitSlaveCount != waitSlaveCount {
			err := node.SetSemiSyncWaitSlaveCount(waitSlaveCount)
			if err != nil {
				return err
			}
		}
		if !state.SemiSyncState.MasterEnabled {
			err := node.SemiSyncSetMaster()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (app *App) enableSemiSyncOnSlave(host string) error {
	node := app.cluster.Get(host)
	err := node.SemiSyncSetSlave()
	if err != nil {
		app.logger.Errorf("failed to enable semi_sync_slave on %s: %s", host, err)
		return err
	}
	err = node.RestartReplica()
	if err != nil {
		app.logger.Errorf("failed restart replication after set semi_sync_slave on %s: %s", host, err)
		return err
	}
	return nil
}

func (app *App) disableSemiSyncOnSlave(host string) error {
	node := app.cluster.Get(host)
	err := node.SemiSyncDisable()
	if err != nil {
		app.logger.Errorf("failed to enable semi_sync_slave on %s: %s", host, err)
		return err
	}
	err = node.RestartSlaveIOThread()
	if err != nil {
		app.logger.Errorf("failed restart slave io thread after set semi_sync_slave on %s: %s", host, err)
		return err
	}
	return nil
}

func (app *App) disableSemiSyncIfNonNeeded(node *mysql.Node, state *NodeState) {
	if state.SemiSyncState != nil && (state.SemiSyncState.MasterEnabled || state.SemiSyncState.SlaveEnabled) {
		host := node.Host()
		app.logger.Warnf("update_active_nodes: semi_sync is enabled on host %s, but disabled in mysync config. turning it off..", host)
		err := node.SemiSyncDisable()
		if err != nil {
			app.logger.Errorf("update_active_nodes: failed to disable semi_sync on %s: %s", host, err)
		}
	}
}

// nolint: gocyclo, funlen
func (app *App) performSwitchover(clusterState map[string]*NodeState, activeNodes []string, switchover *Switchover, oldMaster string) error {
	if switchover.To != "" {
		if !util.ContainsString(activeNodes, switchover.To) {
			return errors.New("switchover: failed: replica is not active, can't switch to it")
		}
	}
	// do not perform switchover if we have connection problems with some hosts
	if dubious := getDubiousHAHosts(clusterState); len(dubious) > 0 {
		return fmt.Errorf("switchover: failed to ping hosts: %v with dubious errors", dubious)
	}

	// calc failoverQuorum before filtering out old master
	failoverQuorum := app.getFailoverQuorum(activeNodes)
	oldActiveNodes := activeNodes

	// filter out old master as may hang and timeout in different ways
	if switchover.Cause == CauseAuto && switchover.From == oldMaster {
		activeNodes = filterOut(activeNodes, []string{oldMaster})
	}

	// set read only everywhere (all HA-nodes) and stop replication
	app.logger.Info("switchover: phase 1: enter read only")
	errs := util.RunParallel(func(host string) error {
		if !clusterState[host].PingOk {
			return fmt.Errorf("switchover: failed to ping host %s", host)
		}
		node := app.cluster.Get(host)
		// in case node is a master
		err := node.SetReadOnly(true)
		if err != nil || app.emulateError("freeze_ro") {
			app.logger.Infof("switchover: failed to set node %s read-only, trying kill bad queries: %v", host, err)
			if err := node.SetReadOnlyWithForce(app.config.ExcludeUsers, true); err != nil {
				return fmt.Errorf("failed to set node %s read-only: %v", host, err)
			}
		}
		app.logger.Infof("switchover: host %s set read-only", host)
		return nil
	}, activeNodes)

	// if master was not among activeNodes - there will be no key in errs
	if err, ok := errs[oldMaster]; ok && err != nil && clusterState[oldMaster].PingOk {
		err = fmt.Errorf("switchover: failed to set old master %s read-only %s", oldMaster, err)
		app.logger.Info(err.Error())
		switchErr := app.FinishSwitchover(switchover, err)
		if switchErr != nil {
			return fmt.Errorf("switchover: failed to reject switchover %s", switchErr)
		}
		app.logger.Info("switchover: rejected")
		return err
	}

	app.logger.Info("switchover: phase 2: stop replication")

	oldMasterNode := app.cluster.Get(oldMaster)
	if clusterState[oldMaster].PingOk {
		err := app.externalReplication.Stop(oldMasterNode)
		if err != nil {
			return fmt.Errorf("got error: %s while stopping external replication on old master: %s", err, oldMaster)
		}
	}

	errs2 := util.RunParallel(func(host string) error {
		if !clusterState[host].PingOk {
			return fmt.Errorf("switchover: failed to ping host %s", host)
		}
		node := app.cluster.Get(host)
		// in case node is a replica
		err := node.StopSlaveIOThread()
		if err != nil || app.emulateError("freeze_stop_slave_io") {
			return fmt.Errorf("failed to stop slave on host %s: %s", host, err)
		}
		app.logger.Infof("switchover: host %s replication IO thread stopped", host)
		return nil
	}, activeNodes)

	// count successfully stopped active nodes and check one more time that we have a quorum
	var frozenActiveNodes []string
	for _, host := range activeNodes {
		if errs[host] == nil && errs2[host] == nil {
			frozenActiveNodes = append(frozenActiveNodes, host)
		}
	}
	if len(frozenActiveNodes) < failoverQuorum {
		return fmt.Errorf("no failoverQuorum: has %d frozen active nodes, while %d is required", len(frozenActiveNodes), failoverQuorum)
	}

	// setting server read-only may take a while so we need to ensure we are still a manager
	if !app.dcs.AcquireLock(pathManagerLock) || app.emulateError("set_read_only_lost_lock") {
		return errors.New("manger lock lost during switchover, new manager should finish the process, leaving")
	}

	// collect active host positions
	app.logger.Info("switchover: phase 3: find most up-to-date host")
	positions, err := app.getNodePositions(frozenActiveNodes)
	if err != nil {
		return err
	}
	if len(positions) != len(frozenActiveNodes) {
		return errors.New("switchover: failed to collect positions")
	}
	if len(positions) == 1 && switchover.From == positions[0].host {
		return fmt.Errorf("switchover: no suitable nodes to switchover from  %s, delaying", switchover.From)
	}

	// find most recent host
	mostRecent, mostRecentGtidSet, splitbrain := findMostRecentNodeAndDetectSplitbrain(positions)
	if splitbrain {
		app.writeEmergeFile("splitbrain detected")
		app.logger.Errorf("SPLITBRAIN")
		for _, pos := range positions {
			app.logger.Errorf("splitbrain: %s: %s", pos.host, pos.gtidset)
		}
		return fmt.Errorf("splitbrain detected")
	}
	app.logger.Infof("switchover: most up-to-date node is %s with gtidset %s", mostRecent, mostRecentGtidSet)

	// choose new master
	var newMaster string
	if switchover.To != "" {
		newMaster = switchover.To
	} else if switchover.From != "" {
		positions2 := filterOutNodeFromPositions(positions, switchover.From)
		// we ignore splitbrain flag as it should be handled during searching most recent host
		newMaster, err = getMostDesirableNode(app.logger, positions2, app.switchHelper.GetPriorityChoiceMaxLag())
		if err != nil {
			return fmt.Errorf("switchover: error while looking for highest priority node: %s", switchover.From)
		}
	} else {
		newMaster = mostRecent
	}
	app.logger.Infof("switchover: newMaster is %s", newMaster)

	newMasterNode := app.cluster.Get(newMaster)

	// catch up
	app.logger.Info("switchover: phase 4: catch up if needed")
	if newMaster != mostRecent {
		app.logger.Infof("switchover: new master %s differs from most recent host %s, need to catch up", newMaster, mostRecent)
		err := app.cluster.Get(mostRecent).SetOnline()
		if err != nil || app.emulateError("catchup_set_most_recent_online") {
			return err
		}
		err = app.performChangeMaster(newMaster, mostRecent)
		if err != nil || app.emulateError("catchup_change_master") {
			return err
		}
	} else {
		app.logger.Infof("switchover: new master %s is the most recent host, waiting for all binlogs to be applied", newMaster)
	}
	caught, err := app.waitForCatchUp(newMasterNode, mostRecentGtidSet, app.config.SlaveCatchUpTimeout, time.Second)
	if err != nil || app.emulateError("catchup_master_status") {
		return fmt.Errorf("failed to get gtid executed from %s: %s", newMaster, err)
	}
	if !caught || app.emulateError("catchup_failed") {
		return fmt.Errorf("new master %s failed to catch up %s within %s",
			newMaster, mostRecent, app.config.SlaveCatchUpTimeout)
	}
	// catching up may take a while so we need to ensure we are still a manager
	if !app.dcs.AcquireLock(pathManagerLock) || app.emulateError("catchup_lost_lock") {
		return errors.New("manger lock lost during switchover, new manager should finish the process, leaving")
	}
	app.logger.Infof("switchover: new master %s caught up", newMaster)

	// update lost servers list, it may change during catchup
	clusterState = app.getClusterStateFromDB()
	if !clusterState[newMaster].PingOk {
		return fmt.Errorf("new master %s suddenly became not available during switchover", newMaster)
	}
	if dubious := getDubiousHAHosts(clusterState); len(dubious) > 0 {
		return fmt.Errorf("switchover: failed to ping hosts: %v with dubious errors", dubious)
	}

	// turn slaves to the new master
	app.logger.Info("switchover: phase 5: turn to the new master")
	err = app.cluster.Get(newMaster).SetOnline()
	if err != nil {
		return fmt.Errorf("got error on setting new master %s online %v", newMaster, err)
	}
	errs = util.RunParallel(func(host string) error {
		if host == newMaster || !clusterState[host].PingOk {
			return nil
		}
		err := app.performChangeMaster(host, newMaster)
		if err != nil || app.emulateError("change_master_failed") {
			return err
		}
		return nil
	}, activeNodes)

	err = util.CombineErrors(errs)
	if err != nil {
		return err
	}

	// check if need recover old master
	oldMasterSlaveStatus, err := oldMasterNode.GetReplicaStatus()
	app.logger.Infof("switchover: old master slave status: %#v", oldMasterSlaveStatus)
	if err != nil || oldMasterSlaveStatus == nil || isSlavePermanentlyLost(oldMasterSlaveStatus, mostRecentGtidSet) {
		err = app.SetRecovery(oldMaster)
		if err != nil {
			return fmt.Errorf("failed to set old master %s to recovery: %v", oldMaster, err)
		}
	} else {
		app.logger.Infof("switchover: old master %s does not need recovery", oldMaster)
		err = app.externalReplication.Reset(oldMasterNode)
		if err != nil {
			return fmt.Errorf("got error: %s while reseting external replication on old master: %s", err, oldMaster)
		}
	}

	// promote new master
	app.logger.Info("switchover: phase 6: promote new master")
	err = newMasterNode.StopSlave()
	if err != nil || app.emulateError("promote_stop_slave") {
		return fmt.Errorf("failed to stop slave on new master %s: %s", newMaster, err)
	}
	err = newMasterNode.ResetSlaveAll()
	if err != nil || app.emulateError("promote_reset_slave") {
		return fmt.Errorf("failed to promote new master %s: %s", newMaster, err)
	}
	app.logger.Infof("switchover: new master %s promoted", newMaster)

	// adjust semi-sync before finishing switchover
	clusterState = app.getClusterStateFromDB()
	err = app.updateActiveNodes(clusterState, clusterState, oldActiveNodes, newMaster)
	if err != nil || app.emulateError("update_active_nodes") {
		app.logger.Warnf("switchover: failed to update active nodes after switchover: %v", err)
	}

	// set new master writable
	err = newMasterNode.SetWritable()
	if err != nil || app.emulateError("promote_set_writable") {
		return fmt.Errorf("failed to set new master %s writable: %s", newMaster, err)
	}
	app.logger.Infof("switchover: new master %s set writable", newMaster)

	// reenable events
	events, err := newMasterNode.ReenableEventsRetry()
	if err != nil || app.emulateError("promote_reenable_events") {
		return fmt.Errorf("failed to reenable slaveside disabled events on %s: %s", newMaster, err)
	}
	if len(events) > 0 {
		app.logger.Infof("switchover: events reenabled on %s: %v", newMaster, events)
	}

	// enable external replication
	err = app.externalReplication.Set(newMasterNode)
	if err != nil {
		app.logger.Errorf("failed to set external replication on new master")
	}

	// set new master in dcs
	err = app.dcs.Set(pathMasterNode, newMaster)
	if err != nil || app.emulateError("promote_set_to_dcs") {
		return fmt.Errorf("failed to set new master to dcs: %s", err)
	}

	return nil
}

func (app *App) getCurrentMaster(clusterState map[string]*NodeState) (string, error) {
	master, err := app.GetMasterHostFromDcs()
	if master != "" && err == nil {
		return master, err
	}
	return app.ensureCurrentMaster(clusterState)
}

func (app *App) ensureCurrentMaster(clusterState map[string]*NodeState) (string, error) {
	master, err := app.getMasterHost(clusterState)
	if err != nil {
		return "", err
	}
	if master == "" {
		return "", ErrNoMaster
	}
	return app.SetMasterHost(master)
}

func (app *App) getMasterHost(clusterState map[string]*NodeState) (string, error) {
	masters := make([]string, 0)
	for host, state := range clusterState {
		if state.PingOk && state.IsMaster {
			masters = append(masters, host)
		}
	}
	if len(masters) > 1 {
		return "", fmt.Errorf("%w: %s", ErrManyMasters, masters)
	}
	if len(masters) == 0 {
		return "", nil
	}
	return masters[0], nil
}

func (app *App) repairOfflineMode(clusterState map[string]*NodeState, master string) {
	masterNode := app.cluster.Get(master)
	for host, state := range clusterState {
		if !state.PingOk {
			continue
		}
		node := app.cluster.Get(host)
		if host == master {
			app.repairMasterOfflineMode(host, node, state)
		} else {
			app.repairSlaveOfflineMode(host, node, state, masterNode, clusterState[master])
		}
	}
}

func (app *App) repairMasterOfflineMode(host string, node *mysql.Node, state *NodeState) {
	if state.IsOffline {
		if app.IsRecoveryNeeded(host) {
			return
		}
		err := node.SetOnline()
		if err != nil {
			app.logger.Errorf("repair: failed to set master %s online: %s", host, err)
		} else {
			app.logger.Infof("repair: master %s set online", host)
		}
	}
}

func (app *App) repairSlaveOfflineMode(host string, node *mysql.Node, state *NodeState, masterNode *mysql.Node, masterState *NodeState) {
	if state.SlaveState != nil && state.SlaveState.ReplicationLag != nil {
		replPermBroken, _ := state.IsReplicationPermanentlyBroken()
		if state.IsOffline && *state.SlaveState.ReplicationLag <= app.config.OfflineModeDisableLag.Seconds() {
			if replPermBroken {
				app.logger.Infof("repair: replica %s is permanently broken, won't set online", host)
				return
			}
			resetupStatus, err := app.GetResetupStatus(host)
			if err != nil {
				app.logger.Errorf("repair: failed to get resetup status from host %s: %v", host, err)
				return
			}
			startupTime, err := node.GetStartupTime()
			if err != nil {
				app.logger.Errorf("repair: failed to get mysql startup time from host %s: %v", host, err)
				return
			}
			if resetupStatus.Status || resetupStatus.UpdateTime.Before(startupTime) {
				app.logger.Errorf("repair: should not turn slave to online until get actual resetup status")
				return
			}
			err = node.SetDefaultReplicationSettings(masterNode)
			if err != nil {
				app.logger.Errorf("repair: failed to set default replication settings on slave %s: %s", host, err)
			}
			err = node.SetOnline()
			if err != nil {
				app.logger.Errorf("repair: failed to set slave %s online: %s", host, err)
			} else {
				app.logger.Infof("repair: slave %s set online, because ReplicationLag (%f s) <= OfflineModeDisableLag (%v)",
					host, *state.SlaveState.ReplicationLag, app.config.OfflineModeDisableLag)
			}
		}
		if !state.IsOffline && !masterState.IsReadOnly && *state.SlaveState.ReplicationLag > app.config.OfflineModeEnableLag.Seconds() {
			err := node.SetOffline()
			if err != nil {
				app.logger.Errorf("repair: failed to set slave %s offline: %s", host, err)
			} else {
				app.logger.Infof("repair: slave %s set offline, because ReplicationLag (%f s) >= OfflineModeEnableLag (%v)",
					host, *state.SlaveState.ReplicationLag, app.config.OfflineModeEnableLag)
				err = node.OptimizeReplication()
				if err != nil {
					app.logger.Errorf("repair: failed to set optimize replication settings on slave %s: %s", host, err)
				}
			}
		}
		// gradual transfer of permanently broken nodes to offline
		lastShutdownNodeTime, err := app.GetLastShutdownNodeTime()
		if err != nil {
			app.logger.Errorf("repair: failed to get last shutdown node time: %s", err)
			return
		}
		setOfflineIsPossible := time.Since(lastShutdownNodeTime) > app.config.OfflineModeEnableInterval
		if !state.IsOffline && replPermBroken && setOfflineIsPossible {
			err = app.UpdateLastShutdownNodeTime()
			if err != nil {
				app.logger.Errorf("repair: failed to update last shutdown node time: %s", err)
			}
			err = node.SetOffline()
			if err != nil {
				app.logger.Errorf("repair: failed to set slave %s offline: %s", host, err)
			} else {
				app.logger.Infof("repair: slave %s set offline, because replication permanently broken", host)
			}
		}
	}
}

// nolint: gocyclo
func (app *App) repairReadOnlyOnMaster(masterNode *mysql.Node, masterState *NodeState, clusterStateDcs map[string]*NodeState) {
	needRo := false
	// we set as true because we need to wish to master writable during first run,
	// before other replicas reported their statuses
	mayWrite := true
	replicasRunning := 0
	replicasLow := 0
	replicasNormal := 0
	for host, node := range clusterStateDcs {
		if node.DiskState == nil {
			continue
		}
		if node.IsMaster && masterNode.Host() == host {
			if node.DiskState.Usage() >= app.config.CriticalDiskUsage {
				app.logger.Errorf("diskusage: master %s has critical disk usage %0.2f%%", host, node.DiskState.Usage())
				needRo = true
			} else if node.DiskState.Usage() > app.config.NotCriticalDiskUsage {
				app.logger.Warnf("diskusage: master %s has grey-zone disk usage %0.2f%%", host, node.DiskState.Usage())
				mayWrite = false
			}
		} else {
			if app.config.SemiSync && node.SemiSyncState != nil && node.SemiSyncState.SlaveEnabled &&
				node.SlaveState != nil && node.SlaveState.ReplicationState == mysql.ReplicationRunning {
				replicasRunning += 1
				if node.DiskState.Usage() >= app.config.CriticalDiskUsage {
					app.logger.Warnf("diskusage: semisync replica %s has critical disk usage %0.2f%%", host, node.DiskState.Usage())
					replicasLow += 1
				} else if node.DiskState.Usage() > app.config.NotCriticalDiskUsage {
					app.logger.Warnf("diskusage: semisync replica %s has grey-zone disk usage %0.2f%%", host, node.DiskState.Usage())
				} else {
					replicasNormal += 1
				}
			}
		}
	}
	if replicasRunning > 0 {
		if masterState.SemiSyncState != nil && replicasLow > replicasRunning-masterState.SemiSyncState.WaitSlaveCount {
			app.logger.Error("diskusage: all semisync replicas have critical disk usage")
			needRo = true
		} else if replicasNormal == 0 {
			app.logger.Warnf("diskusage: there is no semisync replicas with not-critical disk usage")
			mayWrite = false
		}
	}
	if needRo {
		keepSuperWritable := app.config.KeepSuperWritableOnCriticalDiskUsage
		if masterState.IsReadOnly && (keepSuperWritable != masterState.IsSuperReadOnly) {
			app.logger.Infof("diskusage: master is already read-only")
			return
		}
		err := masterNode.SetReadOnlyWithForce(app.config.ExcludeUsers, !keepSuperWritable)
		if err != nil {
			app.logger.Errorf("diskusage: failed to set master read-only: %v", err)
		} else {
			app.logger.Infof("diskusage: master set read-only")
			err = app.dcs.Set(pathLowSpace, true)
			if err != nil {
				app.logger.Errorf("diskusage: failed to set read-only path in dcs: %v", err)
			}
		}
		return
	}
	if mayWrite {
		if !masterState.IsReadOnly {
			return
		}
		err := masterNode.SetWritable()
		if err != nil {
			app.logger.Errorf("diskusage: failed to set master writable: %v", err)
		} else {
			app.logger.Info("diskusage: master set writable")
			err := app.dcs.Set(pathLowSpace, false)
			if err != nil {
				app.logger.Errorf("diskusage: failed to set read-only path in dcs: %v", err)
			}
		}
		return
	}
	app.logger.Warnf("diskusage: cluster in grey-zone, do not change read_only")
}

func (app *App) repairCluster(clusterState, clusterStateDcs map[string]*NodeState, master string) {
	for host, state := range clusterState {
		if !state.PingOk {
			continue
		}
		node := app.cluster.Get(host)
		if host == master {
			app.repairMasterNode(node, clusterState, clusterStateDcs)
		} else {
			app.repairSlaveNode(node, clusterState, master)
		}
	}
}

func (app *App) repairMasterNode(masterNode *mysql.Node, clusterState, clusterStateDcs map[string]*NodeState) {
	host := masterNode.Host()
	masterState := clusterState[host]

	// enter read-only if disk is full
	app.repairReadOnlyOnMaster(masterNode, masterState, clusterStateDcs)

	app.repairExternalReplication(masterNode)

	events, err := masterNode.ReenableEvents()
	if err != nil {
		app.logger.Errorf("repair: failed to reenable slaveside disabled events on %s: %s", host, err)
	}
	if len(events) > 0 {
		app.logger.Infof("repair: events reenabled on %s: %v", host, events)
	}
}

func (app *App) repairSlaveNode(node *mysql.Node, clusterState map[string]*NodeState, master string) {
	host := node.Host()
	state := clusterState[host]
	// node is real slave or stale master here
	// state.SlaveState may be nil
	if !state.IsReadOnly {
		err := node.SetReadOnly(true)
		if err != nil {
			app.logger.Errorf("repair: failed to set host %s read-only: %s", host, err)
		} else {
			app.logger.Infof("repair: slave %s set read-only", host)
		}
	}

	if state.IsMaster {
		// if we found stale master, we should set it offline and resetup
		app.logger.Infof("repair: found stale master %s", host)
		app.logger.Infof("repair: setting stale master %s offline", host)
		err := app.stopReplicationOnMaster(node)
		if err != nil {
			app.logger.Errorf("repair: %s", err)
		}
		err = app.externalReplication.Stop(node)
		if err != nil {
			app.logger.Errorf("repair: %s", err)
		}
		err = app.externalReplication.Reset(node)
		if err != nil {
			app.logger.Errorf("repair: %s", err)
		}
		app.logger.Infof("repair: turning stale master %s to new master %s", host, master)
		err = app.performChangeMaster(host, master)
		if err != nil {
			app.logger.Errorf("repair: error turning stale master %s to new master: %s", host, err)
		}
		app.logger.Infof("repair: mark stale master %s for recovery", host)
		err = app.SetRecovery(host)
		if err != nil {
			app.logger.Errorf("repair: error setting stale master %s for recovery: %s", host, err)
		}
		return
	}

	if state.IsCascade {
		cascadeTopology, err := app.fetchCascadeNodeConfigurations()
		if err != nil {
			app.logger.Warnf("repair: Failed to fetch CascadeNodeConfigurations")
			return
		}
		app.logger.Infof("cascadeTopology=%v", cascadeTopology)
		app.repairCascadeNode(node, clusterState, master, cascadeTopology)
	} else {
		delete(app.streamFromFailedAt, host)
	}

	if !state.IsCascade {
		if state.SlaveState != nil && state.SlaveState.MasterHost != master {
			app.logger.Infof("repair: found stale slave %s, trying to turn it to new replication source %s", host, master)
			err := app.performChangeMaster(host, master)
			if err != nil {
				app.logger.Errorf("repair: %s", err)
			}
		} else if state.SlaveState != nil && state.SlaveState.ReplicationState == mysql.ReplicationStopped {
			// cascade nodes' replication may be stopped during period of changing stream_from host
			err := node.StartSlave()
			if err != nil {
				app.logger.Errorf("repair: failed to start replication on %s: %v", host, err)
			} else {
				app.logger.Infof("repair: replication started on %s", host)
			}
		}
	}

	if state.SlaveState != nil {
		if state.SlaveState.ReplicationState == mysql.ReplicationError {
			if result, code := state.IsReplicationPermanentlyBroken(); result {
				app.logger.Warnf("repair: replication on host %v is permanently broken, error code: %d", host, code)
			} else {
				app.TryRepairReplication(node, master, app.config.ReplicationChannel)
			}
		} else {
			app.MarkReplicationRunning(node, app.config.ReplicationChannel)
		}
	}
}

func (app *App) repairCascadeNode(node *mysql.Node, clusterState map[string]*NodeState, master string, cascadeTopology map[string]mysql.CascadeNodeConfiguration) {
	host := node.Host()
	state := clusterState[host]
	upstreamLostAt := app.streamFromFailedAt[host]
	cnc := cascadeTopology[host]

	if state.SlaveState == nil {
		app.logger.Warnf("repair: current Slave/Replica Status is unknown. Blindly change master on %s to '%s'", host, cnc.StreamFrom)
		err := app.performChangeMaster(host, cnc.StreamFrom)
		if err != nil {
			app.logger.Warnf("repair: failed to change master on host %s to new value %s", host, cnc.StreamFrom)
			return
		}
		err = node.StartSlave()
		if err != nil {
			app.logger.Warnf("repair: failed to start slave %s", host)
			return
		}
		return
	}

	isReplicationRunning := state.SlaveState.ReplicationState == mysql.ReplicationRunning
	app.logger.Infof("repair: checking cascade node %s. Replication=%v, cnc=%+v, slaveSate=%+v", host, isReplicationRunning, cnc, state.SlaveState)

	upstreamMaster := state.SlaveState.MasterHost
	upstreamCandidate := app.findBestStreamFrom(node, clusterState, master, cascadeTopology)
	app.logger.Debugf("repair: best stream_from candidate for %s is %s, current upstream is %s", host, upstreamCandidate, upstreamMaster)

	// scenario - all ok
	if isReplicationRunning && upstreamCandidate == upstreamMaster {
		app.logger.Infof("repair: replication from desired stream_from is running. Do nothing.")
		delete(app.streamFromFailedAt, host)
		return
	}

	if !isReplicationRunning && upstreamCandidate == upstreamMaster {
		app.logger.Warnf("repair: replication from stream_from host `%s` stopped. Trying to start it...", upstreamMaster)
		if result, code := state.IsReplicationPermanentlyBroken(); result {
			app.logger.Warnf("repair: replication on host %v is permanently broken, error code: %d", host, code)
			return
		}
		err := node.StartSlave()
		if err != nil {
			app.logger.Warnf("repair: failed to start slave")
			return
		}
		return
	}

	if !isReplicationRunning && (upstreamLostAt == time.Time{}) {
		app.logger.Warnf("repair: replication from stream_from host `%s` stopped. Schedule switch to new stream_from", upstreamMaster)
		upstreamLostAt = time.Now()
		app.streamFromFailedAt[host] = upstreamLostAt
	}

	if upstreamCandidate != upstreamMaster {
		if isReplicationRunning {
			app.logger.Infof("repair: new stream_from candidate found. Stopping replication.")
			// Stop Slave in order to allow new `stream_from` to catch-up (if needed)
			err := node.StopSlave()
			if err != nil {
				app.logger.Warnf("repair: failed to stop slave")
				return
			}
		}

		// we have chosen candidate... wait till stream_from will have newer GTID than ours:

		// There is a race between GTID sets: order in which it was fetched is not defined.
		// So, fetch cascade replica's ReplicaStatus here
		// As a result, we know that myGITIDs fetched AFTER candidate's GTIDs...
		// We should wait until myGITIDs (fetched later) will be lower or equal to candidateGTIDs (fetched earlier)
		mySlaveStatus, err := node.GetReplicaStatus() // retrieve fresh GTIDs
		if err != nil {
			app.logger.Warnf("repair: cannot obtain own SLAVE/REPLICA STATUS")
			return
		}
		myGITIDs := gtids.ParseGtidSet(mySlaveStatus.GetExecutedGtidSet())

		candidateState := clusterState[upstreamCandidate]
		var candidateGTIDs gtids.GTIDSet
		if candidateState.IsMaster {
			candidateGTIDs = gtids.ParseGtidSet(candidateState.MasterState.ExecutedGtidSet)
		} else {
			candidateGTIDs = gtids.ParseGtidSet(candidateState.SlaveState.ExecutedGtidSet)
		}
		app.logger.Debugf("repair: %s GTID set = %v, new stream_from GTID set is %v", host, myGITIDs, candidateGTIDs)

		if !isGTIDLessOrEqual(myGITIDs, candidateGTIDs) && !isGTIDLessOrEqual(candidateGTIDs, myGITIDs) {
			app.logger.Errorf("repair: %s and %s are splitbrained...", host, upstreamCandidate)
			app.writeEmergeFile("cascade replica splitbain detected")
			return
		}
		if isGTIDLessOrEqual(myGITIDs, candidateGTIDs) {
			app.logger.Infof("repair: new stream_from host GTID set is superset of our GTID set. Switching Master_Host, Starting replication")
			err = app.performChangeMaster(host, upstreamCandidate)
			if err != nil {
				app.logger.Warnf("repair: failed to change master on host %s to new value %s", host, upstreamCandidate)
				return
			}
			err = node.StartSlave()
			if err != nil {
				app.logger.Warnf("repair: failed to start slave %s", host)
				return
			}
			app.logger.Infof("repair: Success. New stream_from host is: %s", upstreamCandidate)
		}
	}
}

func (app *App) repairExternalReplication(masterNode *mysql.Node) {
	extReplStatus, err := app.externalReplication.GetReplicaStatus(masterNode)
	if err != nil {
		if !mysql.IsErrorChannelDoesNotExists(err) {
			app.logger.Errorf("repair (external): host %s failed to get external replica status %v", masterNode.Host(), err)
		}
		return
	}
	if extReplStatus == nil {
		// external replication is not supported
		return
	}

	if app.externalReplication.IsRunningByUser(masterNode) && !extReplStatus.ReplicationRunning() {
		// TODO: remove "". Master is not needed for external replication now
		app.TryRepairReplication(masterNode, "", app.config.ExternalReplicationChannel)
	}
}

func (app *App) findBestStreamFrom(node *mysql.Node, clusterState map[string]*NodeState, master string, cascadeTopology map[string]mysql.CascadeNodeConfiguration) string {
	var loopDetector []string
	loopDetector = append(loopDetector, node.Host())

	for {
		host := loopDetector[len(loopDetector)-1]
		streamFrom := cascadeTopology[host].StreamFrom
		if streamFrom == "" {
			return master
		}
		if util.ContainsString(loopDetector, streamFrom) {
			loopDetector = append(loopDetector, streamFrom)
			app.logger.Errorf("repair: found loop in stream_from references. Fallback to master. Loop: %s", strings.Join(loopDetector, " -> "))
			return master
		}

		candidateState := clusterState[streamFrom]

		// if cascade node is streaming now from configured host - do nothing
		if len(loopDetector) == 1 {
			hostSlaveState := clusterState[node.Host()].SlaveState
			if hostSlaveState != nil &&
				hostSlaveState.ReplicationState == mysql.ReplicationRunning &&
				hostSlaveState.MasterHost == streamFrom {
				return streamFrom
			}
		}

		hasReasonableLag := candidateState.IsMaster || (candidateState.SlaveState != nil &&
			candidateState.SlaveState.ReplicationState == mysql.ReplicationRunning &&
			candidateState.SlaveState.ReplicationLag != nil &&
			*candidateState.SlaveState.ReplicationLag < app.config.StreamFromReasonableLag.Seconds())

		if candidateState.PingOk && !candidateState.IsOffline && hasReasonableLag {
			return streamFrom // first suitable cascadeNodeState is Ok
		}

		loopDetector = append(loopDetector, streamFrom)
	}
}

func (app *App) enterMaintenance(maintenance *Maintenance, master string) error {
	if app.config.DisableSemiSyncReplicationOnMaintenance {
		node := app.cluster.Get(master)
		err := node.SemiSyncDisable()
		if err != nil {
			return err
		}
		err = app.dcs.Delete(pathActiveNodes)
		if err != nil {
			return err
		}
	}
	maintenance.MySyncPaused = true
	return app.dcs.Set(pathMaintenance, maintenance)
}

func (app *App) leaveMaintenance() error {
	err := app.cluster.UpdateHostsInfo()
	if err != nil {
		return err
	}
	clusterState := app.getClusterStateFromDB()
	master, err := app.ensureCurrentMaster(clusterState)
	if err != nil {
		if errors.Is(err, ErrManyMasters) {
			app.writeEmergeFile(err.Error())
		}
		return err
	}
	clusterStateDcs, err := app.getClusterStateFromDcs()
	if err != nil {
		return err
	}
	app.repairCluster(clusterState, clusterStateDcs, master)
	clusterState = app.getClusterStateFromDB()
	err = app.updateActiveNodes(clusterState, clusterStateDcs, []string{}, master)
	if err != nil {
		return err
	}
	activeNodes, err := app.GetActiveNodes()
	if err != nil {
		return err
	}
	if len(activeNodes) == 0 {
		return ErrNoActiveNodes
	}
	return app.dcs.Delete(pathMaintenance)
}

func (app *App) performChangeMaster(host, master string) error {
	// Do we need to change master status here
	if host == master {
		panic(fmt.Sprintf("impossible to change master to itself: %s", host))
	}
	node := app.cluster.Get(host)
	err := node.StopSlave()
	if err != nil {
		return fmt.Errorf("failed to stop slave on host %s: %s", host, err)
	}
	app.logger.Infof("changemaster: host %s replication stopped", host)

	err = node.ChangeMaster(master)
	if err != nil {
		return fmt.Errorf("failed to change master on host %s: %s", host, err)
	}
	app.logger.Infof("changemaster: host %s turned to the new master %s", host, master)

	err = node.StartSlave()
	if err != nil {
		return fmt.Errorf("failed to start slave on host %s: %s", host, err)
	}

	deadline := time.Now().Add(app.config.WaitReplicationStartTimeout)
	var sstatus mysql.ReplicaStatus
	for time.Now().Before(deadline) {
		sstatus, err = node.GetReplicaStatus()
		if err != nil {
			app.logger.Warnf("changemaster: failed to get slave status on host %s: %v", host, err)
			continue
		}
		if sstatus.ReplicationRunning() {
			break
		}
		app.logger.Warnf("changemaster: replication on host %s is not running yet, waiting...", host)
		time.Sleep(time.Second)
	}
	if sstatus != nil && sstatus.ReplicationRunning() {
		app.logger.Infof("changemaster: host %s replication started", host)
	} else {
		app.logger.Warnf("changemaster: host %s replication failed to start", host)
	}
	return nil
}

func (app *App) getNodeState(host string) *NodeState {
	var node *mysql.Node
	if app.cluster.Local().Host() == host {
		node = app.cluster.Local()
	} else {
		node = app.cluster.Get(host)
	}
	nodeState := new(NodeState)
	err := func() error {
		nodeState.CheckAt = time.Now()
		nodeState.CheckBy = app.config.Hostname
		pingOk, err := node.Ping()
		nodeState.PingOk = pingOk
		if err != nil {
			nodeState.PingDubious = mysql.IsErrorDubious(err)
			return err
		}
		if !pingOk {
			return fmt.Errorf("ping is no ok")
		}
		nodeState.IsReadOnly, nodeState.IsSuperReadOnly, err = node.IsReadOnly()
		if err != nil {
			return err
		}
		nodeState.IsOffline, err = node.IsOffline()
		if err != nil {
			return err
		}
		slaveStatus, err := node.GetReplicaStatus()
		if err != nil {
			return err
		}
		if slaveStatus != nil {
			nodeState.IsMaster = false
			nodeState.SlaveState = new(SlaveState)
			nodeState.SlaveState.FromReplicaStatus(slaveStatus)
			lag, err2 := node.ReplicationLag(slaveStatus)
			if err2 != nil {
				return err2
			}
			nodeState.SlaveState.ReplicationLag = lag
		} else {
			nodeState.IsMaster = true
			nodeState.MasterState = new(MasterState)
			gtidExecuted, err2 := node.GTIDExecuted()
			if err2 != nil {
				return err2
			}
			nodeState.MasterState.ExecutedGtidSet = gtidExecuted.ExecutedGtidSet
		}
		semiSyncStatus, err := node.SemiSyncStatus()
		if err != nil {
			return err
		}
		nodeState.SemiSyncState = new(SemiSyncState)
		nodeState.SemiSyncState.MasterEnabled = semiSyncStatus.MasterEnabled > 0
		nodeState.SemiSyncState.SlaveEnabled = semiSyncStatus.SlaveEnabled > 0
		nodeState.SemiSyncState.WaitSlaveCount = semiSyncStatus.WaitSlaveCount
		return nil
	}()
	if err != nil {
		app.logger.Errorf("node %s: error during getNodeState: %v", node.Host(), err)
		nodeState.Error = err.Error()
		// in case ping was ok but later we encountered error
		if nodeState.PingOk {
			if nodeState.PingOk, err = node.Ping(); err != nil {
				nodeState.PingDubious = mysql.IsErrorDubious(err)
				nodeState.Error = err.Error()
			}
		}
	}

	// get cached value:
	nodeState.IsCascade = app.cluster.IsCascadeHost(host)

	return nodeState
}

func (app *App) getLocalNodeState() *NodeState {
	node := app.cluster.Local()
	nodeState := app.getNodeState(node.Host())

	diskUsed, diskTotal, err := node.GetDiskUsage()
	if err == nil {
		nodeState.DiskState = new(DiskState)
		nodeState.DiskState.Total = diskTotal
		nodeState.DiskState.Used = diskUsed
	} else {
		app.logger.Errorf("Failed to get disk usage: %v", err)
	}
	daemonState, err := app.getLocalDaemonState()
	if err == nil {
		nodeState.DaemonState = daemonState
	} else {
		app.logger.Errorf("Failed to get daemon state: %v", err)
	}
	isFSReadonly, err := node.IsFileSystemReadonly()
	if err == nil {
		nodeState.IsFileSystemReadonly = isFSReadonly
	} else {
		app.logger.Errorf("Failed to check file system on readonly: %v", err)
	}

	return nodeState
}

func (app *App) getLocalDaemonState() (*DaemonState, error) {
	app.daemonMutex.Lock()
	defer app.daemonMutex.Unlock()
	node := app.cluster.Local()
	startTime, err := node.GetDaemonStartTime()
	if err != nil {
		return nil, err
	}
	if app.daemonState != nil && app.daemonState.StartTime.Equal(startTime) {
		return app.daemonState, nil
	}
	recoveryTime, err := node.GetCrashRecoveryTime()
	if err != nil {
		return nil, err
	}
	ds := new(DaemonState)
	// startTime is multiple of Second.
	// In docker test recoveryTime often less than startTime by 200-300ms
	// So we just round recoveryTime up to second to prevent flaps
	// In real life recoveryTime is
	//  * either greater than startTime by few seconds
	//  * or less than startTime by days (if recovery was during previous mysql start)
	recoveryTime = recoveryTime.Truncate(time.Second).Add(time.Second)
	ds.RecoveryTime = recoveryTime
	ds.StartTime = startTime
	ds.CrashRecovery = (recoveryTime.After(startTime) || recoveryTime.Equal(startTime)) && !recoveryTime.IsZero()
	app.daemonState = ds
	return ds, nil
}

func (app *App) getClusterStateFromDB() map[string]*NodeState {
	hosts := app.cluster.AllNodeHosts()
	getter := func(host string) (*NodeState, error) {
		return app.getNodeState(host), nil
	}
	clusterState, _ := getNodeStatesInParallel(hosts, getter)
	return clusterState
}

func (app *App) getClusterStateFromDcs() (map[string]*NodeState, error) {
	hosts := app.cluster.AllNodeHosts()
	getter := func(host string) (*NodeState, error) {
		nodeState := new(NodeState)
		err := app.dcs.Get(dcs.JoinPath(pathHealthPrefix, host), nodeState)
		if err != nil && err != dcs.ErrNotFound {
			return nil, err
		}
		return nodeState, nil
	}
	return getNodeStatesInParallel(hosts, getter)
}

func (app *App) waitForCatchUp(node *mysql.Node, gtidset gtids.GTIDSet, timeout time.Duration, sleep time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		gtidExecuted, err := node.GTIDExecutedParsed()
		if err != nil {
			return false, err
		}
		app.logger.Infof("catch up: node %s has gtid %s, waiting for %s", node.Host(), gtidExecuted.String(), gtidset.String())
		if gtidExecuted.Contain(gtidset) {
			return true, nil
		}
		switchover := new(Switchover)
		if app.dcs.Get(pathCurrentSwitch, switchover) == dcs.ErrNotFound {
			return false, nil
		}
		if app.CheckAsyncSwitchAllowed(node, switchover) {
			return true, nil
		}
		time.Sleep(sleep)
		if time.Now().After(deadline) {
			break
		}
	}
	return false, nil
}

// Set master offline and disable semi-sync replication
func (app *App) stopReplicationOnMaster(masterNode *mysql.Node) error {
	host := masterNode.Host()
	app.logger.Infof("setting master %s offline", host)
	err := masterNode.SetOffline()
	if err != nil {
		return fmt.Errorf("cannot set master %s offline: %s", host, err)
	}
	app.logger.Infof("disable semi-sync on master %s", host)
	err = masterNode.SemiSyncDisable()
	if err != nil {
		return fmt.Errorf("failed to disable semi-sync on master %s: %v", host, err)
	}

	return nil
}

func (app *App) fetchCascadeNodeConfigurations() (map[string]mysql.CascadeNodeConfiguration, error) {
	cascadeTopology := make(map[string]mysql.CascadeNodeConfiguration)

	hosts, err := app.dcs.GetChildren(dcs.PathCascadeNodesPrefix)
	if err != nil {
		app.logger.Warnf("repair: Failed to fetch CascadeNodeConfigurations")
		return cascadeTopology, err
	}
	for _, host := range hosts {
		var cnc mysql.CascadeNodeConfiguration
		err := app.dcs.Get(dcs.JoinPath(dcs.PathCascadeNodesPrefix, host), &cnc)
		if err != nil {
			app.logger.Warnf("repair: Failed to fetch CascadeNodeConfiguration for %s", host)
			return cascadeTopology, err
		}
		cascadeTopology[host] = cnc
	}

	return cascadeTopology, nil
}

func (app *App) getNodePositions(activeNodes []string) ([]nodePosition, error) {
	var positions []nodePosition
	var positionsMutex sync.Mutex
	errs := util.RunParallel(func(host string) error {
		node := app.cluster.Get(host)
		sstatus, err := node.GetReplicaStatus()
		if err != nil || app.emulateError("freeze_slave_status") {
			return fmt.Errorf("failed to get slave status on host %s: %s", host, err)
		}
		lag, err := node.ReplicationLag(sstatus)
		if err != nil || app.emulateError("freeze_slave_status2") {
			return fmt.Errorf("failed to get slave replication lag on host %s: %s", host, err)
		}
		if lag == nil {
			// we can treat unknown lag as infinite for the replica compare purpose
			lag = new(float64)
			// 60*60*24*365 = 31 536 000 is a year in seconds. We will use 99 999 999 as maxLag
			*lag = 99999999
		}

		var gtidset gtids.GTIDSet
		if sstatus == nil {
			app.logger.Infof("switchover: current master found: %s", host)
			gtidset, err = node.GTIDExecutedParsed()
			if err != nil || app.emulateError("freeze_master_status") {
				return fmt.Errorf("failed to get master status on host %s: %s", host, err)
			}
		} else {
			gtidset = gtids.ParseGtidSet(sstatus.GetExecutedGtidSet())
			if sstatus.GetRetrievedGtidSet() != "" {
				// slave may have downloaded but not applied transactions
				err := gtidset.Update(sstatus.GetRetrievedGtidSet())
				if err != nil {
					return fmt.Errorf("failed to parse RetrievedGtidSet from host %s: %s", host, err)
				}
			}
		}

		var nc mysql.NodeConfiguration
		err = app.dcs.Get(dcs.JoinPath(pathHANodes, host), &nc)
		if err != nil {
			if err != dcs.ErrNotFound && err != dcs.ErrMalformed {
				return fmt.Errorf("failed to get priority for host %s: %s", host, err)
			}
			// Default value
			nc = mysql.NodeConfiguration{Priority: 0}
		}

		positionsMutex.Lock()
		positions = append(positions, nodePosition{host, gtidset, *lag, nc.Priority})
		positionsMutex.Unlock()
		return nil
	}, activeNodes)

	return positions, util.CombineErrors(errs)
}

/*
Run enters the main application loop
When Run exits mysync process is over
*/
func (app *App) Run() int {
	ctx := app.baseContext()

	err := app.lockFile()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.unlockFile()

	app.logger.Infof("MYSYNC START")
	app.logger.Infof("config failover: %v semisync: %v", app.config.Failover, app.config.SemiSync)

	err = app.connectDCS()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.dcs.Close()

	err = app.newDBCluster()
	if err != nil {
		app.logger.Errorf(err.Error())
		return 1
	}
	defer app.cluster.Close()

	go app.healthChecker(ctx)
	go app.recoveryChecker(ctx)
	go app.stateFileHandler(ctx)
	if app.config.ExternalReplicationType != util.Disabled {
		go app.externalCAFileChecker(ctx)
	}
	if app.config.ReplMon {
		go app.replMonWriter(ctx)
	}

	handlers := map[appState](func() appState){
		stateFirstRun:    app.stateFirstRun,
		stateManager:     app.stateManager,
		stateCandidate:   app.stateCandidate,
		stateLost:        app.stateLost,
		stateMaintenance: app.stateMaintenance,
	}

	ticker := time.NewTicker(app.config.TickInterval)
	for {
		select {
		case <-ticker.C:
			// run states without sleep while app.state changes
			for {
				app.logger.Infof("mysync state: %s", app.state)
				stateHandler := handlers[app.state]
				if stateHandler == nil {
					panic(fmt.Sprintf("unknown state: %s", app.state))
				}
				nextState := stateHandler()
				if nextState == app.state {
					break
				}
				// TODO: update state file ?
				app.state = nextState
			}
		case <-ctx.Done():
			return 0
		}
	}
}
