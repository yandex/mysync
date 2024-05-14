package mysql

const (
	queryPing                           = "ping"
	querySlaveStatus                    = "slave_status"
	queryReplicaStatus                  = "replica_status"
	queryGetVersion                     = "get_version"
	queryGTIDExecuted                   = "gtid_executed"
	queryGetUUID                        = "get_uuid"
	queryShowBinaryLogs                 = "binary_logs"
	queryReplicationLag                 = "replication_lag"
	querySlaveHosts                     = "slave_hosts"
	queryIsReadOnly                     = "is_readonly"
	querySetReadonly                    = "set_readonly"
	querySetReadonlyNoSuper             = "set_readonly_no_super"
	querySetWritable                    = "set_writable"
	queryStopSlave                      = "stop_slave"
	queryStartSlave                     = "start_slave"
	queryStopSlaveIOThread              = "stop_slave_io_thread"
	queryStartSlaveIOThread             = "start_slave_io_thread"
	queryStopSlaveSQLThread             = "stop_slave_sql_thread"
	queryStartSlaveSQLThread            = "start_slave_sql_thread"
	queryResetSlaveAll                  = "reset_slave_all"
	queryChangeMaster                   = "change_master"
	querySemiSyncStatus                 = "semisync_status"
	querySemiSyncSetMaster              = "semisync_set_master"
	querySemiSyncSetSlave               = "semisync_set_slave"
	querySemiSyncDisable                = "semisync_disable"
	querySetSemiSyncWaitSlaveCount      = "set_semisync_wait_slave_count"
	queryListSlavesideDisabledEvents    = "list_slaveside_disabled_events"
	queryEnableEvent                    = "enable_event"
	querySetLockTimeout                 = "set_lock_timeout"
	queryKillQuery                      = "kill_query"
	queryGetProcessIDs                  = "get_process_ids"
	queryEnableOfflineMode              = "enable_offline_mode"
	queryDisableOfflineMode             = "disable_offline_mode"
	queryGetOfflineMode                 = "get_offline_mode"
	queryHasWaitingSemiSyncAck          = "has_waiting_semi_sync_ack"
	queryGetLastStartupTime             = "get_last_startup_time"
	queryGetExternalReplicationSettings = "get_external_replication_settings"
	queryChangeSource                   = "change_source"
	queryResetReplicaAll                = "reset_replica_all"
	queryStopReplica                    = "stop_replica"
	queryStartReplica                   = "start_replica"
	queryIgnoreDB                       = "ignore_db"
	querySetInnodbFlushLogAtTrxCommit   = "set_innodb_flush_log_at_trx_commit"
	querySetSyncBinlog                  = "set_sync_binlog"
	queryGetReplicationSettings         = "get_replication_settings"
	queryGetMdbReplMonTS                = "get_mdb_repl_mon_ts"
	queryCalcMdbReplMonTSDelay          = "calc_mdb_repl_mon_ts_delay"
)

var DefaultQueries = map[string]string{
	queryPing:                `SELECT 1 AS Ok`,
	querySlaveStatus:         `SHOW SLAVE STATUS FOR CHANNEL :channel`,
	queryReplicaStatus:       `SHOW REPLICA STATUS FOR CHANNEL :channel`,
	queryGetVersion:          `SELECT sys.version_major() AS MajorVersion, sys.version_minor() AS MinorVersion, sys.version_patch() AS PatchVersion`,
	queryGTIDExecuted:        `SELECT @@GLOBAL.gtid_executed  as Executed_Gtid_Set`,
	queryGetUUID:             `SELECT @@server_uuid as server_uuid`,
	queryShowBinaryLogs:      `SHOW BINARY LOGS`,
	querySlaveHosts:          `SHOW SLAVE HOSTS`,
	queryReplicationLag:      ``,
	queryIsReadOnly:          `SELECT @@read_only AS ReadOnly, @@super_read_only AS SuperReadOnly`,
	querySetReadonly:         `SET GLOBAL super_read_only = 1`, // @@read_only will be set automatically
	querySetReadonlyNoSuper:  `SET GLOBAL read_only = 1, super_read_only = 0`,
	querySetWritable:         `SET GLOBAL read_only = 0`, // @@super_read_only will be unset automatically
	queryStopSlave:           `STOP SLAVE FOR CHANNEL :channel`,
	queryStartSlave:          `START SLAVE FOR CHANNEL :channel`,
	queryStopReplica:         `STOP REPLICA FOR CHANNEL :channel`,
	queryStartReplica:        `START REPLICA FOR CHANNEL :channel`,
	queryStopSlaveIOThread:   `STOP SLAVE IO_THREAD FOR CHANNEL :channel`,
	queryStartSlaveIOThread:  `START SLAVE IO_THREAD FOR CHANNEL :channel`,
	queryStopSlaveSQLThread:  `STOP SLAVE SQL_THREAD FOR CHANNEL :channel`,
	queryStartSlaveSQLThread: `START SLAVE SQL_THREAD FOR CHANNEL :channel`,
	queryResetSlaveAll:       `RESET SLAVE ALL FOR CHANNEL :channel`,
	queryResetReplicaAll:     `RESET REPLICA ALL FOR CHANNEL :channel`,
	queryChangeMaster: `CHANGE MASTER TO
								MASTER_HOST = :host ,
								MASTER_PORT = :port ,
								MASTER_USER = :user ,
								MASTER_PASSWORD = :password ,
								MASTER_SSL = :ssl ,
								MASTER_SSL_CA = :sslCa ,
								MASTER_SSL_VERIFY_SERVER_CERT = 1,
								MASTER_AUTO_POSITION = 1,
								MASTER_CONNECT_RETRY = :connectRetry,
								MASTER_RETRY_COUNT = :retryCount,
								MASTER_HEARTBEAT_PERIOD = :heartbeatPeriod
						FOR CHANNEL :channel`,
	querySemiSyncStatus: `SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled,
								 @@rpl_semi_sync_slave_enabled AS SlaveEnabled,
								 @@rpl_semi_sync_master_wait_for_slave_count as WaitSlaveCount`,
	querySemiSyncSetMaster:         `SET GLOBAL rpl_semi_sync_master_enabled = 1, rpl_semi_sync_slave_enabled = 0`,
	querySemiSyncSetSlave:          `SET GLOBAL rpl_semi_sync_slave_enabled = 1, rpl_semi_sync_master_enabled = 0`,
	querySemiSyncDisable:           `SET GLOBAL rpl_semi_sync_slave_enabled = 0, rpl_semi_sync_master_enabled = 0`,
	querySetSemiSyncWaitSlaveCount: `SET GLOBAL rpl_semi_sync_master_wait_for_slave_count = :wait_slave_count`,
	queryListSlavesideDisabledEvents: `SELECT EVENT_SCHEMA, EVENT_NAME, DEFINER
										FROM information_schema.EVENTS
										WHERE STATUS = 'SLAVESIDE_DISABLED'`,

	queryEnableEvent:           `ALTER DEFINER = :user@:host EVENT :schema.:name ENABLE`,
	querySetLockTimeout:        `SET SESSION lock_wait_timeout = ?`,
	queryKillQuery:             `KILL :kill_id`,
	queryGetProcessIDs:         `SELECT ID FROM information_schema.PROCESSLIST p WHERE USER NOT IN (?) AND COMMAND != 'Killed'`,
	queryEnableOfflineMode:     `SET GLOBAL offline_mode = ON`,
	queryDisableOfflineMode:    `SET GLOBAL offline_mode = OFF`,
	queryGetOfflineMode:        `SELECT @@GLOBAL.offline_mode AS OfflineMode`,
	queryHasWaitingSemiSyncAck: `SELECT count(*) <> 0 AS IsWaiting FROM information_schema.PROCESSLIST WHERE state = 'Waiting for semi-sync ACK from slave'`,
	queryGetLastStartupTime:    `SELECT UNIX_TIMESTAMP(DATE_SUB(now(), INTERVAL variable_value SECOND)) AS LastStartup FROM performance_schema.global_status WHERE variable_name='Uptime'`,
	queryGetExternalReplicationSettings: `SELECT channel_name AS ChannelName, source_host AS SourceHost, source_user AS SourceUser, source_port AS SourcePort,
											source_password AS SourcePassword, source_ssl_ca AS SourceSslCa, source_delay AS SourceDelay, replication_status AS ReplicationStatus
											FROM mysql.replication_settings WHERE channel_name = 'external'`,
	queryChangeSource: `CHANGE REPLICATION SOURCE TO
								SOURCE_HOST = :host,
								SOURCE_PORT = :port,
								SOURCE_USER = :user,
								SOURCE_PASSWORD = :password,
								SOURCE_SSL = :ssl,
								SOURCE_SSL_CA = :sslCa,
								SOURCE_SSL_VERIFY_SERVER_CERT = 1,
								SOURCE_AUTO_POSITION = 1,
								SOURCE_CONNECT_RETRY = :connectRetry,
								SOURCE_RETRY_COUNT = :retryCount,
								SOURCE_DELAY = :sourceDelay
						FOR CHANNEL :channel`,
	queryIgnoreDB:                     `CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = (:ignoreList) FOR CHANNEl :channel`,
	querySetInnodbFlushLogAtTrxCommit: `SET GLOBAL innodb_flush_log_at_trx_commit = :level`,
	queryGetReplicationSettings:       `SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog`,
	querySetSyncBinlog:                `SET GLOBAL sync_binlog = :sync_binlog`,
	queryGetMdbReplMonTS:              `SELECT UNIX_TIMESTAMP(ts) AS ts FROM mysql.mdb_repl_mon`,
	queryCalcMdbReplMonTSDelay:        `SELECT FLOOR(CAST(:ts AS DECIMAL(20,3)) - UNIX_TIMESTAMP(ts)) AS delay FROM mysql.mdb_repl_mon`,
}
