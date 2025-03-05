package config

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/heetch/confita"
	"github.com/heetch/confita/backend/file"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/util"
)

// MySQLConfig contains MySQL cluster connection info
type MySQLConfig struct {
	ReplicationPassword        string `config:"replication_password,required" yaml:"replication_password"`
	ExternalReplicationSslCA   string `config:"external_replication_ssl_ca" yaml:"external_replication_ssl_ca"`
	ErrorLog                   string `config:"error_log" yaml:"error_log"`
	SslCA                      string `config:"ssl_ca" yaml:"ssl_ca"`
	ReplicationUser            string `config:"replication_user,required" yaml:"replication_user"`
	PidFile                    string `config:"pid_file" yaml:"pid_file"`
	Password                   string `config:"password,required"`
	ReplicationSslCA           string `config:"replication_ssl_ca" yaml:"replication_ssl_ca"`
	User                       string `config:"user,required"`
	DataDir                    string `config:"data_dir" yaml:"data_dir"`
	ReplicationRetryCount      int    `config:"replication_retry_count" yaml:"replication_retry_count"`
	ReplicationHeartbeatPeriod int    `config:"replication_heartbeat_period" yaml:"replication_heartbeat_period"`
	ReplicationConnectRetry    int    `config:"replication_connect_retry" yaml:"replication_connect_retry"`
	ReplicationPort            int    `config:"replication_port" yaml:"replication_port"`
	Port                       int    `config:"port" yaml:"port"`
}

// Config contains all mysync configuration
type Config struct {
	Commands                                map[string]string            `config:"commands"`
	Queries                                 map[string]string            `config:"queries"`
	MySQL                                   MySQLConfig                  `config:"mysql"`
	Log                                     string                       `config:"log"`
	Lockfile                                string                       `config:"lockfile"`
	ExternalReplicationChannel              string                       `config:"external_replication_channel" yaml:"external_replication_channel"`
	ReplicationChannel                      string                       `config:"replication_channel" yaml:"replication_channel"`
	TestFilesystemReadonlyFile              string                       `config:"test_filesystem_readonly_file" yaml:"test_filesystem_readonly_file"`
	TestDiskUsageFile                       string                       `config:"test_disk_usage_file" yaml:"test_disk_usage_file"`
	LogLevel                                string                       `config:"loglevel"`
	DSNSettings                             string                       `config:"dsn_settings" yaml:"dsn_settings"`
	Hostname                                string                       `config:"hostname"`
	ExternalReplicationType                 util.ExternalReplicationType `config:"external_replication_type" yaml:"external_replication_type"`
	InfoFile                                string                       `config:"info_file" yaml:"info_file"`
	Emergefile                              string                       `config:"emergefile"`
	Resetupfile                             string                       `config:"resetupfile"`
	Maintenancefile                         string                       `config:"maintenancefile"`
	ReplMonSchemeName                       string                       `config:"repl_mon_scheme_name" yaml:"repl_mon_scheme_name"`
	ReplMonTableName                        string                       `config:"repl_mon_table_name" yaml:"repl_mon_table_name"`
	ExcludeUsers                            []string                     `config:"exclude_users" yaml:"exclude_users"`
	Zookeeper                               dcs.ZookeeperConfig          `config:"zookeeper"`
	MaxAcceptableLag                        float64                      `config:"max_acceptable_lag" yaml:"max_acceptable_lag"`
	StreamFromReasonableLag                 time.Duration                `config:"stream_from_reasonable_lag" yaml:"stream_from_reasonable_lag"`
	DBLostCheckTimeout                      time.Duration                `config:"db_lost_check_timeout" yaml:"db_lost_check_timeout"`
	DBSetRoTimeout                          time.Duration                `config:"db_set_ro_timeout" yaml:"db_set_ro_timeout"`
	DBSetRoForceTimeout                     time.Duration                `config:"db_set_ro_force_timeout" yaml:"db_set_ro_force_timeout"`
	DBStopSlaveSQLThreadTimeout             time.Duration                `config:"db_stop_slave_sql_thread_timeout" yaml:"db_stop_slave_sql_thread_timeout"`
	TickInterval                            time.Duration                `config:"tick_interval" yaml:"tick_interval"`
	HealthCheckInterval                     time.Duration                `config:"healthcheck_interval" yaml:"healthcheck_interval"`
	InfoFileHandlerInterval                 time.Duration                `config:"info_file_handler_interval" yaml:"info_file_handler_interval"`
	RecoveryCheckInterval                   time.Duration                `config:"recoverycheck_interval" yaml:"recoverycheck_interval"`
	ExternalCAFileCheckInterval             time.Duration                `config:"external_ca_file_check_interval" yaml:"external_ca_file_check_interval"`
	ManagerElectionDelayAfterQuorumLoss     time.Duration                `config:"manager_election_delay_after_quorum_loss" yaml:"manager_election_delay_after_quorum_loss"`
	ManagerLockAcquireDelayAfterQuorumLoss  time.Duration                `config:"manager_lock_acquire_delay_after_quorum_loss" yaml:"manager_lock_acquire_delay_after_quorum_loss"`
	ReplMonSlaveWaitInterval                time.Duration                `config:"repl_mon_slave_wait_interval" yaml:"repl_mon_slave_wait_interval"`
	SlaveCatchUpTimeout                     time.Duration                `config:"slave_catch_up_timeout" yaml:"slave_catch_up_timeout"`
	ReplMonErrorWaitInterval                time.Duration                `config:"repl_mon_error_wait_interval" yaml:"repl_mon_error_wait_interval"`
	ReplMonWriteInterval                    time.Duration                `config:"repl_mon_write_interval" yaml:"repl_mon_write_interval"`
	DcsWaitTimeout                          time.Duration                `config:"dcs_wait_timeout" yaml:"dcs_wait_timeout"`
	OfflineModeEnableInterval               time.Duration                `config:"offline_mode_enable_interval" yaml:"offline_mode_enable_interval"`
	OfflineModeEnableLag                    time.Duration                `config:"offline_mode_enable_lag" yaml:"offline_mode_enable_lag"`
	OfflineModeDisableLag                   time.Duration                `config:"offline_mode_disable_lag" yaml:"offline_mode_disable_lag"`
	SemiSyncEnableLag                       int64                        `config:"semi_sync_enable_lag" yaml:"semi_sync_enable_lag"`
	AsyncAllowedLag                         time.Duration                `config:"async_allowed_lag" yaml:"async_allowed_lag"`
	DBTimeout                               time.Duration                `config:"db_timeout" yaml:"db_timeout"`
	PriorityChoiceMaxLag                    time.Duration                `config:"priority_choice_max_lag" yaml:"priority_choice_max_lag"`
	NotCriticalDiskUsage                    float64                      `config:"not_critical_disk_usage" yaml:"not_critical_disk_usage"`
	RplSemiSyncMasterWaitForSlaveCount      int                          `config:"rpl_semi_sync_master_wait_for_slave_count" yaml:"rpl_semi_sync_master_wait_for_slave_count"`
	WaitReplicationStartTimeout             time.Duration                `config:"wait_start_replication_timeout" yaml:"wait_start_replication_timeout"`
	FailoverCooldown                        time.Duration                `config:"failover_cooldown" yaml:"failover_cooldown"`
	ReplicationRepairCooldown               time.Duration                `config:"replication_repair_cooldown" yaml:"replication_repair_cooldown"`
	ReplicationRepairMaxAttempts            int                          `config:"replication_repair_max_attempts" yaml:"replication_repair_max_attempts"`
	CriticalDiskUsage                       float64                      `config:"critical_disk_usage" yaml:"critical_disk_usage"`
	InactivationDelay                       time.Duration                `config:"inactivation_delay" yaml:"inactivation_delay"`
	FailoverDelay                           time.Duration                `config:"failover_delay" yaml:"failover_delay"`
	KeepSuperWritableOnCriticalDiskUsage    bool                         `config:"keep_super_writable_on_critical_disk_usage" yaml:"keep_super_writable_on_critical_disk_usage"`
	ASync                                   bool                         `config:"async" yaml:"async"`
	ResetupCrashedHosts                     bool                         `config:"resetup_crashed_hosts" yaml:"resetup_crashed_hosts"`
	ReplMon                                 bool                         `config:"repl_mon" yaml:"repl_mon"`
	Failover                                bool                         `config:"failover" yaml:"failover"`
	DisableSetReadonlyOnLost                bool                         `config:"disable_set_readonly_on_lost" yaml:"disable_set_readonly_on_lost"`
	ReplicationRepairAggressiveMode         bool                         `config:"replication_repair_aggressive_mode" yaml:"replication_repair_aggressive_mode"`
	DisableSemiSyncReplicationOnMaintenance bool                         `config:"disable_semi_sync_replication_on_maintenance" yaml:"disable_semi_sync_replication_on_maintenance"`
	DevMode                                 bool                         `config:"dev_mode" yaml:"dev_mode"`
	ShowOnlyGTIDDiff                        bool                         `config:"show_only_gtid_diff" yaml:"show_only_gtid_diff"`
	ManagerSwitchover                       bool                         `config:"manager_switchover" yaml:"manager_switchover"`
	ForceSwitchover                         bool                         `config:"force_switchover" yaml:"force_switchover"`
	SemiSync                                bool                         `config:"semi_sync" yaml:"semi_sync"`
}

// DefaultConfig returns default configuration for MySync
func DefaultConfig() (Config, error) {
	zkConfig, err := dcs.DefaultZookeeperConfig()
	if err != nil {
		return Config{}, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return Config{}, err
	}
	config := Config{
		DevMode:           false,
		SemiSync:          false,
		SemiSyncEnableLag: 100 * 1024 * 1024, // 100Mb
		Failover:          false,
		FailoverCooldown:  time.Hour,
		FailoverDelay:     30 * time.Second,
		InactivationDelay: 30 * time.Second,
		CriticalDiskUsage: 95.0,
		LogLevel:          "Info",
		Log:               "/var/log/mysync/mysync.log",
		Lockfile:          "/var/run/mysync/mysync.lock",
		InfoFile:          "/var/run/mysync/mysync.info",
		Emergefile:        "/var/run/mysync/mysync.emerge",
		Resetupfile:       "/var/run/mysync/mysync.resetup",
		Maintenancefile:   "/var/run/mysync/mysync.maintenance",
		MySQL: MySQLConfig{
			Port:                       3306,
			ReplicationPort:            3306,
			ReplicationHeartbeatPeriod: 10,
			ReplicationRetryCount:      0,
			ReplicationConnectRetry:    10,
			DataDir:                    "/var/lib/mysql",
			PidFile:                    "/var/run/mysqld/mysqld.pid",
			ErrorLog:                   "/var/log/mysql/error.log",
			ExternalReplicationSslCA:   "/etc/mysql/ssl/external_CA.pem",
		},
		Queries:                                 map[string]string{},
		Commands:                                map[string]string{},
		Hostname:                                hostname,
		Zookeeper:                               zkConfig,
		DcsWaitTimeout:                          10 * time.Second,
		DBTimeout:                               5 * time.Second,
		DBLostCheckTimeout:                      5 * time.Second,
		DBSetRoTimeout:                          30 * time.Second,
		DBSetRoForceTimeout:                     30 * time.Second,
		DisableSetReadonlyOnLost:                false,
		ResetupCrashedHosts:                     false,
		DBStopSlaveSQLThreadTimeout:             30 * time.Second,
		TickInterval:                            5 * time.Second,
		HealthCheckInterval:                     5 * time.Second,
		InfoFileHandlerInterval:                 30 * time.Second,
		RecoveryCheckInterval:                   5 * time.Second,
		ExternalCAFileCheckInterval:             5 * time.Second,
		ManagerElectionDelayAfterQuorumLoss:     30 * time.Second, // need more than 15 sec
		ManagerLockAcquireDelayAfterQuorumLoss:  45 * time.Second,
		MaxAcceptableLag:                        60.0,
		SlaveCatchUpTimeout:                     30 * time.Minute,
		DisableSemiSyncReplicationOnMaintenance: true,
		KeepSuperWritableOnCriticalDiskUsage:    false,
		ExcludeUsers:                            []string{},
		OfflineModeEnableInterval:               15 * time.Minute,
		OfflineModeEnableLag:                    24 * time.Hour,
		OfflineModeDisableLag:                   30 * time.Second,
		StreamFromReasonableLag:                 5 * time.Minute,
		PriorityChoiceMaxLag:                    60 * time.Second,
		TestDiskUsageFile:                       "", // fake disk usage, only for docker tests
		RplSemiSyncMasterWaitForSlaveCount:      1,
		WaitReplicationStartTimeout:             10 * time.Second,
		ReplicationRepairAggressiveMode:         false,
		ReplicationRepairCooldown:               1 * time.Minute,
		ReplicationRepairMaxAttempts:            3,
		TestFilesystemReadonlyFile:              "", // fake readonly status, only for docker tests
		ReplicationChannel:                      "",
		ExternalReplicationChannel:              "external",
		ExternalReplicationType:                 util.Disabled,
		ASync:                                   false,
		AsyncAllowedLag:                         0 * time.Second,
		ReplMon:                                 false,
		ReplMonSchemeName:                       "mysql",
		ReplMonTableName:                        "mysync_repl_mon",
		ReplMonWriteInterval:                    1 * time.Second,
		ReplMonErrorWaitInterval:                10 * time.Second,
		ReplMonSlaveWaitInterval:                10 * time.Second,
		ShowOnlyGTIDDiff:                        false,
		ManagerSwitchover:                       false,
		ForceSwitchover:                         false,
		DSNSettings:                             "?autocommit=1&sql_log_off=1",
	}
	return config, nil
}

// ReadFromFile reads config from file, performing all necessary checks
func ReadFromFile(configFile string) (*Config, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	loader := confita.NewLoader(file.NewBackend(configFile))
	if err = loader.Load(context.Background(), &config); err != nil {
		err = fmt.Errorf("failed to load config from %s: %s", configFile, err.Error())
		return nil, err
	}
	if config.DevMode {
		mee := util.GetEnvVariable("MYSYNC_EMULATE_ERROR", "")

		fmt.Println("MySync is running in DevMode, it will emulate ERRORS for testing")
		fmt.Println("Please be sure it's not in the production environment")
		fmt.Printf("MYSYNC_EMULATE_ERROR='%s'", mee)
		fmt.Printf("\n\n")
	}
	config.SetDynamicDefaults()
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (cfg *Config) SetDynamicDefaults() {
	if cfg.NotCriticalDiskUsage == 0.0 {
		cfg.NotCriticalDiskUsage = cfg.CriticalDiskUsage
	}
}

func (cfg *Config) Validate() error {
	if cfg.NotCriticalDiskUsage > cfg.CriticalDiskUsage {
		return fmt.Errorf("not_critical_disk_usage should be <= critical_disk_usage")
	}
	if cfg.SemiSync && cfg.ASync {
		return fmt.Errorf("can't run in both semisync and async mode")
	}
	if cfg.ASync && !cfg.ReplMon {
		return fmt.Errorf("repl mon must be enabled to run mysync in async mode")
	}
	return nil
}
