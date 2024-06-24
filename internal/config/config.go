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
	User                       string `config:"user,required"`
	Password                   string `config:"password,required"`
	Port                       int    `config:"port" yaml:"port"`
	SslCA                      string `config:"ssl_ca" yaml:"ssl_ca"`
	ReplicationUser            string `config:"replication_user,required" yaml:"replication_user"`
	ReplicationPort            int    `config:"replication_port" yaml:"replication_port"`
	ReplicationPassword        string `config:"replication_password,required" yaml:"replication_password"`
	ReplicationSslCA           string `config:"replication_ssl_ca" yaml:"replication_ssl_ca"`
	ReplicationRetryCount      int    `config:"replication_retry_count" yaml:"replication_retry_count"`
	ReplicationConnectRetry    int    `config:"replication_connect_retry" yaml:"replication_connect_retry"`
	ReplicationHeartbeatPeriod int    `config:"replication_heartbeat_period" yaml:"replication_heartbeat_period"`
	ExternalReplicationSslCA   string `config:"external_replication_ssl_ca" yaml:"external_replication_ssl_ca"`
	DataDir                    string `config:"data_dir" yaml:"data_dir"`
	PidFile                    string `config:"pid_file" yaml:"pid_file"`
	ErrorLog                   string `config:"error_log" yaml:"error_log"`
}

// Config contains all mysync configuration
type Config struct {
	DevMode                                 bool                         `config:"dev_mode" yaml:"dev_mode"`
	SemiSync                                bool                         `config:"semi_sync" yaml:"semi_sync"`
	SemiSyncEnableLag                       int64                        `config:"semi_sync_enable_lag" yaml:"semi_sync_enable_lag"`
	Failover                                bool                         `config:"failover" yaml:"failover"`
	FailoverCooldown                        time.Duration                `config:"failover_cooldown" yaml:"failover_cooldown"`
	FailoverDelay                           time.Duration                `config:"failover_delay" yaml:"failover_delay"`
	InactivationDelay                       time.Duration                `config:"inactivation_delay" yaml:"inactivation_delay"`
	CriticalDiskUsage                       float64                      `config:"critical_disk_usage" yaml:"critical_disk_usage"`
	NotCriticalDiskUsage                    float64                      `config:"not_critical_disk_usage" yaml:"not_critical_disk_usage"`
	LogLevel                                string                       `config:"loglevel"`
	Log                                     string                       `config:"log"`
	Hostname                                string                       `config:"hostname"`
	Lockfile                                string                       `config:"lockfile"`
	InfoFile                                string                       `config:"info_file" yaml:"info_file"`
	Emergefile                              string                       `config:"emergefile"`
	Resetupfile                             string                       `config:"resetupfile"`
	Maintenancefile                         string                       `config:"maintenancefile"`
	MySQL                                   MySQLConfig                  `config:"mysql"`
	Queries                                 map[string]string            `config:"queries"`
	Commands                                map[string]string            `config:"commands"`
	Zookeeper                               dcs.ZookeeperConfig          `config:"zookeeper"`
	DcsWaitTimeout                          time.Duration                `config:"dcs_wait_timeout" yaml:"dcs_wait_timeout"`
	DBTimeout                               time.Duration                `config:"db_timeout" yaml:"db_timeout"`
	DBLostCheckTimeout                      time.Duration                `config:"db_lost_check_timeout" yaml:"db_lost_check_timeout"`
	DBSetRoTimeout                          time.Duration                `config:"db_set_ro_timeout" yaml:"db_set_ro_timeout"`
	DBSetRoForceTimeout                     time.Duration                `config:"db_set_ro_force_timeout" yaml:"db_set_ro_force_timeout"`
	DBStopSlaveSQLThreadTimeout             time.Duration                `config:"db_stop_slave_sql_thread_timeout" yaml:"db_stop_slave_sql_thread_timeout"`
	TickInterval                            time.Duration                `config:"tick_interval" yaml:"tick_interval"`
	HealthCheckInterval                     time.Duration                `config:"healthcheck_interval" yaml:"healthcheck_interval"`
	InfoFileHandlerInterval                 time.Duration                `config:"info_file_handler_interval" yaml:"info_file_handler_interval"`
	RecoveryCheckInterval                   time.Duration                `config:"recoverycheck_interval" yaml:"recoverycheck_interval"`
	ExternalCAFileCheckInterval             time.Duration                `config:"external_ca_file_check_interval" yaml:"external_ca_file_check_interval"`
	MaxAcceptableLag                        float64                      `config:"max_acceptable_lag" yaml:"max_acceptable_lag"`
	SlaveCatchUpTimeout                     time.Duration                `config:"slave_catch_up_timeout" yaml:"slave_catch_up_timeout"`
	DisableSemiSyncReplicationOnMaintenance bool                         `config:"disable_semi_sync_replication_on_maintenance" yaml:"disable_semi_sync_replication_on_maintenance"`
	KeepSuperWritableOnCriticalDiskUsage    bool                         `config:"keep_super_writable_on_critical_disk_usage" yaml:"keep_super_writable_on_critical_disk_usage"`
	ExcludeUsers                            []string                     `config:"exclude_users" yaml:"exclude_users"`
	OfflineModeEnableInterval               time.Duration                `config:"offline_mode_enable_interval" yaml:"offline_mode_enable_interval"`
	OfflineModeEnableLag                    time.Duration                `config:"offline_mode_enable_lag" yaml:"offline_mode_enable_lag"`
	OfflineModeDisableLag                   time.Duration                `config:"offline_mode_disable_lag" yaml:"offline_mode_disable_lag"`
	DisableSetReadonlyOnLost                bool                         `config:"disable_set_readonly_on_lost" yaml:"disable_set_readonly_on_lost"`
	ResetupCrashedHosts                     bool                         `config:"resetup_crashed_hosts" yaml:"resetup_crashed_hosts"`
	StreamFromReasonableLag                 time.Duration                `config:"stream_from_reasonable_lag" yaml:"stream_from_reasonable_lag"`
	PriorityChoiceMaxLag                    time.Duration                `config:"priority_choice_max_lag" yaml:"priority_choice_max_lag"`
	TestDiskUsageFile                       string                       `config:"test_disk_usage_file" yaml:"test_disk_usage_file"`
	RplSemiSyncMasterWaitForSlaveCount      int                          `config:"rpl_semi_sync_master_wait_for_slave_count" yaml:"rpl_semi_sync_master_wait_for_slave_count"`
	WaitReplicationStartTimeout             time.Duration                `config:"wait_start_replication_timeout" yaml:"wait_start_replication_timeout"`
	ReplicationRepairAggressiveMode         bool                         `config:"replication_repair_aggressive_mode" yaml:"replication_repair_aggressive_mode"`
	ReplicationRepairCooldown               time.Duration                `config:"replication_repair_cooldown" yaml:"replication_repair_cooldown"`
	ReplicationRepairMaxAttempts            int                          `config:"replication_repair_max_attempts" yaml:"replication_repair_max_attempts"`
	TestFilesystemReadonlyFile              string                       `config:"test_filesystem_readonly_file" yaml:"test_filesystem_readonly_file"`
	ReplicationChannel                      string                       `config:"replication_channel" yaml:"replication_channel"`
	ExternalReplicationChannel              string                       `config:"external_replication_channel" yaml:"external_replication_channel"`
	ExternalReplicationType                 util.ExternalReplicationType `config:"external_replication_type" yaml:"external_replication_type"`
	ASync                                   bool                         `config:"async" yaml:"async"`
	AsyncAllowedLag                         time.Duration                `config:"async_allowed_lag" yaml:"async_allowed_lag"`
	ReplMon                                 bool                         `config:"repl_mon" yaml:"repl_mon"`
	ReplMonSchemeName                       string                       `config:"repl_mon_scheme_name" yaml:"repl_mon_scheme_name"`
	ReplMonTableName                        string                       `config:"repl_mon_table_name" yaml:"repl_mon_table_name"`
	ReplMonWriteInterval                    time.Duration                `config:"repl_mon_write_interval" yaml:"repl_mon_write_interval"`
	ReplMonErrorWaitInterval                time.Duration                `config:"repl_mon_error_wait_interval" yaml:"repl_mon_error_wait_interval"`
	ReplMonSlaveWaitInterval                time.Duration                `config:"repl_mon_slave_wait_interval" yaml:"repl_mon_slave_wait_interval"`
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
