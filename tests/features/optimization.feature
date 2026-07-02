Feature: optimization mode works on replicas

  Scenario: replicas automatically detect absence of lag
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And mysql host "mysql1" should be master
    When I set zookeeper node "/test/optimization_nodes" to
      """
      {}
      """
    And I set zookeeper node "/test/optimization_nodes/mysql2" to
      """
      {}
      """
    And I set zookeeper node "/test/optimization_nodes/mysql3" to
      """
      {}
      """
    Then zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    And SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

    And zookeeper node "/test/optimization_nodes/mysql3" should not exist within "30" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    And SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

  Scenario: replicas automatically detect lag convergence
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And mysql host "mysql1" should be master

    When I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE TABLE IF NOT EXISTS mysql.test_table1 (
          value VARCHAR(30)
      )
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("A"), ("B"), ("C")
      """
    And I wait for "15" seconds
    And I set zookeeper node "/test/optimization_nodes" to
      """
      {}
      """
    And I set zookeeper node "/test/optimization_nodes/mysql2" to
      """
      {}
      """
    And zookeeper node "/test/optimization_nodes/mysql2" should exist within "30" seconds
    And I wait for "15" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """

    When I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0;
      START REPLICA FOR CHANNEL '';
      """
    Then zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    And SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

  Scenario: there can be only one optimized host
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    And cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And mysql host "mysql1" should be master

    When I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE TABLE IF NOT EXISTS mysql.test_table1 (
          value VARCHAR(30)
      )
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("A"), ("B"), ("C")
      """

    And I wait for "15" seconds

    And I set zookeeper node "/test/optimization_nodes" to
      """
      {}
      """
    And I set zookeeper node "/test/optimization_nodes/mysql2" to
      """
      {}
      """
    And I wait for "15" seconds
    And I set zookeeper node "/test/optimization_nodes/mysql3" to
      """
      {}
      """
    And zookeeper node "/test/optimization_nodes/mysql2" should exist within "30" seconds
    And zookeeper node "/test/optimization_nodes/mysql3" should exist within "30" seconds
    And I wait for "15" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """

    And I run SQL on mysql host "mysql3"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

    When I delete zookeeper node "/test/optimization_nodes/mysql2"
    Then zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds
    And I wait for "15" seconds

    And I run SQL on mysql host "mysql3"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """

    # These tests are intended to be in the CLI feature file,
    # but MySQL 8.0 uses different syntax for replication lag than 5.7.
    # Since the main functionality is in MySync, we can skip testing on 5.7.
  Scenario: CLI turbo mode works properly
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    And cluster is up and running
    Then mysql host "mysql1" should be master

    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "15" seconds

    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'can be optimized'
    """

    When I run command on host "mysql2"
    """
    mysync turbo on
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host optimization has been started.
    """
    And zookeeper node "/test/optimization_nodes/mysql2" should exist within "30" seconds
    And I wait for "15" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'optimization is running'
    """

    When I run command on host "mysql2"
    """
    mysync turbo off
    """
    Then command return code should be "0"
    And zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds

    When I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'can be optimized'
    """

  Scenario: CLI turbo mode works properly with non-default replication options
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master

    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "15" seconds

    When I run SQL on mysql host "mysql1"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 999;
    """
    When I run command on host "mysql2"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 999;
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'can be optimized'
    """
    When I run command on host "mysql2"
    """
    mysync turbo on
    """
    Then command return code should be "0"

    When zookeeper node "/test/optimization_nodes/mysql2" should exist within "30" seconds
    And I wait for "15" seconds
    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """
    When I run command on host "mysql2"
    """
    mysync turbo off
    """
    Then command return code should be "0"
    And zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds

    When I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":999}]
    """

  Scenario: CLI turbo mode works properly with non-default replication options, which are more than MySync optimal ones
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master

    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "15" seconds

    When I run SQL on mysql host "mysql1"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 1001;
    """
    When I run SQL on mysql host "mysql2"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 1001;
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'configuration of the host is already optimized'
    """
    When I run command on host "mysql2"
    """
    mysync turbo on
    """
    Then command return code should be "1"
    And zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds
    And command output should match regexp
    """
    Can't optimize host in status 'configuration of the host is already optimized'
    """

    When I run SQL on mysql host "mysql1"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 0; SET GLOBAL sync_binlog = 3;
    """
    When I run SQL on mysql host "mysql2"
    """
    SET GLOBAL innodb_flush_log_at_trx_commit = 0; SET GLOBAL sync_binlog = 3;
    """
    When I run command on host "mysql2"
    """
    mysync turbo get
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    The host is in status 'configuration of the host is already optimized'
    """
    When I run command on host "mysql2"
    """
    mysync turbo on
    """
    Then command return code should be "1"
    And zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds

    When I run command on host "mysql1"
    """
    mysync turbo on
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    Can't optimize host in status 'host is master'
    """

  Scenario: CLI turbo mode works properly on master host
    Given cluster is up and running
    Then mysql host "mysql1" should be master

    When I run command on host "mysql1"
    """
    mysync turbo on
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    Can't optimize host in status 'host is master'
    """

  Scenario: CLI disable-all works
    Given cluster environment is
    """
    HIGH_REPLICATION_MARK=5s
    LOW_REPLICATION_MARK=5s
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master

    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "15" seconds
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "15" seconds

    When I run command on host "mysql2"
    """
    mysync turbo on
    """
    Then command return code should be "0"
    And I wait for "15" seconds

    And I run command on host "mysql3"
    """
    mysync turbo on
    """
    Then command return code should be "0"

    When zookeeper node "/test/optimization_nodes/mysql2" should exist within "30" seconds
    When zookeeper node "/test/optimization_nodes/mysql3" should exist within "30" seconds
    And I wait for "15" seconds

    And I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
    """
    And I run SQL on mysql host "mysql3"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

    When I run command on host "mysql1"
    """
    mysync turbo off-all
    """
    Then command return code should be "0"
    And zookeeper node "/test/optimization_nodes/mysql2" should not exist within "30" seconds
    And zookeeper node "/test/optimization_nodes/mysql3" should not exist within "30" seconds

    When I run SQL on mysql host "mysql2"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """
    When I run SQL on mysql host "mysql3"
    """
    SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@GLOBAL.sync_binlog as SyncBinlog
    """
    Then SQL result should match json
    """
    [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
    """

  Scenario: relay log size enables turbo mode with low replication lag
    Given cluster environment is
    """
    RELAY_LOG_OPTIMIZATION_ENABLED=true
    RELAY_LOG_OPTIMIZATION_INTERVAL=2s
    RELAY_LOG_MAX_BYTES=2097152
    HIGH_REPLICATION_MARK=120s
    LOW_REPLICATION_MARK=60s
    MYSYNC_REPLICATION_LAG_QUERY="SELECT 30 AS Seconds_Behind_Master"
    MYSYNC_SEMISYNC=false
    MYSYNC_ASYNC=true
    ASYNC_ALLOWED_LAG=6000s
    REPL_MON=true
    OFFLINE_MODE_ENABLE_LAG=6000s
    MYSYNC_STREAM_FROM_REASONABLE_LAG=6000s
    """
    Given cluster is up and running
    And mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """

    # Keep each relay file smaller than the optimization threshold.
    When I run SQL on mysql host "mysql3"
    """
    SET GLOBAL max_relay_log_size = 1048576;
    """
    And I run SQL on mysql host "mysql2"
    """
    SET GLOBAL max_relay_log_size = 1048576;
    STOP REPLICA FOR CHANNEL '';
    CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 6000;
    START REPLICA FOR CHANNEL '';
    """
    Then zookeeper node "/test/optimization_nodes/mysql2" should not exist within "15" seconds

    When I run SQL on mysql host "mysql1"
    """
    CREATE TABLE mysql.relay_filler (id INT AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB);
    INSERT INTO mysql.relay_filler (payload) VALUES (REPEAT('x', 1048576));
    INSERT INTO mysql.relay_filler (payload) SELECT payload FROM mysql.relay_filler;
    INSERT INTO mysql.relay_filler (payload) SELECT payload FROM mysql.relay_filler;
    INSERT INTO mysql.relay_filler (payload) SELECT payload FROM mysql.relay_filler;
    """
    Then zookeeper node "/test/optimization_nodes/mysql2" should match json within "60" seconds
    """
    {"reason":"relay_log"}
    """
    And mysql host "mysql2" should have variable "innodb_flush_log_at_trx_commit" set to "2" within "30" seconds
    And mysql host "mysql2" should have variable "sync_binlog" set to "1000" within "30" seconds

    When I run SQL on mysql host "mysql2"
    """
    STOP REPLICA FOR CHANNEL '';
    CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0;
    START REPLICA FOR CHANNEL '';
    """
    Then zookeeper node "/test/optimization_nodes/mysql2" should not exist within "60" seconds
    And mysql host "mysql2" should have variable "innodb_flush_log_at_trx_commit" set to "1" within "30" seconds
    And mysql host "mysql2" should have variable "sync_binlog" set to "1" within "30" seconds
