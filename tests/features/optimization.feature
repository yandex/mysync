Feature: optimization mode works on replicas

  Scenario: replicas automatically detect absense of lag
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
    REPLICATION_LAG_THRESHOLD=5s
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
    REPLICATION_LAG_THRESHOLD=5s
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
