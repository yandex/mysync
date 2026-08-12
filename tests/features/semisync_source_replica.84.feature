Feature: source/replica semisync dialect

  Scenario: mysync manages semisync with source/replica plugins
    Given cluster environment is
      """
      SEMISYNC_DIALECT=sourceReplica
      MYSYNC_WAIT_FOR_SLAVE_COUNT=1
      """
    And cluster is up and running

    When I run SQL on mysql host "mysql1"
      """
      SELECT PLUGIN_NAME AS PluginName, PLUGIN_STATUS AS PluginStatus
      FROM information_schema.PLUGINS
      WHERE PLUGIN_NAME LIKE 'rpl_semi_sync_%'
      ORDER BY PLUGIN_NAME
      """
    Then SQL result should match json_exactly
      """
      [
        {"PluginName":"rpl_semi_sync_replica","PluginStatus":"ACTIVE"},
        {"PluginName":"rpl_semi_sync_source","PluginStatus":"ACTIVE"}
      ]
      """

    And mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "mysql1"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_clients', 'Rpl_semi_sync_source_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """

    When I run command on host "mysql1"
      """
      mysync maint on
      """
    Then command return code should be "0"
    And zookeeper node "/test/maintenance" should match json within "30" seconds
      """
      {
        "mysync_paused": true
      }
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "0" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1"

    When I run command on host "mysql1"
      """
      mysync maint off
      """
    Then command return code should be "0"
    And zookeeper node "/test/maintenance" should not exist within "30" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds

    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/last_switch" should match json within "60" seconds
      """
      {
        "to": "mysql2",
        "master_transition": "switchover",
        "result": {
          "ok": true
        }
      }
      """
    And mysql host "mysql2" should be master
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "mysql2"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_clients', 'Rpl_semi_sync_source_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """

  Scenario: mysync switches between nodes with different semisync dialects
    Given cluster environment is
      """
      MYSQL1_SEMISYNC_DIALECT=masterSlave
      MYSQL2_SEMISYNC_DIALECT=sourceReplica
      MYSQL3_SEMISYNC_DIALECT=sourceReplica
      MYSYNC_WAIT_FOR_SLAVE_COUNT=1
      """
    And cluster is up and running

    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_wait_for_slave_count" set to "1" within "20" seconds
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "mysql1"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_master_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_master_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_master_clients', 'Rpl_semi_sync_master_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """

    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/last_switch" should match json within "60" seconds
      """
      {
        "to": "mysql2",
        "master_transition": "switchover",
        "result": {
          "ok": true
        }
      }
      """
    And mysql host "mysql2" should be master
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "mysql2"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_clients', 'Rpl_semi_sync_source_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """

    When I run command on host "mysql2"
      """
      mysync switch --to mysql1 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/last_switch" should match json within "60" seconds
      """
      {
        "to": "mysql1",
        "master_transition": "switchover",
        "result": {
          "ok": true
        }
      }
      """
    And mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_wait_for_slave_count" set to "1" within "20" seconds
    And mysql host "mysql2" should become replica of "mysql1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should become replica of "mysql1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "mysql1"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_master_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_master_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_master_clients', 'Rpl_semi_sync_master_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """

  Scenario: mysync manages source/replica semisync after failover
    Given cluster environment is
      """
      SEMISYNC_DIALECT=sourceReplica
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_WAIT_FOR_SLAVE_COUNT=1
      """
    And cluster is up and running

    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    And zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "cause": "auto",
        "from": "mysql1",
        "master_transition": "failover",
        "result": {
          "ok": true
        }
      }
      """
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds

    When I run SQL on mysql host "{{.new_master}}"
      """
      SHOW REPLICAS
      """
    And I save SQL result as "replicas"
    And I save "{{ (index .replicas 0).Host }}" as "old_replica"
    Then mysql host "{{.old_replica}}" should be replica of "{{.new_master}}"
    And mysql host "{{.old_replica}}" should have variable "rpl_semi_sync_replica_enabled" set to "1"
    And mysql host "{{.old_replica}}" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run SQL on mysql host "{{.new_master}}"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_clients', 'Rpl_semi_sync_source_status')
      """
    Then SQL result should match json
      """
      [{"clients": "1", "status": "ON", "stuck": 0}]
      """

    When host "mysql1" is started
    Then mysql host "mysql1" should become available within "20" seconds
    And mysql host "mysql1" should become replica of "{{.new_master}}" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "10" seconds

  Scenario: source/replica semisync waits for clients when active nodes grow
    Given cluster environment is
      """
      SEMISYNC_DIALECT=sourceReplica
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=30s
      MYSYNC_FAILOVER_COOLDOWN=0s
      MYSYNC_MASTER_FIRST_ADJUST_SS_ORDER=true
      MYSYNC_WAIT_FOR_SLAVE_COUNT=1
      """
    And cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """

    When host "mysql3" is deleted
    Then mysql host "mysql3" should become unavailable within "10" seconds
    Then zookeeper node "/test/manager" should match regexp
      """
      .*mysql[12].*
      """
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2"]
      """
    And zookeeper node "/test/master" should match regexp
      """
      .*mysql1.*
      """

    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly
      """
      ["mysql1","mysql2"]
      """
    Then zookeeper node "/test/master" should match regexp within "30" seconds
      """
      .*mysql2.*
      """
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable
    And zookeeper node "/test/active_nodes" should match json_exactly within "10" seconds
      """
      ["mysql2"]
      """

    When host "mysql3" is added
    Then mysql host "mysql3" should become available within "10" seconds
    And mysql host "mysql3" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "1" within "20" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql2","mysql3"]
      """

    When host "mysql1" is started
    Then mysql host "mysql1" should become available within "10" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """

    # Regression: raising wait_slave_count from 0 to 1 must happen only after
    # source/replica semi-sync clients have connected to the new source.
    When I run SQL on mysql host "mysql2"
      """
      SELECT
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_clients' THEN VARIABLE_VALUE END) AS clients,
        MAX(CASE WHEN VARIABLE_NAME = 'Rpl_semi_sync_source_status'  THEN VARIABLE_VALUE END) AS status,
        (SELECT count(*) FROM information_schema.PROCESSLIST WHERE state LIKE 'Waiting for semi-sync ACK from%') AS stuck
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_clients', 'Rpl_semi_sync_source_status')
      """
    Then SQL result should match json
      """
      [{"clients": "2", "status": "ON", "stuck": 0}]
      """
    And I have no SQL execution error at mysql host "mysql2" within "5" seconds
