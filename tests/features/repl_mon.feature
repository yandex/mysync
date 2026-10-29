Feature: repl_mon tests

  Scenario: repl_mon enabled
    Given cluster environment is
      """
      REPL_MON=true
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And I wait for "5" seconds
    And I run SQL on mysql host "mysql1" expecting error on number "1050"
      """
        CREATE TABLE mysql.mysync_repl_mon(
            ts TIMESTAMP(3)
        ) ENGINE=INNODB;
      """
    And I run SQL on mysql host "mysql1"
      """
        SELECT (CURRENT_TIMESTAMP(3) - ts) < 2 as res FROM mysql.mysync_repl_mon
      """
    Then SQL result should match json
      """
      [{"res":1}]
      """
    And mysql host "mysql2" should be replica of "mysql1"
    Then zookeeper node "/test/health/mysql2" should match json within "20" seconds
      """
      {
        "is_loading_binlog": true
      }
      """
    And mysql host "mysql3" should be replica of "mysql1"
    Then zookeeper node "/test/health/mysql3" should match json within "20" seconds
      """
      {
        "is_loading_binlog": true
      }
      """

  Scenario: repl_mon disabled
    Given cluster environment is
      """
      REPL_MON=false
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And I wait for "5" seconds
    And I run SQL on mysql host "mysql1" expecting error on number "1146"
      """
        SELECT ts FROM mysql.mysync_repl_mon
      """
    And I run SQL on mysql host "mysql2" expecting error on number "1146"
      """
        SELECT ts FROM mysql.mysync_repl_mon
      """
    And I run SQL on mysql host "mysql3" expecting error on number "1146"
      """
        SELECT ts FROM mysql.mysync_repl_mon
      """
    And mysql host "mysql2" should be replica of "mysql1"
    Then zookeeper node "/test/health/mysql2" should match json within "20" seconds
      """
      {
        "is_loading_binlog": false
      }
      """
    And mysql host "mysql3" should be replica of "mysql1"
    Then zookeeper node "/test/health/mysql3" should match json within "20" seconds
      """
      {
        "is_loading_binlog": false
      }
      """

  Scenario Outline: repeated failover requires heartbeat after the previous failover
    Given cluster environment is
      """
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_FAILOVER_COOLDOWN=3600s
      REPL_MON=false
      """
    And cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    # Emulate an externally maintained heartbeat, without starting the MySync writer.
    When I run SQL on mysql host "mysql1"
      """
      CREATE TABLE mysql.mysync_repl_mon(ts TIMESTAMP(3))
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.mysync_repl_mon VALUES(FROM_UNIXTIME(1))
      """
    And I set zookeeper node "/test/ha_nodes/mysql2" to
      """
      {"priority": 10}
      """
    And I set zookeeper node "/test/ha_nodes/mysql3" to
      """
      {"priority": 5}
      """
    And host "mysql1" is stopped
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {"cause":"auto","from":"mysql1","result":{"ok":true}}
      """
    And mysql host "mysql2" should be master
    When host "mysql1" is started
    Then mysql host "mysql1" should become available within "30" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "30" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I run SQL on mysql host "mysql2"
      """
      UPDATE mysql.mysync_repl_mon SET ts = <heartbeat>
      """
    And I run SQL on mysql host "mysql2"
      """
      SELECT HEX(@@GLOBAL.gtid_executed) AS gtid
      """
    And I save SQL result as "position"
    And I run SQL on mysql host "mysql1"
      """
      SELECT WAIT_FOR_EXECUTED_GTID_SET(UNHEX('{{(index .position 0).gtid}}'), 1) AS caught_up
      """
    Then SQL result should match json
      """
      [{"caught_up":0}]
      """
    When I run SQL on mysql host "mysql3"
      """
      SELECT WAIT_FOR_EXECUTED_GTID_SET(UNHEX('{{(index .position 0).gtid}}'), 1) AS caught_up
      """
    Then SQL result should match json
      """
      [{"caught_up":0}]
      """
    When host "mysql2" is stopped
    And I wait for "30" seconds
    Then zookeeper node "/test/master" should match json within "30" seconds
      """
      "<master>"
      """
    And zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {"cause":"auto","from":"<last_source>","result":{"ok":true}}
      """
    And mysql host "mysql3" should have variable "read_only" set to "<read_only>"

    Examples:
      | heartbeat            | master | last_source | read_only |
      | CURRENT_TIMESTAMP(3) | mysql3 | mysql2      | 0         |
      | FROM_UNIXTIME(1)     | mysql2 | mysql1      | 1         |
