Feature: mysync async mode tests

  Scenario: failover with lag less then allowed
    Given cluster environment is
      """
      MYSYNC_SEMISYNC=false
      MYSYNC_ASYNC=true
      ASYNC_ALLOWED_LAG=70
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_FAILOVER_COOLDOWN=0s
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"


    When I run SQL on mysql host "mysql1"
      """
      CREATE TABLE mysql.mdb_repl_mon(
          ts TIMESTAMP(3)
      ) ENGINE=INNODB;
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.mdb_repl_mon VALUES(CURRENT_TIMESTAMP(3));
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE EVENT mysql.mdb_repl_mon_event
      ON SCHEDULE EVERY 1 SECOND
      DO UPDATE mysql.mdb_repl_mon SET ts = CURRENT_TIMESTAMP(3);
      """
    Then mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"

    And I wait for "2" seconds
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
    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 50;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 90;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "120" seconds
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("D"), ("E"), ("F")
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    Then zookeeper node "/test/manager" should match regexp within "10" seconds
      """
      .*mysql[23].*
      """
    Then zookeeper node "/test/last_switch" should match json within "40" seconds
      """
      {
          "cause": "auto",
          "from": "mysql1",
          "result": {
              "ok": true
          }
      }
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When I run SQL on mysql host "mysql3"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """

  Scenario: failover with lag greater then allowed
    Given cluster environment is
      """
      MYSYNC_SEMISYNC=false
      MYSYNC_ASYNC=true
      ASYNC_ALLOWED_LAG=15
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_FAILOVER_COOLDOWN=0s
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"


    When I run SQL on mysql host "mysql1"
      """
      CREATE TABLE mysql.mdb_repl_mon(
          ts TIMESTAMP(3)
      ) ENGINE=INNODB;
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.mdb_repl_mon VALUES(CURRENT_TIMESTAMP(3));
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE EVENT mysql.mdb_repl_mon_event
      ON SCHEDULE EVERY 1 SECOND
      DO UPDATE mysql.mdb_repl_mon SET ts = CURRENT_TIMESTAMP(3);
      """
    Then mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"

    And I wait for "2" seconds
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
    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 30;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 60;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "120" seconds
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("D"), ("E"), ("F")
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    Then zookeeper node "/test/manager" should match regexp within "10" seconds
      """
      .*mysql[23].*
      """
    Then zookeeper node "/test/last_switch" should match json within "60" seconds
      """
      {
          "cause": "auto",
          "from": "mysql1",
          "result": {
              "ok": true
          }
      }
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C,D,E,F"}]
      """
    When I run SQL on mysql host "mysql3"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
        [{"val":"A,B,C"}]
      """
    And I wait for "60" seconds
    When I run SQL on mysql host "mysql3"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C,D,E,F"}]
      """

  Scenario: manual switchover ignores async
    Given cluster environment is
      """
      MYSYNC_SEMISYNC=false
      MYSYNC_ASYNC=true
      ASYNC_ALLOWED_LAG=70
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_FAILOVER_COOLDOWN=0s
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"


    When I run SQL on mysql host "mysql1"
      """
      CREATE TABLE mysql.mdb_repl_mon(
          ts TIMESTAMP(3)
      ) ENGINE=INNODB;
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.mdb_repl_mon VALUES(CURRENT_TIMESTAMP(3));
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE EVENT mysql.mdb_repl_mon_event
      ON SCHEDULE EVERY 1 SECOND
      DO UPDATE mysql.mdb_repl_mon SET ts = CURRENT_TIMESTAMP(3);
      """
    Then mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"

    And I wait for "2" seconds
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
    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 50;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 90;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "120" seconds
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("D"), ("E"), ("F")
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When I run command on host "mysql1"
      """
      mysync switch --from mysql1 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/switch" should match json
      """
      {
        "from": "mysql1"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "80" seconds
      """
      {
          "from": "mysql1",
          "to": "",
          "cause": "manual",
          "initiated_by": "mysql1",
          "result": {
              "ok": true
          }
      }
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C,D,E,F"}]
      """
    When I wait for "120" seconds
    And I run SQL on mysql host "mysql3"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C,D,E,F"}]
      """

  Scenario: failover with lag less then allowed and more then default PriorityChoiceMaxLag
    Given cluster environment is
      """
      MYSYNC_SEMISYNC=false
      MYSYNC_ASYNC=true
      ASYNC_ALLOWED_LAG=90
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      MYSYNC_FAILOVER_COOLDOWN=0s
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"


    When I run SQL on mysql host "mysql1"
      """
      CREATE TABLE mysql.mdb_repl_mon(
          ts TIMESTAMP(3)
      ) ENGINE=INNODB;
      """
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.mdb_repl_mon VALUES(CURRENT_TIMESTAMP(3));
      """
    And I run SQL on mysql host "mysql1"
      """
      CREATE EVENT mysql.mdb_repl_mon_event
      ON SCHEDULE EVERY 1 SECOND
      DO UPDATE mysql.mdb_repl_mon SET ts = CURRENT_TIMESTAMP(3);
      """
    Then mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"

    And I wait for "2" seconds
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
    And I run SQL on mysql host "mysql2"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 70;
      START REPLICA FOR CHANNEL '';
      """
    And I run SQL on mysql host "mysql3"
      """
      STOP REPLICA FOR CHANNEL '';
      CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 110;
      START REPLICA FOR CHANNEL '';
      """
    And I wait for "120" seconds
    And I run SQL on mysql host "mysql1"
      """
      INSERT INTO mysql.test_table1 VALUES ("D"), ("E"), ("F")
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    Then zookeeper node "/test/manager" should match regexp within "10" seconds
      """
      .*mysql[23].*
      """
    Then zookeeper node "/test/last_switch" should match json within "40" seconds
      """
      {
          "cause": "auto",
          "from": "mysql1",
          "result": {
              "ok": true
          }
      }
      """
    And I wait for "2" seconds
    When I run SQL on mysql host "mysql2"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """
    When I wait for "120" seconds
    And I run SQL on mysql host "mysql3"
      """
      SELECT GROUP_CONCAT(value) as val from (SELECT value from mysql.test_table1 order by value) as t
      """
    Then SQL result should match json
      """
      [{"val":"A,B,C"}]
      """