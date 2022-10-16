Feature: mysync should work without semi-sync replication

  Scenario: switchover works
    Given cluster environment is
      """
      MYSYNC_SEMISYNC=false
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

    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "result": {
          "ok": true
        }
      }
      """
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"

    When I run command on host "mysql1"
      """
      mysync switch --from mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "mysql2",
        "to": "",
        "result": {
          "ok": true
        }
      }
      """
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should be writable
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"

  Scenario: failover works
    Given cluster environment is
      """
      MYSYNC_FAILOVER=true
      MYSYNC_SEMISYNC=false
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

    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    Then zookeeper node "/test/manager" should match regexp within "10" seconds
      """
        .*mysql[23].*
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "cause": "auto",
        "from": "mysql1",
        "result": {
          "ok": true
        }
      }
      """
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql2","mysql3"]
      """
    And zookeeper node "/test/recovery/mysql1" should exist
    And zookeeper node "/test/master" should match regexp
      """
        .*mysql[23].*
      """
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should be writable
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0"


    When host "{{.new_master}}" is stopped
    Then mysql host "{{.new_master}}" should become unavailable within "10" seconds
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "cause": "auto",
        "from": "{{.new_master}}",
        "result": {
          "ok": true
        }
      }
      """
    And zookeeper node "/test/recovery/{{.new_master}}" should exist

    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should be writable
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["{{.new_master}}"]
      """
