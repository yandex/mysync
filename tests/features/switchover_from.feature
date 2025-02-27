Feature: manual switchover from old master

  Scenario: switchover fails without quorum
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When mysql on host "mysql3" is killed
    And mysql on host "mysql2" is killed
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
    Then zookeeper node "/test/last_rejected_switch" should match json within "30" seconds
      """
      {
          "from": "mysql1",
          "to": "",
          "cause": "manual",
          "initiated_by": "mysql1",
          "result": {
              "ok": false,
              "error": "no quorum, have 0 replicas while 2 is required"
          }
      }
      """

  Scenario Outline: if switchover was approved, it will not be rejected
    Given cluster environment is
      """
      FORCE_SWITCHOVER=<force_switchover>
      """
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When mysql on host "mysql3" is killed
    And mysql on host "mysql2" is killed
    And I set zookeeper node "/test/switch" to
      """
      {
          "from": "mysql1",
          "to": "",
          "cause": "manual",
          "initiated_by": "mysql1",
          "run_count": 1
      }
      """
    Then zookeeper node "/test/last_switch" should not exist within "30" seconds  
    Then zookeeper node "/test/last_rejected_switch" should not exist within "30" seconds  
    When mysql on host "mysql3" is started
    And mysql on host "mysql2" is started
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
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
    Examples:
      | force_switchover  |
      | true              |
      | false             |

  Scenario Outline: switchover from works on healthy cluster
    Given cluster environment is
      """
      MYSYNC_FAILOVER=<failover>
      """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql replication on host "mysql3" should run fine within "5" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
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
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "mysql1",
        "result": {
          "ok": true
        }
      }

      """
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should be writable
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_slave_enabled" set to "0"

    When I run SQL on mysql host "{{.new_master}}"
      """
      SHOW SLAVE HOSTS
      """
    And I save SQL result as "slaves"
    And I save "{{ (index .slaves 0).Host }}" as "slave_1"
    And I save "{{ (index .slaves 1).Host }}" as "slave_2"
    Then mysql host "{{.slave_1}}" should be replica of "{{.new_master}}"
    And mysql host "{{.slave_1}}" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "{{.slave_1}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "{{.slave_1}}" should run fine within "3" seconds
    And mysql host "{{.slave_1}}" should be read only
    Then mysql host "{{.slave_2}}" should be replica of "{{.new_master}}"
    And mysql host "{{.slave_2}}" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "{{.slave_2}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "{{.slave_2}}" should run fine within "3" seconds
    And mysql host "{{.slave_2}}" should be read only

    Examples:
      | failover |
      | true     |
      | false    |

  Scenario Outline: switchover from works with dead slave
    Given cluster environment is
      """
      MYSYNC_FAILOVER=<failover>
      """
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When host "mysql3" is stopped
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    And mysql host "mysql3" should become unavailable within "10" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2"]
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
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "mysql1",
        "result": {
          "ok": true
        }
      }
      """
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"

    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "3" seconds
    And mysql host "mysql1" should be read only
    And mysql host "mysql3" should become unavailable within "10" seconds

    When host "mysql3" is started
    Then mysql host "mysql3" should become available within "20" seconds
    And mysql host "mysql3" should become replica of "mysql2" within "10" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "3" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql3" should run fine within "3" seconds
    And mysql host "mysql3" should be read only

    Examples:
      | failover |
      | true     |
      | false    |

  Scenario: switchover from works with dead master
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And I wait for "20" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I run command on host "mysql2"
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
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "mysql1",
        "result": {
          "ok": true
        }
      }

      """
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
    And mysql host "{{.new_master}}" should be writable
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_slave_enabled" set to "0"

    When I run SQL on mysql host "{{.new_master}}"
      """
      SHOW SLAVE HOSTS
      """
    And I save SQL result as "slaves"
    And I save "{{ (index .slaves 0).Host }}" as "old_slave"
    Then mysql host "{{.old_slave}}" should be replica of "{{.new_master}}"
    And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "{{.old_slave}}" should run fine within "3" seconds
    And mysql host "{{.old_slave}}" should be read only

    And host "mysql1" is started
    Then mysql host "mysql1" should become available within "20" seconds
    And mysql host "mysql1" should become replica of "{{.new_master}}" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "10" seconds
    And mysql host "mysql1" should be read only
