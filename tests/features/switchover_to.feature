Feature: manual switchover to new master


  Scenario Outline: switchover on kill all running query on old master
    Given cluster environment is
    """
    FORCE_SWITCHOVER=<force_switchover>
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql replication on host "mysql3" should run fine within "5" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
       ["mysql1","mysql2","mysql3"]
    """
    When I run heavy user requests on host "mysql1" for "3600" seconds
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/switch" should match json
      """
      {
        "from": "",
        "to": "mysql2"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "120" seconds
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
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "3" seconds
    And mysql host "mysql1" should be read only
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql3" should run fine within "3" seconds
    And mysql host "mysql3" should be read only
    Examples:
      | force_switchover  |
      | true              |
      | false             |

  Scenario Outline: switchover to works on healthy cluster
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
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/switch" should match json
      """
      {
        "from": "",
        "to": "mysql2"
      }
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
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "3" seconds
    And mysql host "mysql1" should be read only
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql3" should run fine within "3" seconds
    And mysql host "mysql3" should be read only

    Examples:
      | failover |
      | true     |
      | false    |

  Scenario Outline: switchover to works with dead slave
    Given cluster environment is
      """
      MYSYNC_FAILOVER=<failover>
      """
    Given cluster is up and running
    And host "mysql3" is stopped
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
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/switch" should match json
      """
      {
        "from": "",
        "to": "mysql2"
      }
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
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be writable
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


  Scenario: switchover to works with dead master
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And host "mysql1" is stopped
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
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/switch" should match json
      """
      {
        "from": "",
        "to": "mysql2"
      }
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
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should become unavailable within "10" seconds
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql3" should run fine within "3" seconds
    And mysql host "mysql3" should be read only

    When host "mysql1" is started
    Then mysql host "mysql1" should become available within "20" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql replication on host "mysql1" should run fine within "3" seconds
    And mysql host "mysql1" should be read only

  Scenario: switchover on lagging replica fails
    Given cluster environment is
    """
    MYSYNC_SEMISYNC=false
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql replication on host "mysql3" should run fine within "5" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    When I break replication on host "mysql2"
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql3"]
    """
    When I run command on host "mysql1"
    """
    mysync switch --to mysql2 --wait=0s
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    mysql2 is not active
    """
