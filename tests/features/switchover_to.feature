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
        "to": "mysql2",
        "master_transition": "switchover"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "120" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "master_transition": "switchover",
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
        "to": "mysql2",
        "master_transition": "switchover"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "master_transition": "switchover",
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
    # Regression: new source must not be stuck waiting for a semi-sync ACK after switchover.
    # Checks: both replicas connected (clients=2), master not degraded (status=ON), no stuck processes.
    When I run SQL on mysql host "mysql2"
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
    And I have no SQL execution error at mysql host "mysql2" within "5" seconds

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
        "to": "mysql2",
        "master_transition": "switchover"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "master_transition": "switchover",
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


  Scenario: failover to works with dead master
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
      mysync switch --to mysql2 --wait=0s --failover
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
        "to": "mysql2",
        "master_transition": "failover"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "master_transition": "failover",
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

  Scenario: switchover to does not work with dead master
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
        "to": "mysql2",
        "master_transition": "switchover"
      }
      """
    Then zookeeper node "/test/last_rejected_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "master_transition": "switchover",
        "result": {
          "ok": false,
          "error": "switchover: failed to set old master mysql1 read-only switchover: failed to ping host mysql1"
        }
      }
      """

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

  Scenario: timeout survives manager restart and restores the old master
    Given cluster environment is
    """
    MYSYNC_SEMISYNC=false
    MYSYNC_SWITCHOVER_TIMEOUT=12s
    OFFLINE_MODE_ENABLE_LAG=300s
    """
    And cluster is up and running
    Then mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    When I set replication delay on host "mysql2" to "60" seconds
    And I run SQL on mysql host "mysql1"
    """
    CREATE TABLE IF NOT EXISTS mysql.switchover_timeout_test (id INT PRIMARY KEY)
    """
    And I run SQL on mysql host "mysql1"
    """
    INSERT INTO mysql.switchover_timeout_test VALUES (1)
    """
    And I wait for "3" seconds
    And I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"
    And I run command on host "mysql1"
    """
    mysync switch --to mysql2 --wait=0s
    """
    Then command return code should be "0"
    And zookeeper node "/test/switch" should match json within "10" seconds
    """
    {
      "to": "mysql2",
      "abortable": true,
      "started_at": "REGEXP:^20[0-9]{2}-"
    }
    """
    When I run command on host "{{.manager.hostname}}"
    """
    supervisorctl restart mysync
    """
    Then command return code should be "0"
    And zookeeper node "/test/last_rejected_switch" should match json within "30" seconds
    """
    {
      "to": "mysql2",
      "abortable": true,
      "result": {
        "ok": false,
        "error": "switchover timed out after 12s"
      }
    }
    """
    And zookeeper node "/test/switch" should not exist
    And mysql host "mysql1" should be master
    And mysql host "mysql1" should become writable within "30" seconds
    And mysql host "mysql2" should become replica of "mysql1" within "30" seconds
    And mysql host "mysql3" should become replica of "mysql1" within "30" seconds
    And mysql replication on host "mysql2" should run fine within "30" seconds
    And mysql replication on host "mysql3" should run fine within "30" seconds
    When I set replication delay on host "mysql2" to "0" seconds
    Then mysql replication on host "mysql2" should run fine within "30" seconds
