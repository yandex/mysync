Feature: failover


  Background:
    Given cluster environment is
      """
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      """

    Scenario: failover works
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
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
      And zookeeper node "/test/recovery/mysql1" should exist
      And zookeeper node "/test/master" should match regexp
        """
          .*mysql[23].*
        """
      When I get zookeeper node "/test/master"
      And I save zookeeper query result as "new_master"
      Then mysql host "{{.new_master}}" should be master
      And mysql host "{{.new_master}}" should be writable
      And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_master_enabled" set to "1"
      And mysql host "{{.new_master}}" should have variable "rpl_semi_sync_slave_enabled" set to "0"

      When I run SQL on mysql host "{{.new_master}}"
        """
        SHOW REPLICAS
        """
      And I save SQL result as "slaves"
      And I save "{{ (index .slaves 0).Host }}" as "old_slave"
      Then mysql host "{{.old_slave}}" should be replica of "{{.new_master}}"
      And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_slave_enabled" set to "1"
      And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
      And mysql replication on host "{{.old_slave}}" should run fine within "3" seconds
      And mysql host "{{.old_slave}}" should be read only

      When host "mysql1" is started
      Then mysql host "mysql1" should become available within "20" seconds
      And mysql host "mysql1" should become replica of "{{.new_master}}" within "10" seconds
      And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
      And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0" within "10" seconds
      And mysql replication on host "mysql1" should run fine within "10" seconds
      And mysql host "mysql1" should be read only
      And zookeeper node "/test/recovery/mysql1" should not exist within "10" seconds

    Scenario: failover does not work in absence of quorum
      Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_COOLDOWN=0s
        """
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
      When I get zookeeper node "/test/manager"
      And I save zookeeper query result as "manager"
      When I get zookeeper node "/test/master"
      And I save zookeeper query result as "master"
      When mysql on host "mysql1" is killed
      And mysql on host "mysql2" is killed
      Then mysql host "mysql1" should become unavailable within "10" seconds
      And mysql host "mysql2" should become unavailable within "10" seconds
      # give a change to perform (actually not) failover
      When I wait for "30" seconds
      Then mysql host "mysql3" should be replica of "{{.master}}"
      And zookeeper node "/test/master" should match regexp
        """
          .*{{.master}}.*
        """
      And zookeeper node "/test/manager" should match regexp
        """
          .*{{.manager.hostname}}.*
        """
      When I run command on host "{{.manager.hostname}}"
        """
          grep failover /var/log/mysync.log
        """
      Then command output should match regexp
        """
        .*failover was not approved:.*no quorum.*
        """

    Scenario: failover works well with dynamic quorum
      Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_COOLDOWN=0s
        """
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
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
      And zookeeper node "/test/recovery/mysql1" should exist
      And zookeeper node "/test/master" should match regexp
        """
        .*mysql[23].*
        """
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql2","mysql3"]
        """
      When I get zookeeper node "/test/master"
      And I save zookeeper query result as "new_master"
      Then mysql host "{{.new_master}}" should be master
      And mysql host "{{.new_master}}" should be writable
      When I delete zookeeper node "/test/last_switch"
      When host "{{.new_master}}" is stopped
      Then mysql host "{{.new_master}}" should become unavailable within "10" seconds
      And zookeeper node "/test/last_switch" should exist within "30" seconds
      When I get zookeeper node "/test/master"
      And I save zookeeper query result as "new_master"
      Then mysql host "{{.new_master}}" should be master
      And mysql host "{{.new_master}}" should be writable

    Scenario: failover does not work until cooldown
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
      When host "mysql1" is stopped
      Then mysql host "mysql1" should become unavailable within "10" seconds
      And zookeeper node "/test/manager" should match regexp within "10" seconds
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
      And zookeeper node "/test/master" should match regexp
        """
          .*mysql[23].*
        """
      When I get zookeeper node "/test/master"
      And I save zookeeper query result as "new_master"
      Then mysql host "{{.new_master}}" should be master
      When I run SQL on mysql host "{{.new_master}}"
        """
        SHOW REPLICAS
        """
      And I save SQL result as "slaves"
      And I save "{{ (index .slaves 0).Host }}" as "old_slave"

      When host "mysql1" is started
      Then mysql host "mysql1" should become available within "20" seconds
      And mysql host "mysql1" should become replica of "{{.new_master}}" within "10" seconds

      When host "{{.new_master}}" is stopped
      Then mysql host "{{.new_master}}" should become unavailable within "10" seconds
      # give a change to perform (actually not) failover
      When I wait for "30" seconds
      Then mysql host "mysql1" should be replica of "{{.new_master}}"
      And mysql host "{{.old_slave}}" should be replica of "{{.new_master}}"
      And zookeeper node "/test/manager" should match regexp within "10" seconds
        """
          .*mysql.*
        """

      When I get zookeeper node "/test/manager"
      And I save zookeeper query result as "new_manager"
      And I run command on host "{{.new_manager.hostname}}"
        """
          grep ERROR /var/log/mysync.log
        """
      Then command output should match regexp
        """
        .*failover was not approved:.*cooldown.*
        """


    Scenario: failover honors failover_delay
      Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_DELAY=30s
        """
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
      When host "mysql1" is stopped
      Then mysql host "mysql1" should become unavailable within "10" seconds
      And zookeeper node "/test/manager" should match regexp within "10" seconds
        """
          .*mysql[23].*
        """
      When I wait for "10" seconds
      Then mysql host "mysql2" should be replica of "mysql1"
      Then mysql host "mysql3" should be replica of "mysql1"

      When I get zookeeper node "/test/manager"
      And I save zookeeper query result as "new_manager"
      And I run command on host "{{.new_manager.hostname}}"
        """
          grep ERROR /var/log/mysync.log
        """
      Then command output should match regexp
        """
        .*failover was not approved:.*delay is not yet elapsed.*
        """

      When I wait for "20" seconds
      Then zookeeper node "/test/last_switch" should match json within "20" seconds
        """
        {
          "cause": "auto",
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
        SHOW REPLICAS
        """
      And I save SQL result as "slaves"
      And I save "{{ (index .slaves 0).Host }}" as "old_slave"
      Then mysql host "{{.old_slave}}" should be replica of "{{.new_master}}"
      And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_slave_enabled" set to "1"
      And mysql host "{{.old_slave}}" should have variable "rpl_semi_sync_master_enabled" set to "0"
      And mysql replication on host "{{.old_slave}}" should run fine within "3" seconds
      And mysql host "{{.old_slave}}" should be read only


    Scenario: failover works for 2-node cluster
      Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_DELAY=0s
        MYSYNC_FAILOVER_COOLDOWN=0s
        """
      Given cluster is up and running
      When host "mysql3" is deleted
      Then mysql host "mysql3" should become unavailable within "10" seconds
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2"]
        """
      Then zookeeper node "/test/manager" should match regexp
        """
          .*mysql[12].*
        """
      And zookeeper node "/test/master" should match regexp
        """
          .*mysql1.*
        """
      When host "mysql1" is stopped
      Then mysql host "mysql1" should become unavailable within "10" seconds
      Then zookeeper node "/test/manager" should match regexp within "10" seconds
        """
          .*mysql2.*
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
      Then zookeeper node "/test/master" should match regexp within "30" seconds
        """
          .*mysql2.*
        """
      Then mysql host "mysql2" should be master
      And mysql host "mysql2" should be writable
      # should disable semi-sync as we are the only node in cluster
      And zookeeper node "/test/active_nodes" should match json_exactly within "10" seconds
        """
        ["mysql2"]
        """
      And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"


  Scenario: failover fails for 2-node cluster with lagging replica
    Given cluster environment is
    """
    MYSYNC_FAILOVER=true
    MYSYNC_FAILOVER_DELAY=0s
    MYSYNC_FAILOVER_COOLDOWN=0s
    """
    Given cluster is up and running
    When host "mysql3" is deleted
    Then mysql host "mysql3" should become unavailable within "10" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "10" seconds
      """
      ["mysql1","mysql2"]
      """
    And zookeeper node "/test/manager" should match regexp
    """
      .*mysql[12].*
    """
    And zookeeper node "/test/master" should match regexp
    """
      .*mysql1.*
    """

    When I run SQL on mysql host "mysql1"
    """
    CREATE TABLE mysql.test_table1 (
        value VARCHAR(30)
    ) ENGINE=INNODB;
    """
    When host "mysql2" is stopped
    Then mysql host "mysql2" should become unavailable within "10" seconds
    Then mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0" within "20" seconds
    And mysql host "mysql1" should be writable
    And mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "10" seconds
      """
      ["mysql1"]
      """

    # now we have 2 nodes cluster with unavailable replica, we starting insert some data
    When I run SQL on mysql host "mysql1"
    """
      INSERT INTO mysql.test_table1 (value) VALUES('Hello!It`s me, data loss!)');
    """

    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds

    When host "mysql2" is started
    Then mysql host "mysql2" should become available within "10" seconds
    Then zookeeper node "/test/manager" should match regexp within "10" seconds
    """
      .*mysql2.*
    """
    Then zookeeper node "/test/master" should match regexp
    """
      .*mysql1.*
    """
    When I wait for "30" seconds
    When I run command on host "mysql2"
    """
      grep failover /var/log/mysync.log
    """
    Then command output should match regexp
    """
      .*failover was not approved: no quorum.*
    """


  Scenario: failover works with cascade replicas
    Given cluster environment is
    """
    MYSYNC_FAILOVER=true
    MYSYNC_FAILOVER_DELAY=0s
    MYSYNC_FAILOVER_COOLDOWN=0s
    """
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    When I run command on host "mysql3"
    """
    mysync host add mysql3 --stream-from mysql2
    """
    Then command return code should be "0"
    And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
    """
    ["mysql1","mysql2"]
    """

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
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable

