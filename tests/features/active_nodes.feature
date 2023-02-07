Feature: mysync saves quorum hosts in zk

  Scenario: active nodes works with add/delete hosts
    Given cluster environment is
    """
    MYSYNC_FAILOVER=true
    MYSYNC_FAILOVER_DELAY=30s
    MYSYNC_FAILOVER_COOLDOWN=0s
    """
    Given cluster is up and running
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
    Then zookeeper node "/test/master" should match regexp within "40" seconds
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
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql2","mysql3"]
    """
    When host "mysql1" is started
    Then mysql host "mysql1" should become available within "10" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """

  Scenario: active nodes works with splitbrain
    Given cluster environment is
    """
    MYSYNC_FAILOVER=true
    MYSYNC_FAILOVER_DELAY=0s
    MYSYNC_FAILOVER_COOLDOWN=0s
    """
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And zookeeper node "/test/master" should match regexp
    """
      .*mysql1.*
    """
    When I run command on host "mysql1"
    """
    mysync maint on
    """
    Then command return code should be "0"
    When I run SQL on mysql host "mysql2"
    """
    SET GLOBAL READ_ONLY=0;
    """
    When I run SQL on mysql host "mysql2"
    """
    CREATE TABLE mysql.test_table1 (
        value VARCHAR(30)
    ) ENGINE=INNODB;
    """
    When I run command on host "mysql1"
    """
    mysync maint off
    """
    Then command return code should be "0"
    When I wait for "10" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql3"]
    """
 
  Scenario: active nodes works with broken replication
    Given cluster environment is
    """
    MYSYNC_FAILOVER=true
    MYSYNC_FAILOVER_DELAY=0s
    MYSYNC_FAILOVER_COOLDOWN=0s
    """
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And zookeeper node "/test/master" should match regexp
    """
    .*mysql1.*
    """
    When I break replication on host "mysql2"
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql3"]
    """
 
  Scenario: active nodes honors inactivation timeout
    Given cluster environment is
    """
    MYSYNC_INACTIVATION_DELAY=40s
    """
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    And zookeeper node "/test/master" should match regexp
    """
    .*mysql1.*
    """
    When host "mysql2" is detached from the network
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    When I wait for "30" seconds
    Then zookeeper node "/test/active_nodes" should match json_exactly
    """
    ["mysql1","mysql2","mysql3"]
    """
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql3"]
    """
    When host "mysql2" is attached to the network
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
