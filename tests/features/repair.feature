Feature: repair hosts in cluster

  Scenario: master became writable after manual changes
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should be writable
    When I run SQL on mysql host "mysql1"
    """
    SET GLOBAL READ_ONLY=1;
    """
    Then mysql host "mysql1" should become writable within "20" seconds
    When I run SQL on mysql host "mysql1"
    """
    SET GLOBAL SUPER_READ_ONLY=1;
    """
    Then mysql host "mysql1" should become writable within "20" seconds

  Scenario: async replication fallback
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"
    When host "mysql2" is detached from the network
    And host "mysql3" is detached from the network
    Then mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "0" within "30" seconds
    And mysql host "mysql1" should be writable
    And mysql host "mysql1" should be master

  Scenario: semisync replication setup
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_master_enabled" set to "0"

  Scenario: master became writable again after network loss
    Given cluster environment is
      """
      MYSYNC_FAILOVER=false
      """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    When host "mysql1" is detached from the network
    Then mysql host "mysql1" should become unavailable within "10" seconds
    When host "mysql1" is attached to the network
    Then mysql host "mysql1" should become available within "20" seconds
    And mysql host "mysql1" should be master
    And mysql host "mysql1" should become writable within "20" seconds

  Scenario: master became writable if zookeeper was unavailable during first run
    Given cluster is up and running
    And host "mysql3" is stopped
    And host "mysql2" is stopped
    And host "mysql1" is stopped
    And I delete zookeeper node "/test/master"
    And host "zoo1" is detached from the network
    And host "zoo2" is detached from the network
    And host "zoo3" is detached from the network
    When host "mysql1" is started
    And host "mysql2" is started
    And host "mysql3" is started
    Then mysql host "mysql1" should become available within "20" seconds
    And mysql host "mysql1" should be master
    And mysql host "mysql1" should be read only
    When host "zoo1" is attached to the network
    And host "zoo2" is attached to the network
    And host "zoo3" is attached to the network
    Then mysql host "mysql1" should become writable within "20" seconds

  Scenario: mysync repairs replication on replication error
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should be writable
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    # just to have stable tests - turn on maintenance mode
    And I run command on host "mysql1"
    """
      mysync maint on
    """
    And I wait for "5" seconds
    When I break replication on host "mysql2" in repairable way
    Then mysql replication on host "mysql2" should not run fine
    And I run command on host "mysql1"
    """
      mysync maint off
    """
    And mysql replication on host "mysql2" should run fine within "60" seconds

  Scenario: mysync repairs unrepairable replication in aggressive mode
    Given cluster environment is
    """
    MYSYNC_REPLICATION_REPAIR_AGGRESSIVE_MODE=true
    """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should be writable
    # just to have stable tests - turn on maintenance mode
    And I run command on host "mysql1"
    """
      mysync maint on
    """
    And I wait for "5" seconds
    When I break replication on host "mysql2"
    Then mysql replication on host "mysql2" should not run fine
    And I run command on host "mysql1"
    """
      mysync maint off
    """
    And mysql replication on host "mysql2" should run fine within "60" seconds
