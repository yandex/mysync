Feature: maintenance mode

  Scenario: mysync maintenance control via zookeeper
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I set zookeeper node "/test/maintenance" to
      """
      {
        "initiated_by": "test"
      }
      """
    Then zookeeper node "/test/maintenance" should match json within "30" seconds
      """
      {
        "mysync_paused": true
      }
      """
    And zookeeper node "/test/active_nodes" should not exist
    When I set zookeeper node "/test/maintenance" to
      """
      {
        "initiated_by": "test",
        "should_leave": true
      }
      """
    Then zookeeper node "/test/maintenance" should not exist within "30" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I set zookeeper node "/test/maintenance" to
      """
      {
        "initiated_by": "test"
      }
      """
    Then zookeeper node "/test/maintenance" should match json within "30" seconds
      """
      {
        "mysync_paused": true
      }
      """
    And zookeeper node "/test/active_nodes" should not exist
    When I delete zookeeper node "/test/maintenance"
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """

  Scenario: mysync disables SemySync replication in maintenance mode
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I run SQL on mysql host "mysql2"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "aaaa":"0",
          "bbbb":"1",
          "SlaveEnabled":"1"
      }]
      """
    When I run SQL on mysql host "mysql1"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "MasterEnabled":"1",
          "SlaveEnabled":"0"
      }]
      """
    When I run command on host "mysql1"
      """
      mysync maint on
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      maintenance enabled
      """
    And I wait for "5" seconds
    And zookeeper node "/test/maintenance" should match json
      """
      {
        "mysync_paused": true
      }
      """
    And zookeeper node "/test/active_nodes" should not exist
    When I run SQL on mysql host "mysql2"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "MasterEnabled":"0",
          "SlaveEnabled":"1"
      }]
      """
    When I run SQL on mysql host "mysql1"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "MasterEnabled":"0",
          "SlaveEnabled":"0"
      }]
      """
    When I run command on host "mysql1"
      """
      mysync maint off
      """
    Then command return code should be "0"
    And zookeeper node "/test/maintenance" should not exist
    And zookeeper node "/test/active_nodes" should match json_exactly
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I run SQL on mysql host "mysql2"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "MasterEnabled":"0",
          "SlaveEnabled":"1"
      }]
      """
    When I run SQL on mysql host "mysql1"
      """
      SELECT @@rpl_semi_sync_master_enabled AS MasterEnabled, @@rpl_semi_sync_slave_enabled AS SlaveEnabled;
      """
    Then SQL result should match regexp
      """
      [{
          "MasterEnabled":"1",
          "SlaveEnabled":"0"
      }]
      """

  Scenario: master host in DCS updated correctly after manual master change
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    Then zookeeper node "/test/master" should match json
      """
      "mysql1"
      """
    When I run command on host "mysql1"
      """
      mysync maint on
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      maintenance enabled
      """
    And I wait for "5" seconds
    And zookeeper node "/test/maintenance" should match json
      """
      {
        "initiated_by": "mysql1"
      }
      """
    When I run command on host "mysql1"
      """
      mysync maint get
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      on
      """
    When I run SQL on mysql host "mysql1"
    """
      SET GLOBAL super_read_only = 1
    """
    And I run SQL on mysql host "mysql1"
    """
      CHANGE MASTER TO master_user = 'repl', master_password = 'repl_pwd', master_host = 'mysql2', master_auto_position = 1  FOR CHANNEL ''
    """
    And I run SQL on mysql host "mysql1"
    """
      START SLAVE
    """
    And I run SQL on mysql host "mysql3"
    """
      STOP SLAVE
    """
    And I run SQL on mysql host "mysql3"
    """
      CHANGE MASTER TO master_user = 'repl', master_password = 'repl_pwd', master_host = 'mysql2', master_auto_position = 1 FOR CHANNEL ''
    """
    And I run SQL on mysql host "mysql3"
    """
      START SLAVE
    """
    And I run SQL on mysql host "mysql2"
    """
      STOP SLAVE
    """
    And I run SQL on mysql host "mysql2"
    """
      RESET SLAVE ALL
    """
    And I run SQL on mysql host "mysql2"
    """
      SET GLOBAL read_only = 0
    """
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql1" should be read only
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql host "mysql3" should be read only
    And zookeeper node "/test/master" should match json
      """
      "mysql1"
      """
    When I run command on host "mysql1"
      """
      mysync maint off
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      maintenance disabled
      """
    Then zookeeper node "/test/master" should match json within "15" seconds
      """
      "mysql2"
      """
    When I run command on host "mysql1"
      """
      mysync maint get
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      off
      """
