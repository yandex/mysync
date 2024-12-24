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
