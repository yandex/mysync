Feature: loading binlogs tests

  Scenario: repl_mon enabled
    Given cluster environment is
      """
      REPL_MON=true
      """
    Given cluster is up and running
    When I wait for "10" seconds
    Then zookeeper node "/test/health/mysql2" should match json within "20" seconds
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
    Then zookeeper node "/test/health/mysql2" should match json within "20" seconds
      """
      {
        "is_loading_binlog": false
      }
      """

