Feature: failover and switchover timings are logged via log_timing command

  Scenario: failover logs downtime and failover timings
    Given cluster environment is
      """
      MYSYNC_FAILOVER=true
      MYSYNC_FAILOVER_DELAY=0s
      """
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When host "mysql1" is stopped
    Then mysql host "mysql1" should become unavailable within "10" seconds
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "cause": "auto",
        "from": "mysql1",
        "master_transition": "failover",
        "result": {
          "ok": true
        }
      }
      """
    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"
    When I run command on host "{{.manager.hostname}}" until result match regexp "failover:" with timeout "30" seconds
      """
      cat /tmp/timing.log
      """
    Then command output should match regexp
      """
      failover:
      """
    And command output should match regexp
      """
      downtime:
      """

  Scenario: switchover logs downtime and switchover timings
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    Then zookeeper node "/test/last_switch" should match json within "120" seconds
      """
      {
        "to": "mysql2",
        "master_transition": "switchover",
        "result": {
          "ok": true
        }
      }
      """
    Then mysql host "mysql2" should be master
    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"
    When I run command on host "{{.manager.hostname}}" until result match regexp "switchover:" with timeout "30" seconds
      """
      cat /tmp/timing.log
      """
    Then command output should match regexp
      """
      switchover:
      """
    And command output should match regexp
      """
      downtime:
      """

  Scenario: switchover issued externally without master_transition logs switchover timing
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I set zookeeper node "/test/switch" to
      """
      {
          "from": "mysql1",
          "to": "mysql2",
          "cause": "manual",
          "initiated_by": "worker"
      }
      """
    Then zookeeper node "/test/last_switch" should match json within "120" seconds
      """
      {
          "to": "mysql2",
          "result": {
              "ok": true
          }
      }
      """
    Then mysql host "mysql2" should be master
    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"
    When I run command on host "{{.manager.hostname}}" until result match regexp "switchover:" with timeout "30" seconds
      """
      cat /tmp/timing.log
      """
    Then command output should match regexp
      """
      switchover:
      """
    And command output should match regexp
      """
      downtime:
      """
