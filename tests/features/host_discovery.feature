Feature: mysync use keys in zk properly

  Scenario: dynamic host resolve over zookeeper works
    Given cluster is up and running
    When I run command on host "mysql1"
        """
        mysync info
        """
    Then command return code should be "0"
    When zookeeper node "/test/ha_nodes/mysql1" should match regexp within "10" seconds
        """
        """
    When zookeeper node "/test/ha_nodes/mysql2" should match regexp within "10" seconds
        """
        """
    When zookeeper node "/test/ha_nodes/mysql3" should match regexp within "10" seconds
        """
        """
    When host "mysql2" is deleted
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql3"]
      """
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2
      """
    Then command return code should be "1"
    And command output should match regexp
      """
      .*no HA-nodes matching 'mysql2'.*
      """

  Scenario: mysync does not perform changes on not HA nodes
    Given cluster environment is
      """
      MYSYNC_FAILOVER=true
      """
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    When I change replication source on host "mysql3" to "mysql1"
    Then zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
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
    And zookeeper node "/test/master" should match regexp
    """
      .*mysql2.*
    """
    And mysql host "mysql3" should be replica of "mysql1"
    When host "mysql2" is stopped
    Then mysql host "mysql2" should become unavailable within "10" seconds
    And mysql host "mysql3" should be replica of "mysql1"
