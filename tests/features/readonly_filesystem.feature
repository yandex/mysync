Feature: readonly filesystem
  Scenario: check master failure when disk on muster become readonly
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

    When I set readonly file system on host "mysql1" to "true"
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

    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "{{.new_master}}" should be master
