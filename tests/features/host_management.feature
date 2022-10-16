Feature: update host topology using CLI

  Scenario: removing and adding HA host via CLI works
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
    When I run command on host "mysql1"
        """
        mysync host remove mysql3
        """
    Then command return code should be "1"
    When host "mysql3" is stopped
    When I run command on host "mysql1"
        """
        mysync host remove mysql3
        """
    Then command return code should be "0"
    Then zookeeper node "/test/ha_nodes/mysql3" should not exist
    When I run command on host "mysql1"
        """
        mysync host add mysql3
        """
    Then command return code should be "1"
    When host "mysql3" is started
    Then mysql host "mysql3" should become available within "20" seconds
    When I run command on host "mysql1"
        """
        mysync host add mysql3
        """
    Then command return code should be "0"
    Then zookeeper node "/test/ha_nodes/mysql3" should exist

  Scenario: removing and adding HA host via CLI works
    Given cluster is up and running
    Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
    When I run command on host "mysql3"
        """
        mysync host add mysql2 --stream-from mysql1
        """
    Then command return code should be "0"
    Then zookeeper node "/test/ha_nodes/mysql2" should not exist
    Then zookeeper node "/test/cascade_nodes/mysql2" should exist

    # loops are forbiden:
    When I run command on host "mysql1"
        """
        mysync host add mysql2 --stream-from mysql2
        """
    Then command return code should be "1"
    # loops are forbiden:
    When I run command on host "mysql1"
        """
        mysync host add mysql1 --stream-from mysql2
        """
    Then command return code should be "1"
    # master cannot be cascade:
    When I run command on host "mysql1"
        """
        mysync host add mysql1 --stream-from mysql3
        """
    Then command return code should be "1"
