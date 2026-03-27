Feature: offline mode AZ limit

  Scenario: with pct=50 only one of two replicas goes offline when both lag
    Given cluster environment is
        """
        OFFLINE_MODE_ENABLE_LAG=5s
        OFFLINE_MODE_MAX_OFFLINE_PCT=50
        """
    And cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql1" should be online within "10" seconds
    And mysql host "mysql2" should be online within "10" seconds
    And mysql host "mysql3" should be online within "10" seconds

    When I run SQL on mysql host "mysql2"
        """
            STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 300 FOR CHANNEL ''; START REPLICA
        """
    And I run SQL on mysql host "mysql3"
        """
            STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 300 FOR CHANNEL ''; START REPLICA
        """
    And I run SQL on mysql host "mysql1"
        """
            CREATE TABLE IF NOT EXISTS mysql.test_table1 (
                value VARCHAR(30)
            )
        """
    And I run SQL on mysql host "mysql1"
        """
            INSERT INTO mysql.test_table1 VALUES ("A"), ("B"), ("C")
        """

    # Exactly one replica must go offline
    And 1 mysql replicas should be online within "15" seconds

    When I run SQL on mysql host "mysql2"
        """
            STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0 FOR CHANNEL ''; START REPLICA
        """
    And I run SQL on mysql host "mysql3"
        """
            STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0 FOR CHANNEL ''; START REPLICA
        """
    Then mysql host "mysql2" should be online within "15" seconds
    And mysql host "mysql3" should be online within "15" seconds
