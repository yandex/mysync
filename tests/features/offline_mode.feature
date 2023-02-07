Feature: offline mode for lagging replicas

  Scenario: mysync switches replicas to offline mode and back when slave lags
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"

    Then mysql host "mysql1" should be online within "10" seconds
    And mysql host "mysql2" should be online within "10" seconds
    And mysql host "mysql3" should be online within "10" seconds

    #When I break replication on host "mysql3"
    And I run SQL on mysql host "mysql3"
        """
            STOP SLAVE; CHANGE MASTER TO MASTER_DELAY = 300 FOR CHANNEL ''; START SLAVE
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
    Then mysql host "mysql3" should be offline within "15" seconds
    When I run SQL on mysql host "mysql3"
        """
            STOP SLAVE; CHANGE MASTER TO MASTER_DELAY = 0 FOR CHANNEL ''; START SLAVE
        """
    Then mysql host "mysql3" should be online within "10" seconds
