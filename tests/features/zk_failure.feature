Feature: mysync handles zookeeper lost

  Scenario: mysync handles single rack network issue that could lead to split brain
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should be writable
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"

    And I run heavy user requests on host "mysql1" for "300" seconds
    When host "mysql1" is detached from the network
    When I wait for "60" seconds
    Then I have no SQL execution error at mysql host "mysql2" within "3" seconds
    And I have no SQL execution error at mysql host "mysql3" within "3" seconds
    When I run command on host "mysql1"
        """
           mysql -s --skip-column-names -e "SELECT (CASE WHEN count(*) = 0 THEN 'OK' ELSE 'HAS_USER_QUERIES' END)
                            FROM information_schema.PROCESSLIST p
                            WHERE USER NOT IN ('admin', 'monitor', 'event_scheduler', 'repl')"
        """
    Then command output should match regexp
        """
        .*OK.*
        """

    When I run command on host "mysql1"
        """
           mysql -s --skip-column-names -e "SELECT
                (CASE WHEN @@read_only = 1 THEN 'RO' ELSE 'RW' END) as RO,
                (CASE WHEN @@super_read_only = 1 THEN 'RO' ELSE 'RW' END) as SRO"
        """
    Then command output should match regexp
        """
        .*RO[[:space:]]+RO.*
        """


  Scenario: mysync handles ZK failure - do nothing
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should be writable
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"

    When I run heavy user requests on host "mysql1" for "300" seconds
    And I run long read user requests on host "mysql2" for "300" seconds
    And I run long read user requests on host "mysql3" for "300" seconds

    And host "zoo3" is detached from the network
    And host "zoo2" is detached from the network
    And host "zoo1" is detached from the network
    Then I have no SQL execution error at mysql host "mysql1" within "60" seconds
    And I have no SQL execution error at mysql host "mysql2" within "0" seconds
    And I have no SQL execution error at mysql host "mysql3" within "0" seconds
    And mysql host "mysql1" should be writable
    And mysql host "mysql2" should be read only
    And mysql host "mysql3" should be read only


  Scenario: failover works when old muster is stuck waiting semisync ack
    Given cluster environment is
    """
    MYSYNC_FAILOVER=false
    MYSYNC_SEMISYNC=true
    MYSYNC_DB_LOST_CHECK_TIMEOUT=3s
    """
    Given cluster is up and running
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """

    Then mysql host "mysql1" should be master
    And mysql host "mysql1" should become writable within "5" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"

    When I run SQL on mysql host "mysql1"
    """
        CREATE TABLE IF NOT EXISTS mysql.test_table1 (
            value VARCHAR(30)
        )
    """
    And I run SQL on mysql host "mysql1"
    """
        INSERT INTO mysql.test_table1 VALUES ("A"), ("B"), ("C")
    """

    When host "mysql1" is detached from the network
    Then mysql host "mysql1" should become unavailable within "10" seconds
    # following request should stuck in 'Waiting for semi-sync ACK from slave' state
    When I run async command on host "mysql1"
    """
       mysql -s --skip-column-names -e "INSERT INTO mysql.test_table1 VALUES ('D'), ('E'), ('F')"
    """
    And I wait for "5" seconds
    When I run command on host "mysql1"
    """
       mysql -s --skip-column-names -e "SELECT state FROM information_schema.PROCESSLIST"
    """
    Then command output should match regexp
    """
        .*Waiting for semi-sync ACK from slave.*
    """
    # start manual deterministic switchover - we will use this in last check
    When I run command on host "mysql2"
    """
        mysync switch --from mysql1 --wait=0s
    """
    Then zookeeper node "/test/last_switch" should match json within "90" seconds
    """
          {
            "from": "mysql1",
            "result": {
              "ok": true
            }
          }
    """
    And I run command on host "mysql1" until result match regexp ".*OK.*" with timeout "90" seconds
    """
           mysql -s --skip-column-names -e "SELECT (CASE WHEN count(*) = 0 THEN 'OK' ELSE 'STILL_WAITING' END)
                            FROM information_schema.PROCESSLIST
                            WHERE state = 'Waiting for semi-sync ACK from slave'"
    """
    When host "mysql1" is attached to the network
    Then mysql host "mysql1" should become available within "20" seconds
    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "new_master"
    Then mysql host "mysql1" should become replica of "{{.new_master}}" within "30" seconds
