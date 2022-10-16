Feature: host priority

    Scenario: CLI works
        Given cluster is up and running
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2","mysql3"]
        """

        When I run command on host "mysql3"
        """
           mysync host add mysql2 --priority 5
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql2" should match json within "5" seconds
        """
           { "priority": 5 }
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql3 --priority 10
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql3" should match json within "5" seconds
        """
           { "priority": 10 }
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql3 --priority -10
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        .*priority must be >= 0.*
        """

    Scenario Outline: Switchover chooses replica with greater priority
        And cluster is up and running
        Then mysql host "mysql1" should be master

        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2","mysql3"]
        """

        When I run command on host "mysql3"
        """
           mysync host add <host> --priority <priority>
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/<host>" should match json within "5" seconds
        """
           { "priority": <priority> }
        """
        When I run command on host "mysql2"
        """
        mysync switch --from mysql1 --wait=0s
        """
        Then command return code should be "0"
        Then zookeeper node "/test/last_switch" should match json within "30" seconds
        """
        {
            "from": "mysql1",
            "result": {
            "ok": true
            }
        }
        """
        Then mysql host "<new_master>" should be master
        And mysql host "<new_master>" should be writable
        Examples:
            | priority | host   | new_master | 
            | 10       | mysql2 | mysql2     | 
            | 10       | mysql3 | mysql3     | 

    Scenario: Switchover ignores cascade replica
        Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_DELAY=1s
        MYSYNC_STREAM_FROM_REASONABLE_LAG=1s
        """
        And cluster is up and running
        Then mysql host "mysql1" should be master

        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2","mysql3"]
        """

        When I run command on host "mysql3"
        """
           mysync host add mysql2 --priority 5
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql2" should match json within "5" seconds
        """
           { "priority": 5 }
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql3 --priority 10
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql3" should match json within "5" seconds
        """
           { "priority": 10 }
        """
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql1
        """
        Then command return code should be "0"
        Then zookeeper node "/test/cascade_nodes/mysql3" should match json within "5" seconds
        """
            { "stream_from": "mysql1" }
        """
        And zookeeper node "/test/active_nodes" should match json_exactly within "10" seconds
        """
           ["mysql1","mysql2"]
        """
        When I run command on host "mysql2"
        """
        mysync switch --from mysql1 --wait=0s
        """
        Then command return code should be "0"
        Then zookeeper node "/test/last_switch" should match json within "30" seconds
        """
        {
            "from": "mysql1",
            "result": {
            "ok": true
            }
        }
        """
        Then mysql host "mysql2" should be master
        And mysql host "mysql2" should be writable
