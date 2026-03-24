Feature: light maintenance mode behaviour

    Scenario: Cascade replicas switches between HA-nodes during light maintenance mode
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

        When I set zookeeper node "/test/maintenance" to
        """
        {
            "initiated_by": "test",
            "mode": "light"
        }
        """
        Then zookeeper node "/test/maintenance" should match json within "30" seconds
        """
        {
            "initiated_by": "test",
            "mode": "light"
        }
        """

        # configure cluster:
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql2
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql1" should exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql2" should exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql3" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql1" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql2" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql3" should match json within "5" seconds
        """
            { "stream_from": "mysql2" }
        """
        And mysql host "mysql2" should become replica of "mysql1" within "30" seconds
        And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
        And mysql host "mysql3" should become replica of "mysql2" within "30" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds

        # remove master node:
        When host "mysql2" is stopped
        Then mysql host "mysql2" should become unavailable within "10" seconds
        Then mysql host "mysql3" should become replica of "mysql1" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

        # return master node:
        When host "mysql2" is started
        Then mysql host "mysql3" should become replica of "mysql2" within "90" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

    Scenario: failover during light maintenance doesn't work
        Given cluster is up and running
        Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
        When I set zookeeper node "/test/maintenance" to
        """
        {
            "initiated_by": "test",
            "mode": "light"
        }
        """
        Then zookeeper node "/test/maintenance" should match json within "30" seconds
        """
        {
            "initiated_by": "test",
            "mode": "light"
        }
        """
        When host "mysql1" is stopped
        Then mysql host "mysql1" should become unavailable within "10" seconds
        Then zookeeper node "/test/manager" should match regexp within "10" seconds
        """
            .*mysql[23].*
        """

        And zookeeper node "/test/master" should match regexp
        """
            .*mysql1.*
        """
        When host "mysql1" is started
        Then mysql host "mysql1" should become available within "20" seconds
        # Wait some time to avoid tcp connection errors with master
        When I wait for "15" seconds
        Then mysql host "mysql1" should be master
        And mysql host "mysql1" should be writable
        And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1"
        And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"

    Scenario: mysync stays enabled in light maintenance mode
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql host "mysql3" should be replica of "mysql1"
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
        And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1"
        And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
        When I set zookeeper node "/test/maintenance" to
        """
        {
        "initiated_by": "test",
        "mode": "light"
        }
        """
        Then zookeeper node "/test/maintenance" should match json within "30" seconds
        """
        {
        "initiated_by": "test",
        "mode": "light"
        }
        """
        And zookeeper node "/test/active_nodes" should match json_exactly
        """
        ["mysql1","mysql2","mysql3"]
        """
        And mysql host "mysql1" should have variable "rpl_semi_sync_master_enabled" set to "1"
        And mysql host "mysql1" should have variable "rpl_semi_sync_slave_enabled" set to "0"
        When I delete zookeeper node "/test/maintenance"
        Then zookeeper node "/test/maintenance" should not exist within "30" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly
        """
        ["mysql1","mysql2","mysql3"]
        """

    Scenario: switchover is rejected in light maintenance mode
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
        When I set zookeeper node "/test/maintenance" to
        """
        {
        "initiated_by": "test",
        "mode": "light"
        }
        """

        # Expecting mysync_paused field to be empty(false), because in light mode
        # we don't deactivate it

        Then zookeeper node "/test/maintenance" should match json within "30" seconds
        """
        {
        "initiated_by": "test",
        "mode": "light"
        }
        """
        When I run command on host "mysql1"
        """
        mysync switch --to mysql2 --wait=0s
        """
        Then command return code should be "0"

        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
