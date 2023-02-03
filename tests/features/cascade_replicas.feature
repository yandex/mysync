Feature: cascade replicas

    Scenario: Cascade replicas chooses best suitable host from the chain to replicate from
        Given cluster environment is
        """
        MYSYNC_FAILOVER_DELAY=10s
        MYSYNC_STREAM_FROM_REASONABLE_LAG=1s
        """
        And cluster is up and running
        Then mysql host "mysql1" should be master

        When I wait for "10" seconds
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2","mysql3"]
        """

        # configure cluster:
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from mysql1
        """
        Then command return code should be "0"
        When I run command on host "mysql3"
        """
           mysync host add mysql3 --stream-from mysql2
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql1" should exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql2" should not exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql3" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql1" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql2" should match json within "5" seconds
        """
           { "stream_from": "mysql1" }
        """
        Then zookeeper node "/test/cascade_nodes/mysql3" should match json within "5" seconds
        """
           { "stream_from": "mysql2" }
        """
        And mysql host "mysql2" should become replica of "mysql1" within "30" seconds
        And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql host "mysql3" should become replica of "mysql2" within "30" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds

        # remove intermediate node:
        When host "mysql2" is detached from the network
        Then mysql host "mysql3" should become replica of "mysql1" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

        # return intermediate node:
        When host "mysql2" is attached to the network
        Then mysql host "mysql3" should become replica of "mysql2" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds


    Scenario: Cascade replicas switches between HA-nodes
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

        # configure cluster:
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql1
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql1" should exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql2" should exist within "5" seconds
        Then zookeeper node "/test/ha_nodes/mysql3" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql1" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql2" should not exist within "5" seconds
        Then zookeeper node "/test/cascade_nodes/mysql3" should match json within "5" seconds
        """
            { "stream_from": "mysql1" }
        """
        And mysql host "mysql2" should become replica of "mysql1" within "30" seconds
        And mysql host "mysql2" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
        And mysql host "mysql3" should become replica of "mysql1" within "30" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds

        # remove master node:
        When host "mysql1" is stopped
        Then mysql host "mysql1" should become unavailable within "10" seconds
        Then mysql host "mysql3" should become replica of "mysql2" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

        # return master node:
        When host "mysql1" is started
        Then mysql host "mysql3" should become replica of "mysql1" within "90" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

    Scenario: CLI works
        Given cluster environment is
        """
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

        # setup cascade replica
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql2
        """
        Then command return code should be "0"
        Then mysql host "mysql3" should become replica of "mysql2" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds

        # change stream_from
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql1
        """
        Then command return code should be "0"
        Then mysql host "mysql3" should become replica of "mysql1" within "90" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2"]
        """

        # remove host from cascade nodes
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from ""
        """
        Then command return code should be "0"
        Then mysql host "mysql3" should become replica of "mysql1" within "90" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "1" within "10" seconds
        And mysql replication on host "mysql3" should run fine within "15" seconds
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2","mysql3"]
        """

    Scenario: Cascade replica does not switch from laggy replica
        Given cluster environment is
        """
        MYSYNC_FAILOVER_DELAY=10s
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
           mysync host add mysql3 --stream-from mysql2
        """
        Then command return code should be "0"
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2"]
        """
        And mysql host "mysql3" should be replica of "mysql2"
        When I run SQL on mysql host "mysql2"
        """
            STOP SLAVE; CHANGE MASTER TO MASTER_DELAY = 10 FOR CHANNEL 'test_channel'; START SLAVE
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
        When I wait for "10" seconds
        And mysql replication on host "mysql2" should run fine
        And mysql host "mysql3" should be replica of "mysql2"