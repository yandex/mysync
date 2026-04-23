Feature: CLI

    Scenario: CLI initial add host works
        Given cluster is up and running with clean zk
        Then zookeeper node "/test/ha_nodes" should not exist

        When I run command on host "mysql3"
        """
           mysync host add mysql3 --skip-mysql-check
        """
        Then command return code should be "0"
        Then zookeeper node "/test/ha_nodes/mysql3" should exist within "5" seconds

    Scenario: setting priority on cascade replica does nothing
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
        And zookeeper node "/test/ha_nodes/mysql3" should exist within "30" seconds

        # setup cascade replica
        When I run command on host "mysql3"
        """
            mysync host add mysql3 --stream-from mysql2
        """
        Then command return code should be "0"
        Then mysql host "mysql3" should become replica of "mysql2" within "45" seconds
        And mysql host "mysql3" should have variable "rpl_semi_sync_slave_enabled" set to "0" within "10" seconds
        And mysql replication on host "mysql3" should run fine
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2"]
        """
        And zookeeper node "/test/ha_nodes/mysql3" should not exist within "30" seconds

        When I run command on host "mysql3"
        """
            mysync host add mysql3 --priority 5
        """
        Then command return code should be "0"
        And mysql replication on host "mysql3" should run fine
        And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
        """
           ["mysql1","mysql2"]
        """
        And zookeeper node "/test/ha_nodes/mysql3" should not exist within "30" seconds


    Scenario: CLI dry run works with priority
        Given cluster is up and running

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
           mysync host add mysql2 --priority 5 --dry-run
        """
        Then command return code should be "0"
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --priority 10 --dry-run
        """
        Then command return code should be "2"
        And command output should match regexp
        """
        .*node priority can be set to 10.*
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from mysql3 --dry-run
        """
        Then command return code should be "2"
        And command output should match regexp
        """
        .*replica can be set cascade.*
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from mysql3 --priority 5 --dry-run
        """
        Then command return code should be "2"
        And command output should match regexp
        """
        .*replica can be set cascade.*
        """
        And command output should match regexp
        """
        .*node already has priority 5 set.*
        """
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from mysql3
        """
        Then command return code should be "0"
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from mysql3 --priority 5 --dry-run
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        .*node is already streaming from mysql3.*
        """
        And command output should match regexp
        """
        .*node mysql2 is not HA node, priority cannot be set.*
        """
        Then zookeeper node "/test/cascade_nodes/mysql2" should exist within "5" seconds
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --priority 10
        """
        Then command return code should be "0"
        Then zookeeper node "/test/cascade_nodes/mysql2" should exist within "5" seconds
        When I run command on host "mysql3"
        """
           mysync host add mysql2 --stream-from ""
        """
        Then command return code should be "0"
        Then zookeeper node "/test/cascade_nodes/mysql2" should not exist within "5" seconds

    Scenario: CLI maintenance light mode writes to DCS and exits correctly
        Given cluster is up and running
        When I run command on host "mysql1"
        """
        mysync maint on --light
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        maintenance enabled
        """
        And zookeeper node "/test/maintenance" should match json within "30" seconds
        """
        {
          "initiated_by": "REGEXP:.*@mysql1",
          "mode": "light",
          "mysync_paused": true
        }
        """
        When I run command on host "mysql1"
        """
        mysync maint off
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        maintenance disabled
        """
        And zookeeper node "/test/maintenance" should not exist within "30" seconds

    Scenario: CLI info short prints cluster nodes
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds

        When I run command on host "mysql1"
        """
        mysync info -s
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        (?s).*ha_nodes:.*mysql1.*mysql2.*mysql3.*
        """
        And command output should match regexp
        """
        (?s).*active_nodes:.*mysql1.*mysql2.*mysql3.*
        """
        And command output should match regexp
        """
        (?s).*health:\n- '===> "mysql1": .*\n- '     "mysql2": .*\n- '     "mysql3": .*
        """

    Scenario: CLI info short filters health by host and zone
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds

        When I run command on host "mysql1"
        """
        mysync info -s --host mysql2
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        (?s).*health:.*"mysql2".*
        """
        And command output should not match regexp
        """
        (?s).*health:.*"mysql1".*
        """

        When I run command on host "mysql1"
        """
        mysync info -s --zone klg
        """
        Then command return code should be "0"
        And command output should not match regexp
        """
        (?s).*health:.*"mysql2".*
        """
        And command output should not match regexp
        """
        (?s).*health:.*"mysql1".*
        """

