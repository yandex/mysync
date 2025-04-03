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
        .*replica can be set cascade.*[[:space:]].*node already has priority 5 set.*
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
        .*node is already streaming from mysql3[[:space:]].*node mysql2 is not HA node, priority cannot be set.*
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

    Scenario: CLI turbo mode works properly
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql host "mysql3" should be replica of "mysql1"

        # Try to optimize host with default settings
        When I run SQL on mysql host "mysql2"
        """
        SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog
        """
        Then SQL result should match json
        """
        [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'can be optimized'
        """

        When I run command on host "mysql2"
        """
        mysync turbo on
        """
        Then command return code should be "0"
        When I run SQL on mysql host "mysql2"
        """
        SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog
        """
        Then SQL result should match json
        """
        [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'optimization is running'
        """

        # Try to stop optimization on host with default settings
        When I run command on host "mysql2"
        """
        mysync turbo off
        """
        Then command return code should be "0"
        When I run SQL on mysql host "mysql2"
        """
        SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog
        """
        Then SQL result should match json
        """
        [{"InnodbFlushLogAtTrxCommit":1,"SyncBinlog":1}]
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'can be optimized'
        """

        # Host optimization with non-default option which less than MySync "optimal" ones
        When I run SQL on mysql host "mysql1"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 999;
        """
        When I run command on host "mysql2"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 999;
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'can be optimized'
        """
        When I run command on host "mysql2"
        """
        mysync turbo on
        """
        Then command return code should be "0"
        When I run SQL on mysql host "mysql2"
        """
        SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog
        """
        Then SQL result should match json
        """
        [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":1000}]
        """
        When I run command on host "mysql2"
        """
        mysync turbo off
        """
        Then command return code should be "0"
        When I run SQL on mysql host "mysql2"
        """
        SELECT @@innodb_flush_log_at_trx_commit as InnodbFlushLogAtTrxCommit, @@sync_binlog as SyncBinlog
        """
        Then SQL result should match json
        """
        [{"InnodbFlushLogAtTrxCommit":2,"SyncBinlog":999}]
        """

        # Host optimization with non-default option which more than MySync "optimal" ones
        When I run SQL on mysql host "mysql1"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 1001;
        """
        When I run SQL on mysql host "mysql2"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 1001;
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'configuration of the cluster is already optimized'
        """
        When I run command on host "mysql2"
        """
        mysync turbo on
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        Can't optimize host in status 'configuration of the cluster is already optimized'
        """

        # Host optimization with non-default option 'innodb_flush_log_at_trx_commit'
        When I run SQL on mysql host "mysql1"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 0; SET GLOBAL sync_binlog = 3;
        """
        When I run SQL on mysql host "mysql2"
        """
        SET GLOBAL innodb_flush_log_at_trx_commit = 0; SET GLOBAL sync_binlog = 3;
        """
        When I run command on host "mysql2"
        """
        mysync turbo get
        """
        Then command return code should be "0"
        And command output should match regexp
        """
        The host is in status 'configuration of the cluster is already optimized'
        """
        When I run command on host "mysql2"
        """
        mysync turbo on
        """
        Then command return code should be "1"

        When I run command on host "mysql1"
        """
        mysync turbo on
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        Can't optimize host in status 'host is master'
        """

        # Master host can't be optimizated
        When I run command on host "mysql1"
        """
        mysync turbo on
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        Can't optimize host in status 'host is master'
        """
