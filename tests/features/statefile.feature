Feature: mysync touches info file when diskusage is too high

    Scenario: mysync touch info file on high disk usage
        Given cluster environment is
        # force mysync to enable RO in any case
        """
        MYSYNC_CRITICAL_DISK_USAGE=0
        """
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/health/mysql1" should match json within "30" seconds
        """
        {
            "is_master": true,
            "is_readonly": true
        }
        """
        When I wait for "60" seconds
        And info file "/var/run/mysync/mysync.info" on "mysql1" match json
        """
        {
            "low_space": true
        }
        """
        And info file "/var/run/mysync/mysync.info" on "mysql2" match json
        """
        {
            "low_space": true
        }
        """
        And info file "/var/run/mysync/mysync.info" on "mysql3" match json
        """
        {
            "low_space": true
        }
        """

    Scenario: mysync does not touch info file on ok disk consumption
        Given cluster environment is
        # force mysync to enable RO in any case
        """
        MYSYNC_CRITICAL_DISK_USAGE=95
        """
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql replication on host "mysql2" should run fine within "5" seconds
        And mysql host "mysql3" should be replica of "mysql1"
        And mysql replication on host "mysql3" should run fine within "5" seconds
        And zookeeper node "/test/health/mysql1" should match json within "30" seconds
        """
        {
            "is_master": true,
            "is_readonly": false
        }
        """
        And info file "/var/run/mysync/mysync.info" on "mysql1" match json
        """
        {
            "low_space": false
        }
        """
        And info file "/var/run/mysync/mysync.info" on "mysql2" match json
        """
        {
            "low_space": false
        }
        """
        And info file "/var/run/mysync/mysync.info" on "mysql3" match json
        """
        {
            "low_space": false
        }
        """

