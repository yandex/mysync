Feature: hosts recovery

    Scenario: failover honors recovery hosts
      Given cluster environment is
        """
        MYSYNC_FAILOVER=true
        MYSYNC_FAILOVER_DELAY=0s
        MYSYNC_FAILOVER_COOLDOWN=0s
        """
      Given cluster is up and running
      When host "mysql3" is deleted
      Then mysql host "mysql3" should become unavailable within "10" seconds
      And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2"]
      """
      When host "mysql1" is detached from the network
      Then mysql host "mysql1" should become unavailable within "10" seconds
      Then zookeeper node "/test/manager" should match regexp within "10" seconds
        """
          .*mysql2.*
        """
      Then zookeeper node "/test/last_switch" should match json within "120" seconds
        """
        {
          "cause": "auto",
          "from": "mysql1",
          "result": {
            "ok": true
          }
        }
        """
      Then zookeeper node "/test/master" should match regexp within "30" seconds
        """
          .*mysql2.*
        """
      And zookeeper node "/test/recovery/mysql1" should exist
      Then mysql host "mysql2" should be master
      And mysql host "mysql2" should be writable
      And mysql host "mysql2" should have variable "rpl_semi_sync_master_enabled" set to "0"
      # Commit normal transaction
      Then I run SQL on mysql host "mysql2"
      """
        CREATE TABLE splitbrain(id int);
      """

      # Emulate lost transactions on old master
      # mysync may set super_read_only while we run query
      When I run command on host "mysql1" until return code is "0" with timeout "5" seconds
        """
          mysql -e '
            SET GLOBAL rpl_semi_sync_master_enabled = 0;
            SET GLOBAL super_read_only = 0;
            CREATE TABLE mysql.splitbrain(id int);
            SET GLOBAL read_only = 1;
          '
        """

      When host "mysql1" is attached to the network
      Then mysql host "mysql1" should become available within "10" seconds
      And mysql host "mysql1" should become replica of "mysql2" within "10" seconds
      And mysql replication on host "mysql1" should not run fine within "3" seconds
      And zookeeper node "/test/recovery/mysql1" should exist
      And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql2"]
      """
      And host "mysql1" should have file "/tmp/mysync.resetup" within "20" seconds
