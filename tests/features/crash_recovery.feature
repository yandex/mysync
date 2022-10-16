Feature: resetup after crash recovery

    Scenario: replica resetup after crash recovery
      Given cluster environment is
        """
        MYSYNC_RESETUP_CRASHED_HOSTS=true
        """
      Given cluster is up and running
      Then mysql host "mysql1" should be master
      When mysql on host "mysql2" is killed
      Then mysql host "mysql2" should become unavailable within "60" seconds
      When mysql on host "mysql2" is started
      Then mysql host "mysql2" should become available within "60" seconds
      Then host "mysql2" should have file "/tmp/mysync.resetup" within "60" seconds

   Scenario: force failover for crashed master
     Given cluster environment is
       """
       MYSYNC_RESETUP_CRASHED_HOSTS=true
       MYSYNC_FAILOVER=true
       MYSYNC_FAILOVER_DELAY=120s
       """
     Given cluster is up and running
     Then mysql host "mysql1" should be master
     And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
       """
       ["mysql1","mysql2","mysql3"]
       """
     When mysql on host "mysql1" is killed
     Then mysql host "mysql1" should become unavailable within "60" seconds
     When mysql on host "mysql1" is started
     Then mysql host "mysql1" should become available within "60" seconds
     Then zookeeper node "/test/last_switch" should match json within "30" seconds
       """
       {
         "cause": "auto",
         "from": "mysql1",
         "result": {
           "ok": true
         }
       }
       """
     And zookeeper node "/test/master" should match regexp
       """
         .*mysql[23].*
       """
     Then host "mysql1" should have file "/tmp/mysync.resetup" within "60" seconds

   Scenario: cascade failures within cooldown should not leave master offline
     Given cluster environment is
       """
       MYSYNC_RESETUP_CRASHED_HOSTS=true
       MYSYNC_FAILOVER=true
       MYSYNC_FAILOVER_DELAY=180s
       MYSYNC_FAILOVER_COOLDOWN=600s
       """
     Given cluster is up and running
     Then mysql host "mysql1" should be master
     And zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
       """
       ["mysql1","mysql2","mysql3"]
       """
     When mysql on host "mysql1" is killed
     Then mysql host "mysql1" should become unavailable within "60" seconds
     When mysql on host "mysql1" is started
     Then mysql host "mysql1" should become available within "60" seconds
     And zookeeper node "/test/last_switch" should match json within "30" seconds
       """
       {
         "cause": "auto",
         "from": "mysql1",
         "result": {
           "ok": true
         }
       }
       """
     And host "mysql1" should have file "/tmp/mysync.resetup" within "60" seconds

     When I get zookeeper node "/test/master"
     And I save zookeeper query result as "new_master"
     Then mysql host "{{.new_master}}" should be master
     When mysql on host "{{.new_master}}" is killed
     Then mysql host "{{.new_master}}" should become unavailable within "60" seconds
     When mysql on host "{{.new_master}}" is started
     Then mysql host "{{.new_master}}" should become available within "60" seconds
     # As cooldown not elapsed
     Then mysql host "{{.new_master}}" should be master
     And mysql host "{{.new_master}}" should become writable within "15" seconds
     And mysql host "{{.new_master}}" should be online within "15" seconds
