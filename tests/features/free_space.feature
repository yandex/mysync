Feature: free space
    Background:
      Given cluster environment is
        """
        MYSYNC_CRITICAL_DISK_USAGE=95
        MYSYNC_KEEP_SUPER_WRITABLE_ON_CRITICAL_DISK_USAGE=false
        """

    Scenario: master become read only on low free space
      Given cluster is up and running
      Then zookeeper node "/test/active_nodes" should match json_exactly within "20" seconds
        """
        ["mysql1","mysql2","mysql3"]
        """
      And mysql host "mysql1" should be master
      And mysql host "mysql1" should be writable
      When I set used space on host "mysql1" to 99%
      Then mysql host "mysql1" should become read only within "30" seconds
      When I set used space on host "mysql1" to 80%
      Then mysql host "mysql1" should become writable within "30" seconds

    Scenario: master become read only no super on low free space
      Given cluster environment is
        """
        MYSYNC_CRITICAL_DISK_USAGE=95
        MYSYNC_KEEP_SUPER_WRITABLE_ON_CRITICAL_DISK_USAGE=true
        MYSYNC_FAILOVER=false
        """
      Given cluster is up and running
      Then mysql host "mysql1" should be master
      And mysql host "mysql1" should be writable
      When I set used space on host "mysql1" to 99%
      Then mysql host "mysql1" should become read only no super within "30" seconds
      When mysql on host "mysql1" is restarted
      Then mysql host "mysql1" should become available within "60" seconds
      And mysql host "mysql1" should become read only no super within "30" seconds
      When I set used space on host "mysql1" to 80%
      Then mysql host "mysql1" should become writable within "30" seconds

    Scenario: single replica overflow should not make master read only
      Given cluster is up and running
      Then mysql host "mysql1" should be master
      And mysql host "mysql1" should be writable
      When I set used space on host "mysql2" to 99%
      And I wait for "15" seconds
      Then mysql host "mysql1" should be writable

    Scenario: all replicas overflow should make master read only
      Given cluster is up and running
      Then mysql host "mysql1" should be master
      And mysql host "mysql1" should be writable
      When I set used space on host "mysql2" to 99%
      When I set used space on host "mysql3" to 99%
      Then mysql host "mysql1" should become read only within "30" seconds
      When I set used space on host "mysql2" to 80%
      Then mysql host "mysql1" should become writable within "30" seconds
