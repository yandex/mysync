Feature: maintenance during dead zookeeper

  Scenario: mysync keeps maintenance while zookeeper is down
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql1" should be writable
    When I run command on host "mysql1"
      """
      mysync maint on
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      maintenance enabled
      """
    And I wait for "5" seconds

    When host "zoo3" is detached from the network
    And host "zoo2" is detached from the network
    And host "zoo1" is detached from the network
    When I run command on host "mysql1"
      """
      mysync info
      """
    Then command return code should be "1"

    When I wait for "10" seconds
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql1" should be writable


    When I run command on host "mysql1" with timeout "20" seconds
      """
      supervisorctl restart mysync
      """
    Then command return code should be "0"
    When I run command on host "mysql2" with timeout "20" seconds
      """
      supervisorctl restart mysync
      """
    Then command return code should be "0"
    When I run command on host "mysql3" with timeout "20" seconds
      """
      supervisorctl restart mysync
      """
    Then command return code should be "0"

    When I wait for "30" seconds
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql1" should be writable

    When host "zoo3" is attached to the network
    And host "zoo2" is attached to the network
    And host "zoo1" is attached to the network

    Then zookeeper node "/test/maintenance" should match json within "90" seconds
      """
      {
        "initiated_by": "REGEXP:.*@mysql1"
      }
      """
    When I run command on host "mysql1" with timeout "30" seconds
      """
      mysync maint off
      """
    Then command return code should be "0"

    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql1" should be writable
    And zookeeper node "/test/health/mysql1" should match json within "30" seconds
      """
      {
        "ping_ok": true,
        "is_readonly": false
      }
      """
    And zookeeper node "/test/health/mysql2" should match json within "30" seconds
      """
      {
        "ping_ok": true,
        "is_readonly": true
      }
      """
    And zookeeper node "/test/health/mysql3" should match json within "30" seconds
      """
      {
        "ping_ok": true,
        "is_readonly": true
      }
      """
