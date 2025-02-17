Feature: manager swithover 
  Scenario: manager loss quorum than release lock 
    Given cluster environment is
    """
    MANAGER_ELECTION_DELAY_AFTER_QUORUM_LOSS=180s
    MANAGER_LOCK_ACQUIRE_DELAY_AFTER_QUORUM_LOSS=180s
    """
    And cluster is up and running

    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"

    When I run command on host "{{.manager.hostname}}"
      """
      mysync switch --from "{{.manager.hostname}}"
      """
    Then command return code should be "0"

    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "master"

    Then mysql host "{{.manager.hostname}}" should be replica of "{{.master}}"

    When host "{{.manager.hostname}}" is detached from the user network
    Then mysql host "{{.manager.hostname}}" should become unavailable within "10" seconds

    Then zookeeper node "/test/manager" should match regexp within "30" seconds
      """
        .*{{.manager.hostname}}.*
      """
    Then zookeeper node "/test/manager" should not match regexp within "300" seconds
      """
        .*{{.manager.hostname}}.*
      """

  Scenario: manager loss quorum near master that`s why manager dont release lock 
    Given cluster is up and running

    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"

    When I run command on host "{{.manager.hostname}}"
      """
      mysync switch --to "{{.manager.hostname}}"
      """
    Then command return code should be "0"

    When host "{{.manager.hostname}}" is detached from the user network
    Then mysql host "{{.manager.hostname}}" should become unavailable within "10" seconds

    Then I wait for "300" seconds

    Then zookeeper node "/test/manager" should match regexp 
      """
        .*{{.manager.hostname}}.*
      """

  Scenario: manager switch is off
    Given cluster environment is
    """
    MANAGER_SWITCHOVER = false
    """
    And cluster is up and running

    When I get zookeeper node "/test/manager"
    And I save zookeeper query result as "manager"

    When I run command on host "{{.manager.hostname}}"
      """
      mysync switch --from "{{.manager.hostname}}"
      """
    Then command return code should be "0"

    When I get zookeeper node "/test/master"
    And I save zookeeper query result as "master"

    Then mysql host "{{.manager.hostname}}" should be replica of "{{.master}}"

    When host "{{.manager.hostname}}" is detached from the user network
    Then mysql host "{{.manager.hostname}}" should become unavailable within "10" seconds

    Then I wait for "300" seconds

    Then zookeeper node "/test/manager" should match regexp 
      """
        .*{{.manager.hostname}}.*
      """
