Feature: resume an interrupted switchover on the same manager

  Scenario: resume phase 5 when the old master and both other hosts are replicas
    Given cluster environment is
      """
      MYSYNC_SWITCHOVER_MAX_ATTEMPTS=1
      MYSYNC_SWITCHOVER_TIMEOUT=10s
      MYSYNC_START_REPLICATION_QUERY=CALL mysql.mysync_test_start_replication()
      """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1", "mysql2", "mysql3"]
      """
    # START REPLICA succeeds on the old master, then the procedure reports an
    # error. All three hosts have replica channels while phase 5 is retried.
    When I run command on host "mysql1"
      """
      mysql <<'SQL'
      DELIMITER //
      CREATE PROCEDURE mysql.mysync_test_start_replication()
      BEGIN
        START REPLICA;
        IF @@server_id = 1 AND @@max_prepared_stmt_count = 100 THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected error after demotion';
        END IF;
      END//
      SQL
      """
    Then command return code should be "0"
    And mysql replication on host "mysql2" should run fine within "10" seconds
    And mysql replication on host "mysql3" should run fine within "10" seconds
    When I run SQL on mysql host "mysql1"
      """
      SET GLOBAL max_prepared_stmt_count = 100
      """
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And mysql host "mysql1" should become replica of "mysql2" within "30" seconds
    When I run SQL on mysql host "mysql2"
      """
      SHOW REPLICA STATUS
      """
    And I save SQL result as "candidate_status"
    When I wait for "15" seconds
    Then mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql host "mysql2" should be replica of "{{ (index .candidate_status 0).Source_Host }}"
    And zookeeper node "/test/master" should match json_exactly
      """
      "mysql1"
      """
    And zookeeper node "/test/switch" should match json
      """
      {"to": "mysql2", "result": {"ok": false}}
      """
    When I run SQL on mysql host "mysql1"
      """
      SET GLOBAL max_prepared_stmt_count = 101
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {"to": "mysql2", "result": {"ok": true}}
      """
    And zookeeper node "/test/switch" should not exist
    And zookeeper node "/test/master" should match json_exactly
      """
      "mysql2"
      """
    And mysql host "mysql2" should be master
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql replication on host "mysql1" should run fine within "10" seconds
    And mysql replication on host "mysql3" should run fine within "10" seconds

  Scenario: finish promotion after a temporary SQL error despite retry and time limits
    Given cluster environment is
      """
      MYSYNC_SWITCHOVER_MAX_ATTEMPTS=1
      MYSYNC_SWITCHOVER_TIMEOUT=10s
      MYSYNC_SET_WRITABLE_QUERY=CALL mysql.mysync_test_set_writable()
      """
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1", "mysql2", "mysql3"]
      """
    # Fail only the make-writable step on mysql2. Changing the marker clears
    # the fault without restarting or replacing the manager process.
    When I run command on host "mysql1"
      """
      mysql <<'SQL'
      SET GLOBAL read_only = 0;
      DELIMITER //
      CREATE PROCEDURE mysql.mysync_test_set_writable()
      BEGIN
        IF @@server_id = 2 AND @@max_prepared_stmt_count = 100 THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected make-writable failure';
        END IF;
        SET GLOBAL read_only = 0;
      END//
      SQL
      """
    Then command return code should be "0"
    And mysql replication on host "mysql2" should run fine within "10" seconds
    And mysql replication on host "mysql3" should run fine within "10" seconds
    When I run SQL on mysql host "mysql2"
      """
      SET GLOBAL max_prepared_stmt_count = 100
      """
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And mysql host "mysql2" should be master within "30" seconds
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql3" should be replica of "mysql2"
    When I wait for "15" seconds
    Then mysql host "mysql2" should be master
    And mysql host "mysql2" should be read only
    And zookeeper node "/test/master" should match json_exactly
      """
      "mysql1"
      """
    And zookeeper node "/test/switch" should match json
      """
      {"to": "mysql2", "result": {"ok": false}}
      """
    When I run SQL on mysql host "mysql2"
      """
      SET GLOBAL max_prepared_stmt_count = 101
      """
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {"to": "mysql2", "result": {"ok": true}}
      """
    And zookeeper node "/test/switch" should not exist
    And zookeeper node "/test/master" should match json_exactly
      """
      "mysql2"
      """
    And mysql host "mysql2" should be writable
    And mysql host "mysql1" should be replica of "mysql2"
    And mysql host "mysql3" should be replica of "mysql2"
    And mysql replication on host "mysql1" should run fine within "10" seconds
    And mysql replication on host "mysql3" should run fine within "10" seconds
