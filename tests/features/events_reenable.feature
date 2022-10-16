Feature: mysync reenables slaveside disabled events

  Scenario: reenable events after switchover
    Given cluster is up and running
    Then mysql host "mysql1" should be master
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql replication on host "mysql2" should run fine within "5" seconds
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql replication on host "mysql3" should run fine within "5" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
    """
    ["mysql1","mysql2","mysql3"]
    """
    When I run SQL on mysql host "mysql1"
    """
        CREATE TABLE mysql.mdb_repl_mon(
            ts TIMESTAMP(3)
        ) ENGINE=INNODB;
    """
    And I run SQL on mysql host "mysql1"
    """
        INSERT INTO mysql.mdb_repl_mon VALUES(CURRENT_TIMESTAMP(3));
    """
    And I run SQL on mysql host "mysql1"
    """
        CREATE EVENT mysql.mdb_repl_mon_event
        ON SCHEDULE EVERY 1 SECOND
        DO UPDATE mysql.mdb_repl_mon SET ts = CURRENT_TIMESTAMP(3);
    """
    Then mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"
    And mysql host "mysql2" should have event "mysql.mdb_repl_mon_event" in status "SLAVESIDE_DISABLED" within "10" seconds
    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    Then zookeeper node "/test/last_switch" should match json within "30" seconds
      """
      {
        "from": "",
        "to": "mysql2",
        "result": {
          "ok": true
        }
      }

      """
    Then mysql host "mysql2" should be master
    And mysql host "mysql1" should be replica of "mysql2"

    Then mysql host "mysql2" should have event "mysql.mdb_repl_mon_event" in status "ENABLED"
    And mysql host "mysql1" should have event "mysql.mdb_repl_mon_event" in status "SLAVESIDE_DISABLED" within "10" seconds
