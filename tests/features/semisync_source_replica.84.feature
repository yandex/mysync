Feature: source/replica semisync dialect

  Scenario: mysync manages semisync with source/replica plugins
    Given cluster environment is
      """
      SEMISYNC_DIALECT=sourceReplica
      MYSYNC_WAIT_FOR_SLAVE_COUNT=2
      """
    And cluster is up and running

    When I run SQL on mysql host "mysql1"
      """
      SELECT PLUGIN_NAME AS PluginName
      FROM information_schema.PLUGINS
      WHERE PLUGIN_NAME LIKE 'rpl_semi_sync_%'
      ORDER BY PLUGIN_NAME
      """
    Then SQL result should match json_exactly
      """
      [
        {"PluginName":"rpl_semi_sync_replica"},
        {"PluginName":"rpl_semi_sync_source"}
      ]
      """

    And mysql host "mysql1" should be master
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "2" within "20" seconds
    And mysql host "mysql2" should be replica of "mysql1"
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should be replica of "mysql1"
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"

    When I run command on host "mysql1"
      """
      mysync maint on
      """
    Then command return code should be "0"
    And zookeeper node "/test/maintenance" should match json within "30" seconds
      """
      {
        "mysync_paused": true
      }
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "0" within "10" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "1"

    When I run command on host "mysql1"
      """
      mysync maint off
      """
    Then command return code should be "0"
    And zookeeper node "/test/maintenance" should not exist within "30" seconds
    And zookeeper node "/test/active_nodes" should match json_exactly within "30" seconds
      """
      ["mysql1","mysql2","mysql3"]
      """
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "2" within "20" seconds

    When I run command on host "mysql1"
      """
      mysync switch --to mysql2 --wait=0s
      """
    Then command return code should be "0"
    And command output should match regexp
      """
      switchover scheduled
      """
    And zookeeper node "/test/last_switch" should match json within "60" seconds
      """
      {
        "to": "mysql2",
        "master_transition": "switchover",
        "result": {
          "ok": true
        }
      }
      """
    And mysql host "mysql2" should be master
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_enabled" set to "1" within "20" seconds
    And mysql host "mysql2" should have variable "rpl_semi_sync_replica_enabled" set to "0"
    And mysql host "mysql2" should have variable "rpl_semi_sync_source_wait_for_replica_count" set to "2" within "20" seconds
    And mysql host "mysql1" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql1" should have variable "rpl_semi_sync_source_enabled" set to "0"
    And mysql host "mysql3" should become replica of "mysql2" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_replica_enabled" set to "1" within "20" seconds
    And mysql host "mysql3" should have variable "rpl_semi_sync_source_enabled" set to "0"
