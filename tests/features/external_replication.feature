Feature: external replication

    Scenario: external replication
        Given cluster is up and running
        Then mysql host "mysql1" should be master
        And mysql host "mysql2" should be replica of "mysql1"
        And mysql host "mysql3" should be replica of "mysql1"
        When I run SQL on mysql host "mysql1"
        """
            CREATE TABLE mysql.replication_settings(
                channel_name VARCHAR(50) NOT NULL,
                source_host VARCHAR(50) NOT NULL,
                source_user VARCHAR(50) NOT NULL,
                source_password VARCHAR(50) NOT NULL,
                source_port INT UNSIGNED NOT NULL,
                source_ssl_ca VARCHAR(4096) NOT NULL DEFAULT '',
                source_delay INT UNSIGNED NOT NULL DEFAULT 0,
                source_log_file VARCHAR(50) NOT NULL DEFAULT '',
                source_log_pos INT UNSIGNED NOT NULL DEFAULT 0,
                PRIMARY KEY (channel_name)
            ) ENGINE=INNODB;
        """
        And I run SQL on mysql host "mysql1"
        """
            INSERT INTO mysql.replication_settings
            (channel_name, source_host, source_user, source_password, source_port)
            VALUES ('external', 'test_source', 'test_user', 'test_pass', 2222);
        """
        And I wait for "5" seconds
        And I run SQL on mysql host "mysql1" expecting error on number "3074"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        When I run SQL on mysql host "mysql1"
        """
            CHANGE REPLICATION SOURCE TO  SOURCE_HOST = 'test_source',
                SOURCE_USER = 'test_user',
                SOURCE_PASSWORD = 'test_pass',
                SOURCE_PORT = 1111,
                SOURCE_AUTO_POSITION = 1
                FOR CHANNEL 'external'
        """
        And I run SQL on mysql host "mysql1"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "Replica_IO_State": "Connecting to source",
            "Source_Host": "test_source",
            "Source_Port": 1111,
            "Source_User": "test_user",
            "Replica_IO_Running": "Connecting",
            "Relay_Source_Log_File": "",
            "Exec_Source_Log_Pos": 0,
            "Last_IO_Errno": 2005,
            "Channel_Name": "external"
        }]
        """

        When I run command on host "mysql2"
          """
          mysync switch --to mysql2 --wait=0s
          """
        Then command return code should be "0"
        And command output should match regexp
          """
          switchover scheduled
          """
        And zookeeper node "/test/switch" should match json
        """
        {
            "from": "",
            "to": "mysql2"
        }
        """
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
        And I wait for "10" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "Replica_IO_State": "Connecting to source",
            "Source_Host": "test_source",
            "Source_Port": 1111,
            "Source_User": "test_user",
            "Replica_IO_Running": "Connecting",
            "Relay_Source_Log_File": "",
            "Exec_Source_Log_Pos": 0,
            "Last_IO_Errno": 2005,
            "Channel_Name": "external"
        }]
        """

        When host "mysql1" is started
        Then mysql host "mysql1" should become available within "20" seconds
        And mysql host "mysql1" should become replica of "mysql2" within "10" seconds
