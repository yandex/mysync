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
        And I run SQL on mysql host "mysql1" expecting error on number "3074"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        And I run SQL on mysql host "mysql1"
        """
            SELECT source_host, source_user, source_password, source_port  FROM mysql.replication_settings WHERE channel_name = 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "source_host": "test_source to source",
            "source_user": "test_user",
            "source_password": "test_pass",
            "source_port": 2222,
        }]
        """
        When I wait for "5" seconds
        And I run SQL on mysql host "mysql2"
        """
            SELECT source_host, source_user, source_password, source_port  FROM mysql.replication_settings WHERE channel_name = 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "source_host": "test_source to source",
            "source_user": "test_user",
            "source_password": "test_pass",
            "source_port": 2222,
        }]
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
        And I wait for "1" seconds
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
        And I run SQL on mysql host "mysql1" expecting error on number "3074"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then I run SQL on mysql host "mysql1"
        """
            SELECT source_host, source_user, source_password, source_port  FROM mysql.replication_settings WHERE channel_name = 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "source_host": "test_source to source",
            "source_user": "test_user",
            "source_password": "test_pass",
            "source_port": 2222,
        }]
        """

        When I run SQL on mysql host "mysql2"
        """
            UPDATE mysql.replication_settings
            SET source_ssl_ca = '-----BEGIN CERTIFICATE-----
             MIIDCTCCAfGgAwIBAgIBATANBgkqhkiG9w0BAQsFADA/MT0wOwYDVQQDDDRNeVNR
             TF9TZXJ2ZXJfOC4wLjMwLTIyX0F1dG9fR2VuZXJhdGVkX0NBX0NlcnRpZmljYXRl
             MB4XDTIzMDUyMDE3MTExMVoXDTMzMDUxNzE3MTExMVowPzE9MDsGA1UEAww0TXlT
             UUxfU2VydmVyXzguMC4zMC0yMl9BdXRvX0dlbmVyYXRlZF9DQV9DZXJ0aWZpY2F0
             ZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMxEcfxGCEuGSUA4ZDVU
             YXl94bslyjFDh9BFrt/gbj9iu2H88VjtmuU+qhBOGV6wHsqMY3EBlfP2/6CHu+wP
             XYRTBqhwWF0AijEI63RGpuEmtl9mf7baxx9bXNkWWWEFke0y6w08VHj3hYkvOGCA
             JsBXpUvBqgxPtHvXRdWks/WUs50a10HZ0T2sim4CV9rbUpq0lblhmYeiHrPsmvaM
             fK+lgZwt4NMSqcrGiXY0KpiSCM0LGIPm8Be6aUQlZVKbKrEP4S/BpC3HQLBUcrk4
             Wp18o8S8eUgxLaybGKiQrtms12/+VgfXDdsxSXfkX77ZYqXSiBJZ5GXevZRhrfvA
             WCkCAwEAAaMQMA4wDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAFr5N
             vejmieL0u47Zcw1z6dssED96ETMGciFvRAj2cTnGT1KpPMOB/UcidS3RclOnas43
             HpqnFWiEDBePUZuPiepFm/rJZnUMFs0Yng7D4y++6EzjDb8hUG/7gIeTvmW2G/pl
             y9qlrZp7ZmfJ3mgtd3BJktaidnzleWl/xWw8vTeVkteoPE1QGzlgzObHf4UVnVs3
             tO8jq1ZBq1uywP9xjYzdDMaBBjvhJJfwOllHo5d3ME/l7Qzv+ApFvYQjWlAIXj5n
             KJ3etZdZDFBUPZBRawJjmBKGyxVo4x2chbxzQ92HsCPYK4eMXafGZhGKIJ586w5w
             Y2AirKuDzA5GErKOfQ==
             -----END CERTIFICATE-----'
        """
        And I wait for "10" seconds
        And I run command on host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match regexp
        """
        [{
            "Replica_IO_State": "Connecting to source",
            "Source_Host": "test_source_2",
            "Source_Port": 2222,
            "Source_User": "test_user_2",
            "Source_SSL_CA_File": "/etc/mysql/ssl/external_CA.pem",
            "Replica_IO_Running": "Connecting",
            "Relay_Source_Log_File": "",
            "Exec_Source_Log_Pos": 0,
            "Last_IO_Errno": 2005,
            "Channel_Name": "external"
        }]
        """