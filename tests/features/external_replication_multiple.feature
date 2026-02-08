Feature: external replication switchover

    Scenario: external replication works with multiple sources
        Given cluster environment is
        """
        MYSYNC_REPLICATION_REPAIR_COOLDOWN=30s
        """    
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
                source_log_file VARCHAR(150) NOT NULL DEFAULT '',
                source_log_pos INT UNSIGNED NOT NULL DEFAULT 0,
                replication_status ENUM ('stopped', 'running') NOT NULL DEFAULT 'stopped',
                replication_filter VARCHAR(4096) NOT NULL DEFAULT '',
                PRIMARY KEY (channel_name)
            ) ENGINE=INNODB;
        """
        And I run SQL on mysql host "mysql1"
        """
            INSERT INTO mysql.replication_settings
            (channel_name, source_host, source_user, source_password, source_port, replication_status, replication_filter)
            VALUES ('external', 'test_source_2', 'test_user_2', 'test_pass_2', 2222, 'running', 'REPLICATE_DO_DB = (testdb1)');
        """
        And I run SQL on mysql host "mysql1" expecting error on number "3074"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        And I run SQL on mysql host "mysql1"
        """
            SELECT source_host, source_user, source_password, source_port, replication_filter 
            FROM mysql.replication_settings WHERE channel_name = 'external'
        """
        Then SQL result should match json
        """
        [{
            "source_host": "test_source_2",
            "source_password": "test_pass_2",
            "source_port": 2222,
            "source_user": "test_user_2",
            "replication_filter": "REPLICATE_DO_DB = (testdb1)"
        }]
        """
        When I wait for "5" seconds
        And I run SQL on mysql host "mysql2"
        """
            SELECT source_host, source_user, source_password, source_port, replication_filter 
            FROM mysql.replication_settings WHERE channel_name = 'external'
        """
        Then SQL result should match json
        """
        [{
            "source_host": "test_source_2",
            "source_port": 2222,
            "source_password": "test_pass_2",
            "source_user": "test_user_2",
            "replication_filter": "REPLICATE_DO_DB = (testdb1)"
        }]
        """
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
        And I run SQL on mysql host "mysql2" expecting error on number "3074"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        And I run SQL on mysql host "mysql1"
        """
            START REPLICA FOR CHANNEL 'external'
        """
        And I run SQL on mysql host "mysql1"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match json
        """
        [{
            "Exec_Source_Log_Pos": 0,
            "Replica_IO_State": "Connecting to source",
            "Replica_SQL_Running": "Yes",
            "Source_Host": "test_source",
            "Source_Port": 1111,
            "Source_User": "test_user",
            "Replica_IO_Running": "Connecting",
            "Relay_Source_Log_File": "",
            "Channel_Name": "external",
            "Source_SSL_CA_File": ""
        }]
        """
        When I run SQL on mysql host "mysql1"
        """
            UPDATE mysql.replication_settings
            SET source_ssl_ca = '-----BEGIN CERTIFICATE-----
MIIDDDCCAfSgAwIBAgIBATANBgkqhkiG9w0BAQsFADA/MT0wOwYDVQQDDDRNeVNR
TF9TZXJ2ZXJfOC4wLjMyLTI0X0F1dG9fR2VuZXJhdGVkX0NBX0NlcnRpZmljYXRl
MB4XDTIzMDUxNDE2NDA1OFoXDTMzMDUxMTE2NDA1OFowPzE9MDsGA1UEAww0TXlT
UUxfU2VydmVyXzguMC4zMi0yNF9BdXRvX0dlbmVyYXRlZF9DQV9DZXJ0aWZpY2F0
ZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOFExOlSI8gd0LtIko+z
SpVP94Kk0mxRALdNWry6Ua1PoLogq+ScE0OMN6JamaLqG268K5gIdydLOaK9kx2h
4XXyPUTTepuivpnpiI4KqMcaWYQzmot5eoSOOQL6E5hb09oRXY+IhlaynFg0l/E7
t5uMMUopmcfOH6OGMXTCFXebKbWGnzHx83bXkyzMWWc1p4X+aP18dewHsYuwZOdx
1goNZNNz0BaJq2y0RYnfYeNOLV6d+S6BAMAUkWbABdols8Pi8ezsPwZ8x/1vk7uy
tUOmiuMkLsC6LzJnnUaoGR3tflCH+yU3XSPQpnZYzaFaeA3d6mgV93w7y3Jreavx
tHkCAwEAAaMTMBEwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEA
dZ9vGVJaAauomoDp9VY4zOr0G4n7WnEElqMAxOQPzLJwRXe81/GchmUKWvX5Fc6o
6RiEa7Nw4YiXKyFMqoJbQN3j8EkOiHs1FtrwJNsobzmlVmjuqxqCBWmVQPpUfOQh
f6I/gQr2BVxvNsj+IvuI0vIVjP5J3GBxL9ySvFKsfp4xtk1oTHIuA2G3haIv2AJp
j/Hm7nVvoXWrb/zX+fagi0rrf+3hDCsHMXtxaxXk2sGRLKHgkTYTVwEPQ6SKEqrW
qnSOx+SMl4up6AVfEq6kVR8ZIt/CzJBWZ4qYQnOf0eK4KQC6UB22adzsaFMmhzRB
YZQy1bHIhscLf8wjTYbzAg==
-----END CERTIFICATE-----'
        """
        Then host "mysql1" should have file "/etc/mysql/ssl/external_CA.pem" within "10" seconds
        And host "mysql2" should have no file "/etc/mysql/ssl/external_CA.pem"
        And file "/etc/mysql/ssl/external_CA.pem" on host "mysql1" should have content
        """
-----BEGIN CERTIFICATE-----
MIIDDDCCAfSgAwIBAgIBATANBgkqhkiG9w0BAQsFADA/MT0wOwYDVQQDDDRNeVNR
TF9TZXJ2ZXJfOC4wLjMyLTI0X0F1dG9fR2VuZXJhdGVkX0NBX0NlcnRpZmljYXRl
MB4XDTIzMDUxNDE2NDA1OFoXDTMzMDUxMTE2NDA1OFowPzE9MDsGA1UEAww0TXlT
UUxfU2VydmVyXzguMC4zMi0yNF9BdXRvX0dlbmVyYXRlZF9DQV9DZXJ0aWZpY2F0
ZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOFExOlSI8gd0LtIko+z
SpVP94Kk0mxRALdNWry6Ua1PoLogq+ScE0OMN6JamaLqG268K5gIdydLOaK9kx2h
4XXyPUTTepuivpnpiI4KqMcaWYQzmot5eoSOOQL6E5hb09oRXY+IhlaynFg0l/E7
t5uMMUopmcfOH6OGMXTCFXebKbWGnzHx83bXkyzMWWc1p4X+aP18dewHsYuwZOdx
1goNZNNz0BaJq2y0RYnfYeNOLV6d+S6BAMAUkWbABdols8Pi8ezsPwZ8x/1vk7uy
tUOmiuMkLsC6LzJnnUaoGR3tflCH+yU3XSPQpnZYzaFaeA3d6mgV93w7y3Jreavx
tHkCAwEAAaMTMBEwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEA
dZ9vGVJaAauomoDp9VY4zOr0G4n7WnEElqMAxOQPzLJwRXe81/GchmUKWvX5Fc6o
6RiEa7Nw4YiXKyFMqoJbQN3j8EkOiHs1FtrwJNsobzmlVmjuqxqCBWmVQPpUfOQh
f6I/gQr2BVxvNsj+IvuI0vIVjP5J3GBxL9ySvFKsfp4xtk1oTHIuA2G3haIv2AJp
j/Hm7nVvoXWrb/zX+fagi0rrf+3hDCsHMXtxaxXk2sGRLKHgkTYTVwEPQ6SKEqrW
qnSOx+SMl4up6AVfEq6kVR8ZIt/CzJBWZ4qYQnOf0eK4KQC6UB22adzsaFMmhzRB
YZQy1bHIhscLf8wjTYbzAg==
-----END CERTIFICATE-----
        """
        When I run command on host "mysql1"
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
        Then zookeeper node "/test/last_switch" should match json within "60" seconds
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
        And mysql host "mysql2" should be writable
        When I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match json witch I will save as "external_repl_status"
        """
        {
            "Replica_IO_State": "Connecting to source",
            "Source_Host": "test_source_2",
            "Source_Port": 2222,
            "Source_User": "test_user_2",
            "Replica_IO_Running": "Connecting",
            "Replica_SQL_Running": "Yes",
            "Relay_Source_Log_File": "",
            "Exec_Source_Log_Pos": 0,
            "Channel_Name": "external",
            "Replicate_Ignore_DB": "mysql",
            "Source_SSL_CA_File": "/etc/mysql/ssl/external_CA.pem",
            "Replicate_Do_DB": "testdb1"
        }
        """
        And host "mysql2" should have file "/etc/mysql/ssl/external_CA.pem" within "10" seconds

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
        Then SQL result should match json
        """
        [{
            "source_host": "test_source_2",
            "source_user": "test_user_2",
            "source_password": "test_pass_2",
            "source_port": 2222
        }]
        """
        When I run SQL on mysql host "mysql2"
        """
            CREATE TABLE mysql.replication_sources(
                channel_name VARCHAR(50) NOT NULL,
                source_host VARCHAR(50) NOT NULL,
                priority INT NOT NULL DEFAULT 0,
                PRIMARY KEY (channel_name, source_host)
            ) ENGINE=INNODB
        """
        Then I wait for "60" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status"
        When I run SQL on mysql host "mysql2"
        """
            INSERT INTO mysql.replication_sources (channel_name, source_host, priority) VALUES 
            ('external', 'test_source', 100),
            ('external', 'test_source_2', 50),
            ('external', 'test_source_3', 10)
        """
        Then I wait for "45" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status"
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_3"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_3"}
        """
        Then I wait for "45" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_2"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_2"}
        """
        Then I wait for "45" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
        Then I wait for "20" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_3"}
        """
        Then I wait for "16" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source_3"}
        """
        Then I wait for "45" seconds
        And I run SQL on mysql host "mysql2"
        """
            SHOW REPLICA STATUS FOR CHANNEL 'external'
        """
        Then SQL result should match saved json text on path "external_repl_status" with following changes
        """
        {"Source_Host": "test_source"}
        """
