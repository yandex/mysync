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
        SHOW REPLICA STATUS FOR CHANNEL "external"
    """
