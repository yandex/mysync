#!/bin/bash

set -x
set -e

semisync_dialect="${SEMISYNC_DIALECT_OVERRIDE:-${SEMISYNC_DIALECT:-sourceReplica}}"
if [[ "$semisync_dialect" != "sourceReplica" ]]; then
    echo "unsupported SEMISYNC_DIALECT for MySQL 9.7: $semisync_dialect" >&2
    exit 1
fi

cat <<EOF > /tmp/mysync-semisync.cnf
[mysqld]
plugin_load_add = 'rpl_semi_sync_source=semisync_source.so;rpl_semi_sync_replica=semisync_replica.so'
rpl_semi_sync_source_timeout = 31536000000
rpl_semi_sync_source_wait_for_replica_count = 1
rpl_semi_sync_source_wait_no_replica = ON
rpl_semi_sync_source_wait_point = AFTER_SYNC
EOF

# MySQL 9.7 creates INFORMATION_SCHEMA system views after executing init_file.
# Enabling super_read_only in init_file blocks those internal CREATE VIEW
# statements. The normal server start enables it from my.cnf.9.7 instead.
cat <<EOF > /etc/mysql/init.sql
   SET GLOBAL super_read_only = 0;
   CREATE USER $MYSQL_ADMIN_USER@'%' IDENTIFIED WITH caching_sha2_password BY '$MYSQL_ADMIN_PASSWORD';
   GRANT ALL ON *.* TO $MYSQL_ADMIN_USER@'%' WITH GRANT OPTION;
   CREATE USER repl@'%' IDENTIFIED WITH caching_sha2_password BY 'repl_pwd';
   CREATE USER user@'%' IDENTIFIED WITH caching_sha2_password BY 'user_pwd';
   GRANT ALL ON *.* TO user@'%';
   GRANT REPLICATION SLAVE ON *.* TO repl@'%';
   CREATE DATABASE test1;
   RESET BINARY LOGS AND GTIDS;
EOF

if [ ! -f /etc/mysql/slave.sql ]; then
    if [ -n "$MYSQL_MASTER" ]; then
    cat <<EOF > /etc/mysql/slave.sql
        SET GLOBAL server_id = $MYSQL_SERVER_ID;
        RESET REPLICA FOR CHANNEL '';
        CHANGE REPLICATION SOURCE TO SOURCE_HOST = '$MYSQL_MASTER', SOURCE_USER = 'repl', SOURCE_PASSWORD = 'repl_pwd', SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1, SOURCE_CONNECT_RETRY = 1, SOURCE_RETRY_COUNT = 100500 FOR CHANNEL '';
        START REPLICA;
EOF
    else
        touch /etc/mysql/slave.sql
    fi
else
    : > /etc/mysql/slave.sql
fi

if [ ! -f /var/lib/mysql/auto.cnf ]; then
    /usr/sbin/mysqld --defaults-file=/etc/mysql/init.cnf \
    --initialize --datadir=/var/lib/mysql --init-file=/etc/mysql/init.sql --server-id="$MYSQL_SERVER_ID"
    echo "==INITIALIZED=="
fi

# workaround for docker on mac
chown -R mysql:mysql /var/lib/mysql
find /var/lib/mysql -type f -exec touch {} +

echo "==STARTING=="
exec /usr/sbin/mysqld --defaults-file=/etc/mysql/my.cnf --datadir=/var/lib/mysql --init-file=/etc/mysql/slave.sql --server-id="$MYSQL_SERVER_ID" --report-host="$(hostname)"
