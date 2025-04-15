#!/bin/bash

set -x
set -e

cat <<EOF > /etc/mysql/init.sql
   SET GLOBAL super_read_only = 0;
   CREATE USER $MYSQL_ADMIN_USER@'%' IDENTIFIED BY '$MYSQL_ADMIN_PASSWORD';
   GRANT ALL ON *.* TO $MYSQL_ADMIN_USER@'%' WITH GRANT OPTION;
   CREATE USER repl@'%' IDENTIFIED BY 'repl_pwd';
   CREATE USER user@'%' IDENTIFIED BY 'user_pwd';
   GRANT ALL ON *.* TO user@'%';
   GRANT REPLICATION SLAVE ON *.* TO repl@'%';
   CREATE DATABASE test1;
   RESET BINARY LOGS AND GTIDS;
   SET GLOBAL super_read_only = 1;
EOF

if [ ! -f /etc/mysql/slave.sql ]; then
    if [ ! -z "$MYSQL_MASTER" ]; then
    cat <<EOF > /etc/mysql/slave.sql
        SET GLOBAL server_id = $MYSQL_SERVER_ID;
        RESET REPLICA FOR CHANNEL '';
        CHANGE REPLICATION SOURCE TO SOURCE_HOST = '$MYSQL_MASTER', SOURCE_USER = 'repl', SOURCE_PASSWORD = 'repl_pwd', SOURCE_AUTO_POSITION = 1, SOURCE_CONNECT_RETRY = 1, SOURCE_RETRY_COUNT = 100500 FOR CHANNEL '';
        START REPLICA;
EOF
    else
        touch /etc/mysql/slave.sql
    fi
else
    echo "" > /etc/mysql/slave.sql
fi

if [ ! -f /var/lib/mysql/auto.cnf ]; then
    /usr/sbin/mysqld --defaults-file=/etc/mysql/init.cnf \
    --initialize --datadir=/var/lib/mysql --init-file=/etc/mysql/init.sql --server-id=$MYSQL_SERVER_ID || true
    echo "==INITIALIZED=="
fi

# workaround for docker on mac
chown -R mysql:mysql /var/lib/mysql
find /var/lib/mysql -type f -exec touch {} +

echo "==STARTING=="
exec /usr/sbin/mysqld --defaults-file=/etc/mysql/my.cnf --datadir=/var/lib/mysql --init-file=/etc/mysql/slave.sql --server-id=$MYSQL_SERVER_ID --report-host=`hostname`
