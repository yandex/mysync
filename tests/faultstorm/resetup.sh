#!/bin/bash
set -euo pipefail

resetup() {
    master=$(mysync info --short | awk '$1 == "master:" {print $2}')
    if [ -z "$master" ] || [ "$master" = "$(hostname)" ]; then
        echo "resetup deferred: no separate master"
        return 1
    fi
    mysqladmin -h "$master" --user=admin --password=admin_pwd ping >/dev/null
    echo "resetup from $master started"
    supervisorctl stop mysqld || true
    find /var/lib/mysql -mindepth 1 -maxdepth 1 ! -name auto.cnf -exec rm -rf -- {} +
    ssh -o StrictHostKeyChecking=no root@"$master" \
        'xtrabackup --backup --stream=xbstream' | sudo -u mysql xbstream --extract --directory=/var/lib/mysql
    sudo -u mysql xtrabackup --prepare --target-dir=/var/lib/mysql
    supervisorctl start mysqld
    for i in $(seq 1 120); do
        mysql -e 'SELECT 1' >/dev/null 2>&1 && break
        sleep 1
    done
    mysql -e 'SELECT 1' >/dev/null
    gtid=$(awk '{print $3}' /var/lib/mysql/xtrabackup_binlog_info)
    mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtid'"
    rm /tmp/mysync.resetup
    echo "resetup from $master completed"
}

while sleep 2; do
    [ -e /tmp/mysync.resetup ] || continue
    resetup || echo "resetup failed; retrying"
done
