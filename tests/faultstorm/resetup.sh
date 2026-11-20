#!/bin/bash
set -euo pipefail

resetup() {
    supervisorctl stop mysqld >/dev/null 2>&1 || true
    state=$(supervisorctl status mysqld 2>/dev/null || true)
    [ "$(echo "$state" | awk '$1 == "mysqld" {print $2}')" = STOPPED ] || {
        echo "resetup deferred: mysqld not stopped ($state)"
        return 1
    }
    process_status=0
    pgrep -x mysqld >/dev/null 2>&1 || process_status=$?
    [ "$process_status" -eq 1 ] || {
        echo "resetup deferred: mysqld process still running or state unknown"
        return 1
    }
    master=$(mysync info --short | awk '$1 == "master:" {print $2}') || return 1
    if [ -z "$master" ] || [ "$master" = "$(hostname)" ]; then
        echo "resetup deferred: no separate master"
        return 1
    fi
    mysqladmin -h "$master" --user=admin --password=admin_pwd ping >/dev/null || return 1
    echo "resetup from $master started"
    find /var/lib/mysql -mindepth 1 -maxdepth 1 ! -name auto.cnf -exec rm -rf -- {} + || return 1
    ssh -o StrictHostKeyChecking=no root@"$master" \
        'xtrabackup --backup --stream=xbstream' | sudo -u mysql xbstream --extract --directory=/var/lib/mysql || return 1
    sudo -u mysql xtrabackup --prepare --target-dir=/var/lib/mysql || return 1
    supervisorctl start mysqld || return 1
    for i in $(seq 1 120); do
        mysql -e 'SELECT 1' >/dev/null 2>&1 && break
        sleep 1
    done
    mysql -e 'SELECT 1' >/dev/null || return 1
    gtid=$(awk '{print $3}' /var/lib/mysql/xtrabackup_binlog_info) || return 1
    if [ "$VERSION" = "8.4" ] || [ "$VERSION" = "9.7" ]; then
        mysql -e "RESET BINARY LOGS AND GTIDS; SET @@GLOBAL.GTID_PURGED='$gtid'" || return 1
    else
        mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtid'" || return 1
    fi
    rm /tmp/mysync.resetup || return 1
    echo "resetup from $master completed"
}

while sleep 2; do
    [ -e /tmp/mysync.resetup ] || continue
    resetup || echo "resetup failed; retrying"
done
