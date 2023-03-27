set -e

chown mysql:root /etc/mysql
touch /etc/mysync.yaml
chown mysql:mysql /etc/mysync.yaml
if [[ "$VERSION" == "8-0" ]]; then
  cp /var/lib/dist/mysql/my.cnf.8.0 /etc/mysql/my.cnf
else
  cp /var/lib/dist/mysql/my.cnf /etc/mysql/my.cnf
fi

cat <<EOF >> /etc/mysql/my.cnf
  rpl_semi_sync_master_timeout = 31536000000
  rpl_semi_sync_master_wait_for_slave_count = 1
  rpl_semi_sync_master_wait_no_slave = ON
  rpl_semi_sync_master_wait_point = AFTER_SYNC
EOF

cp /var/lib/dist/mysql/.my.cnf /root/.my.cnf
cp /var/lib/dist/mysql/supervisor_mysql.conf /etc/supervisor/conf.d
