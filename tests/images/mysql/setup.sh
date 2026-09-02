set -e

chown mysql:root /etc/mysql
touch /etc/mysync.yaml
chown mysql:mysql /etc/mysync.yaml
if [[ "$VERSION" == "8.0" ]]; then
  mkdir /etc/mysql/ssl
  chown mysql:mysql /etc/mysql/ssl
  cp /var/lib/dist/mysql/my.cnf.8.0 /etc/mysql/my.cnf
  cp /var/lib/dist/mysql/my.cnf.8.0 /etc/mysql/init.cnf
cat <<EOF >> /etc/mysql/my.cnf
rpl_semi_sync_master_timeout = 31536000000
rpl_semi_sync_master_wait_for_slave_count = 1
rpl_semi_sync_master_wait_no_slave = ON
rpl_semi_sync_master_wait_point = AFTER_SYNC
EOF
elif [[ "$VERSION" == "8.4" || "$VERSION" == "9.7" ]]; then
  mkdir /etc/mysql/ssl
  chown mysql:mysql /etc/mysql/ssl
  cp "/var/lib/dist/mysql/my.cnf.$VERSION" /etc/mysql/my.cnf
  cp "/var/lib/dist/mysql/my.cnf.$VERSION" /etc/mysql/init.cnf
  # Semi-sync plugins and their variables are only needed by the running server.
  # Keep them out of init.cnf: mysqld --initialize must use the base config only.
  if grep -qF 'mysync-semisync.cnf' /etc/mysql/init.cnf; then
    echo "runtime semi-sync config must not be included from init.cnf" >&2
    exit 1
  fi
  cat <<EOF >> /etc/mysql/my.cnf

!include /tmp/mysync-semisync.cnf
EOF
else
  cp /var/lib/dist/mysql/my.cnf /etc/mysql/my.cnf
  cp /var/lib/dist/mysql/my.cnf /etc/mysql/init.cnf
fi

cp /var/lib/dist/mysql/.my.cnf /root/.my.cnf
if [[ "$VERSION" == "8.4" ]]; then
  cp /var/lib/dist/mysql/supervisor_mysql.conf.8.4 /etc/supervisor/conf.d/supervisor_mysql.conf
elif [[ "$VERSION" == "9.7" ]]; then
  cp /var/lib/dist/mysql/supervisor_mysql.conf.9.7 /etc/supervisor/conf.d/supervisor_mysql.conf
else
  cp /var/lib/dist/mysql/supervisor_mysql.conf /etc/supervisor/conf.d/supervisor_mysql.conf
fi
