set -e

chown mysql:root /etc/mysql
touch /etc/mysync.yaml
chown mysql:mysql /etc/mysync.yaml 
cp /var/lib/dist/mysql/my.cnf /etc/mysql/my.cnf
cp /var/lib/dist/mysql/.my.cnf /root/.my.cnf
cp /var/lib/dist/mysql/supervisor_mysql.conf /etc/supervisor/conf.d
