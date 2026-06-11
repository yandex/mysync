FROM mysync-jepsen-test-base:latest
ENV MYSQL_VERSION="8.0"
COPY . /var/lib/dist/mysql
COPY ./sh-scripts/my-wait-started.sh /usr/bin/my-wait-started
COPY ./mysync /usr/bin/mysync
COPY ./sh-scripts/my-resetup.sh /usr/bin/my-resetup
COPY ./sh-scripts/my-resetup-wd.sh /usr/bin/my-resetup-wd
COPY ./setup.sh /usr/local/bin/setup.sh
COPY ./.my.cnf /root/.my.cnf
COPY ./supervisor_mysql.conf /etc/supervisor/conf.d
RUN chown mysql:root /etc/mysql
RUN touch /etc/mysync.yaml
RUN chown mysql:mysql /etc/mysync.yaml
COPY ./my.cnf.8.0 /etc/mysql/my.cnf

RUN chmod +x /usr/bin/my-resetup

ENV MYSQL_ADMIN_USER=admin
ENV MYSQL_ADMIN_PASSWORD=admin_pwd
