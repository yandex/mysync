FROM ubuntu:bionic
ENV container docker
ENV DEBIAN_FRONTEND noninteractive
ENV ZK_VERSION=3.7.1
ENV MYSQL_VERSION=5.7
COPY . /var/lib/dist/base
RUN bash /var/lib/dist/base/setup.sh
CMD ["/usr/local/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
