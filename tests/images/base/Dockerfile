FROM ubuntu:bionic
ENV container docker
ENV DEBIAN_FRONTEND noninteractive
ENV ZK_VERSION=3.7.1
ARG MYSQL_VERSION=""
ENV MYSQL_VERSION=${MYSQL_VERSION}
COPY . /var/lib/dist/base
RUN bash /var/lib/dist/base/setup.sh
CMD ["/usr/local/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
