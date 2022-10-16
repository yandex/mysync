FROM mysync-jepsen-test-base:latest
COPY . /var/lib/dist/zookeeper
COPY ./supervisor_zookeeper.conf /etc/supervisor/conf.d
COPY ./retriable_path_create.sh /usr/local/bin/retriable_path_create.sh
