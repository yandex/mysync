#!/bin/bash

for i in 1 2 3
do
    mkdir -p logs/mysql${i}
    mkdir -p logs/zookeeper${i}

    for logfile in /var/log/mysync.log /var/log/mysql/error.log /var/log/mysql/query.log /var/log/resetup.log /var/log/supervisor.log
    do
        logname=$(echo "${logfile}" | rev | cut -d/ -f1 | rev)
        docker exec mysync_mysql${i}_1 cat "${logfile}" > "logs/mysql${i}/${logname}"
    done

    docker exec mysync_zoo${i}_1 cat /var/log/zookeeper/zookeeper--server-mysync_zookeeper${i}_1.log > logs/zookeeper${i}/zk.log 2>&1
done

tail -n 18 logs/jepsen.log
# Explicitly fail here
exit 1
