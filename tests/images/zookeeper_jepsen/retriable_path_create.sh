#!/bin/bash

if [ "$1" == "" ]
then
    echo "Usage $(basename "${0}") <path in zk>"
    exit 1
fi

retry_create() {
    tries=0
    ret=1
    while [ ${tries} -le 60 ]
    do
        if /opt/zookeeper/bin/zkCli.sh create "${1}"
        then
            ret=0
            break
        else
            tries=$(( tries + 1 ))
            sleep 1
        fi
    done
    return ${ret}
}

retry_create "${1}"
