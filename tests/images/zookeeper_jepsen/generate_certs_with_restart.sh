#!/bin/bash
set -ex

# Wait for supervisord to be ready
for i in $(seq 1 30); do
    supervisorctl status && break || true
    sleep 1
done

supervisorctl stop zookeeper
ps -aux | grep [z]oo.cfg | awk '{print $2}' | xargs kill || true
/var/lib/dist/base/generate_certs.sh $1
supervisorctl start zookeeper
