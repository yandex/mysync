#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."
export MYSYNC_NET_PREFIX=${MYSYNC_NET_PREFIX:-192.168.234}
compose="docker compose -p mysync-faultstorm -f tests/images/docker-compose.yaml -f tests/faultstorm/compose.yaml"
mkdir -p tests/logs/faultstorm
docker build --platform=linux/amd64 -t "mysync-test-base${VERSION}" tests/images/base --build-arg "MYSQL_VERSION=$VERSION"
GOOS=linux GOARCH=amd64 go build -o tests/images/mysql/mysync ./cmd/mysync
trap '$compose logs --no-color >tests/logs/faultstorm/containers.log 2>&1 || true; $compose down --remove-orphans' EXIT
$compose up -d --build --wait
for node in zoo1 zoo2 zoo3 mysql1 mysql2 mysql3; do
  $compose exec -T faultstorm docker exec "mysync-faultstorm-$node" iptables -N FSTORM_PROBE
  $compose exec -T faultstorm docker exec "mysync-faultstorm-$node" iptables -X FSTORM_PROBE
done
{
  printf 'addauth digest testuser:testpassword123\n'
  for path in /test /test/ha_nodes /test/ha_nodes/mysql1 /test/ha_nodes/mysql2 /test/ha_nodes/mysql3; do
    printf 'create %s\nsetAcl %s auth:testuser:testpassword123:crwad\n' "$path" "$path"
  done
  printf 'quit\n'
} | $compose exec -T zoo1 /opt/zookeeper/bin/zkCli.sh -server zoo1:2181 >tests/logs/faultstorm/zk-bootstrap.log
for node in mysql1 mysql2 mysql3; do
  $compose exec -T "$node" sh -c '/var/lib/dist/base/generate_certs.sh && supervisorctl restart mysync' >/dev/null 2>&1
done
$compose exec -T faultstorm python main.py "$@"
