build:
	go build -o ./cmd/mysync/mysync ./cmd/mysync/...

unittests:
	go test ./cmd/...
	go test ./internal/...

base_img:
	docker build --tag=mysync-test-base tests/images/base

jepsen_base_img:
	docker build --tag=mysync-jepsen-test-base tests/images/jepsen_common

test:
	GOOS=linux go build -o ./cmd/mysync/mysync ./cmd/mysync/...
	go build ./tests/...
	rm -fr ./tests/images/mysql/mysync && cp ./cmd/mysync/mysync ./tests/images/mysql/mysync
	rm -rf ./tests/logs
	mkdir ./tests/logs
	(cd tests; go test -timeout 150m)

jepsen_test:
	GOOS=linux go build -o ./cmd/mysync/mysync ./cmd/mysync/...
	go build ./tests/...
	rm -fr ./tests/images/mysql_jepsen/mysync && cp ./cmd/mysync/mysync ./tests/images/mysql_jepsen/mysync
	docker-compose -p mysync -f ./tests/images/jepsen-compose.yml up -d --force-recreate --build
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql1_1
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql2_1
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql3_1
	timeout 600 docker exec mysync_mysql1_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql2_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql3_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql1_1 setup.sh
	mkdir -p ./tests/logs
	(docker exec mysync_jepsen_1 /root/jepsen/run.sh > ./tests/logs/jepsen.log 2>&1 && tail -n 4 ./tests/logs/jepsen.log) || ./tests/images/jepsen_main/save_logs.sh
	docker-compose -p mysync -f ./tests/images/jepsen-compose.yml down --rmi all

clean:
	docker ps | grep mysync | awk '{print $$1}' | xargs docker rm -f || true
	docker network ls | grep mysync | awk '{print $$1}' | xargs docker network rm || true
	rm -rf ./tests/logs
