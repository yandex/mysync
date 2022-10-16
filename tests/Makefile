ARCADIA_ROOT=../../../..
YA_BIN=$(ARCADIA_ROOT)/ya
PROJECT=mysync

base_img:
	docker build --tag=registry.yandex.net/dbaas/mysync-test-base images/base
	docker push registry.yandex.net/dbaas/mysync-test-base

jepsen_base:
	docker build --tag=registry.yandex.net/dbaas/mysync-jepsen-test-base images/jepsen_common
	docker push registry.yandex.net/dbaas/mysync-jepsen-test-base

test:
	$(YA_BIN) make --target-platform=default-linux-x86_64 ../cmd/mysync
	$(YA_BIN) make .
	rm -rf ./logs
	mkdir ./logs
	$(YA_BIN) tool go test -timeout 150m

jepsen_test:
	$(YA_BIN) make --target-platform=default-linux-x86_64 ../cmd/mysync
	$(YA_BIN) make
	rm -fr ./images/mysql_jepsen/mysync && cp ../cmd/mysync/mysync ./images/mysql_jepsen/mysync
	docker-compose -p $(PROJECT) -f images/jepsen-compose.yml up -d --force-recreate --build
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql1_1
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql2_1
	timeout 600 docker exec mysync_zoo1_1 retriable_path_create.sh /test/ha_nodes/mysync_mysql3_1
	timeout 600 docker exec mysync_mysql1_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql2_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql3_1 supervisorctl start mysqld
	timeout 600 docker exec mysync_mysql1_1 setup.sh
	mkdir -p logs
	(docker exec mysync_jepsen_1 /root/jepsen/run.sh >logs/jepsen.log 2>&1 && tail -n 4 logs/jepsen.log) || ./images/jepsen_main/save_logs.sh
	docker-compose -p $(PROJECT) -f images/jepsen-compose.yml down --rmi all

clean:
	docker ps | grep mysync | awk '{print $$1}' | xargs docker rm -f || true
	docker network ls | grep mysync | awk '{print $$1}' | xargs docker network rm || true
	rm -rf ./logs
