PROJECT=mysync

base_img:
	docker build --tag=mysync-test-base images/base
	#docker push mysync-test-base

jepsen_base:
	docker build --tag=mysync-jepsen-test-base images/jepsen_common
	#docker push mysync-jepsen-test-base

test:
	GOOS=linux go build -o ../cmd/mysync/mysync ../cmd/mysync
	go build ./...
	rm -rf ./logs
	mkdir ./logs
	go test -timeout 150m

jepsen_test:
	GOOS=linux go build -o ../cmd/mysync/mysync ../cmd/mysync
	go build ./...
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
