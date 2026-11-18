import argparse
import logging
import sys
import time

import pymysql

from faultstorm.cluster import ClusterManager
from faultstorm.config import TestConfig
from faultstorm.faults.actions import create_default_registry
from faultstorm.runner import TestRunner

from actions import MaintenanceAction, ResetupAction, SwitchoverAction
from mysql_client import MySQLClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-duration", type=int, default=180)
    parser.add_argument("--read-duration", type=int, default=60)
    parser.add_argument("--replay-scenario")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    ClusterManager.container_template = "mysync-faultstorm-{node}"
    client = MySQLClient()
    config = TestConfig(
        db_nodes=client.nodes,
        extra_nodes=["zoo1", "zoo2", "zoo3"],
        load_node="faultstorm",
        write_phase_duration=args.write_duration,
        read_phase_duration=args.read_duration,
        fault_active_duration=30,
        fault_pause_duration=10,
        fault_types=["partition_random_node", "partition_random_halves",
                     "partition_majorities_ring", "partition_random_subnet", "partition_random_dc",
                     "kill", "freeze_processes", "freeze_processes_group",
                     "switchover", "maintenance", "resetup"],
        action_params={
            "kill": {"processes": ["mysqld", "/usr/bin/mysync"]},
            "freeze_processes": {"processes": ["mysqld", "/usr/bin/mysync", "zookeeper"]},
            "freeze_processes_group": {"processes": ["mysqld", "/usr/bin/mysync", "zookeeper"]},
        },
        parallel_faults_count=1,
        complex_faults_enabled=False,
        add_interval=0.1,
        writers_per_node=1,
        operations_log="/logs/operations.log",
        scenario_log="/logs/scenario.log",
        replay_scenario=args.replay_scenario,
    )

    # The replica containers start after mysql1; wait for all three databases.
    for node in client.nodes:
        for _ in range(60):
            try:
                with client.connect(node) as db, db.cursor() as cursor:
                    cursor.execute("SELECT 1")
                break
            except pymysql.Error:
                time.sleep(2)
        else:
            raise RuntimeError(f"{node} did not start")

    for _ in range(60):
        for node in client.nodes:
            try:
                client.setup(node)
                break
            except pymysql.Error:
                pass
        else:
            time.sleep(2)
            continue
        break
    else:
        raise RuntimeError("no writable MySQL node")

    dc_map = ClusterManager.build_dc_map(config.all_nodes)
    if len(dc_map) != 3:
        raise RuntimeError(f"incomplete DC map: {dc_map}")
    registry = create_default_registry()
    registry.register(SwitchoverAction).register(MaintenanceAction).register(ResetupAction)
    result = TestRunner(config, client, registry, dc_map).run()
    logging.info("valid=%s successful_adds=%d attempts=%d", result.valid,
                 result.successful_adds, result.total_attempts)
    if not result.valid or result.successful_adds == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
