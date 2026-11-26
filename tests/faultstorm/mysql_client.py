import logging
import time

import pymysql

from faultstorm.cluster import ClusterManager
from faultstorm.db_client import DatabaseClient


class MySQLClient(DatabaseClient):
    nodes = ["mysql1", "mysql2", "mysql3"]

    def get_db_nodes(self):
        return self.nodes

    def connect(self, node):
        return pymysql.connect(
            host=node, user="admin", password="admin_pwd", database="test1",
            autocommit=True, connect_timeout=3, read_timeout=5, write_timeout=5,
        )

    def setup(self, node):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS faultstorm_set (value BIGINT PRIMARY KEY)")
            cursor.execute("TRUNCATE TABLE faultstorm_set")

    def add(self, node, value):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("INSERT INTO faultstorm_set VALUES (%s)", (value,))

    def read(self, node):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("SELECT value FROM faultstorm_set")
            return {row[0] for row in cursor.fetchall()}

    def recovery_state(self, node):
        with self.connect(node) as db, db.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT @@GLOBAL.read_only AS read_only, @@GLOBAL.offline_mode AS offline_mode, "
                           "@@GLOBAL.gtid_executed AS gtid_executed")
            state = cursor.fetchone()

            try:
                cursor.execute("SHOW REPLICA STATUS")
            except pymysql.err.ProgrammingError as error:
                if error.args[0] != 1064:  # MySQL 5.7 uses the old command name.
                    raise
                cursor.execute("SHOW SLAVE STATUS")
            replica = cursor.fetchone()
            state["source"] = replica.get("Source_Host", replica.get("Master_Host")) if replica else None
            state["io"] = replica.get("Replica_IO_Running", replica.get("Slave_IO_Running")) if replica else None
            state["sql"] = replica.get("Replica_SQL_Running", replica.get("Slave_SQL_Running")) if replica else None

            if not state["read_only"]:
                cursor.execute("SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status "
                               "WHERE VARIABLE_NAME IN ('Rpl_semi_sync_master_status', 'Rpl_semi_sync_source_status', "
                               "'Rpl_semi_sync_master_clients', 'Rpl_semi_sync_source_clients')")
                semisync = {row["VARIABLE_NAME"].lower(): row["VARIABLE_VALUE"] for row in cursor.fetchall()}
                state["semisync"] = any(
                    value.upper() in ("ON", "1") for key, value in semisync.items() if key.endswith("_status")
                )
                state["semisync_clients"] = max(
                    (int(value) for key, value in semisync.items() if key.endswith("_clients")), default=0
                )
            return state

    def has_executed(self, node, gtid_set):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed)", (gtid_set,))
            return bool(cursor.fetchone()[0])

    def wait_for_recovery(self, timeout=600):
        deadline = time.monotonic() + timeout
        target = None
        last_status = None
        while time.monotonic() < deadline:
            states = {}
            errors = {}
            for node in self.nodes:
                try:
                    states[node] = self.recovery_state(node)
                except Exception as error:
                    errors[node] = str(error)

            masters = [node for node, state in states.items() if not state["read_only"]]
            status = {"roles": {node: "master" if node in masters else "replica" for node in states}, "errors": errors}
            if not errors and len(masters) == 1:
                master = masters[0]
                replicas = [node for node in self.nodes if node != master]
                source = states[master]
                if target is None or target[0] != master:
                    target = (master, source["gtid_executed"])
                status.update({"master": master, "semisync": source["semisync"],
                               "semisync_clients": source["semisync_clients"],
                               "replication": {node: (states[node]["source"], states[node]["io"], states[node]["sql"])
                                               for node in replicas}})
                if (not source["offline_mode"] and source["semisync"] and source["semisync_clients"] >= 1 and
                        all(states[node]["read_only"] and states[node]["source"] == master and
                            states[node]["io"] == "Yes" and states[node]["sql"] == "Yes" for node in replicas)):
                    try:
                        caught_up = {node: self.has_executed(node, target[1]) for node in replicas}
                        pending_resetup = []
                        for node in self.nodes:
                            try:
                                ClusterManager.exec_on_node(
                                    node, ["sh", "-c", "test ! -e /tmp/mysync.resetup"], timeout=3
                                )
                            except Exception:
                                pending_resetup.append(node)
                        dcs_info = ClusterManager.exec_on_node(
                            self.nodes[0], ["mysync", "info", "--short"], timeout=10
                        ).splitlines()
                        pending_switch = any(line.startswith("switch:") for line in dcs_info)
                        dcs_master = next((line.partition(":")[2].strip() for line in dcs_info
                                           if line.startswith("master:")), None)
                        status.update({"caught_up": caught_up, "pending_resetup": pending_resetup,
                                       "pending_switch": pending_switch, "dcs_master": dcs_master})
                        if (all(caught_up.values()) and not pending_resetup and not pending_switch and
                                dcs_master == master):
                            logging.info("Cluster recovered: master=%s, GTID=%s", master, target[1])
                            return
                    except Exception as error:
                        status["catchup_error"] = str(error)
            else:
                target = None

            if status != last_status:
                logging.info("Waiting for cluster recovery: %s", status)
                last_status = status
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise RuntimeError(f"cluster did not recover before reading within {timeout}s; last state: {last_status}")

    def is_definite_failure(self, exc):
        return isinstance(exc, pymysql.err.OperationalError) and exc.args[0] in (1290, 1792)
