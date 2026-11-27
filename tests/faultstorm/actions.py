import logging
import random

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction
from faultstorm.network_latency import freeze_drop_restore_for_ips


def _master(node):
    info = ClusterManager.exec_on_node(node, ["mysync", "info", "--short"])
    return next(line.split(":", 1)[1].strip() for line in info.splitlines()
                if line.startswith("master:"))


def _has_active_replica(info, master):
    for line in info.partition("active_nodes:\n")[2].splitlines():
        if not line.startswith(" "):
            break
        if line.strip().startswith("- ") and line.strip()[2:] != master:
            return True
    return False


class SwitchoverAction(FaultAction):
    name = "switchover"

    def __init__(self, db_nodes, extra_nodes, ordinal=0, dc_map=None, node=None):
        super().__init__(db_nodes, extra_nodes, ordinal, dc_map)
        self.node = node

    def execute(self, stop_event=None):
        if self.node is None:
            master = _master(self.db_nodes[0])
            self.node = random.choice([node for node in self.db_nodes if node != master])
        result = ClusterManager.exec_on_node(self.db_nodes[0],
                                             ["mysync", "switch", "--to", self.node, "--wait=0s"])
        logging.info("Switchover to %s: %s", self.node, result.strip())

    def serialize(self):
        return f"{self.ordinal} {self.node or ''}".strip()

    @classmethod
    def deserialize(cls, params, db_nodes, extra_nodes, dc_map=None):
        parts = params.split()
        return cls(db_nodes, extra_nodes, int(parts[0]), dc_map,
                   parts[1] if len(parts) > 1 else None)


class MaintenanceAction(FaultAction):
    name = "maintenance"
    healable = True

    def execute(self, stop_event=None):
        result = ClusterManager.exec_on_node(self.db_nodes[0],
                                             ["mysync", "maintenance", "on", "--wait=10s"], timeout=15)
        logging.info("Maintenance: %s", result.strip())

    def heal(self):
        result = ClusterManager.exec_on_node(self.db_nodes[0],
                                             ["mysync", "maintenance", "off", "--wait=30s"], timeout=35)
        logging.info("Maintenance: %s", result.strip())

    def serialize(self):
        return str(self.ordinal)

    @classmethod
    def deserialize(cls, params, db_nodes, extra_nodes, dc_map=None):
        return cls(db_nodes, extra_nodes, int(params), dc_map)


class ResetupAction(SwitchoverAction):
    name = "resetup"
    host_targetable = True
    destructive = True

    def execute(self, stop_event=None):
        if self.node is None:
            self.node = random.choice(self.db_nodes)
        other_ips = {ClusterManager.get_node_ip(node) for node in self.db_nodes if node != self.node}
        requested = False

        def wipe():
            nonlocal requested
            info = ClusterManager.exec_on_node(self.node, ["mysync", "info", "--short"])
            master = next(line.split(":", 1)[1].strip() for line in info.splitlines()
                          if line.startswith("master:"))
            if self.node == master and not _has_active_replica(info, master):
                logging.info("Resetup skipped on %s: no active replica", master)
                return
            ClusterManager.exec_on_node(self.node, [
                "sh", "-c",
                "find /var/lib/mysql -mindepth 1 -maxdepth 1 ! -name auto.cnf -exec rm -rf -- {} + && "
                "touch /tmp/mysync.resetup && "
                "(supervisorctl stop mysqld || true)",
            ], timeout=60)
            requested = True

        if other_ips:
            freeze_drop_restore_for_ips(self.node, other_ips, pre_kill_sleep=5, kill_callback=wipe)
        else:
            wipe()
        if requested:
            logging.info("Resetup requested on %s", self.node)
