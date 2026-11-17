import logging
import random
import time

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction


def _master(node):
    info = ClusterManager.exec_on_node(node, ["mysync", "info", "--short"])
    return next(line.split(":", 1)[1].strip() for line in info.splitlines()
                if line.startswith("master:"))


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
    destructive = True

    def execute(self, stop_event=None):
        master = _master(self.db_nodes[0])
        if self.node is None:
            self.node = random.choice([node for node in self.db_nodes if node != master])
        if self.node == master:
            raise ValueError("resetup target is master")
        ClusterManager.exec_on_node(self.node, ["touch", "/tmp/mysync.resetup"])
        logging.info("Resetup requested on %s", self.node)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if ClusterManager.exec_on_node(self.node,
                                           ["sh", "-c", "test ! -e /tmp/mysync.resetup || echo pending"]).strip() != "pending":
                logging.info("Resetup completed on %s", self.node)
                return
            time.sleep(2)
        raise TimeoutError(f"resetup on {self.node} did not complete")
