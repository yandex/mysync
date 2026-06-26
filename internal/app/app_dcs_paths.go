package app

// ZooKeeper path constants used by mysync business logic.
// All paths are relative to the DCS root configured in config.
const (
	// manager's lock
	pathManagerLock = "manager"

	pathMasterNode = "master"

	// def 1: activeNodes are master + alive running HA replicas ???
	// def 2: active nodes is last iteration surely ok ones
	// structure: list of hosts(strings)
	pathActiveNodes = "active_nodes"

	// structure: pathHealthPrefix/hostname -> NodeState
	pathHealthPrefix = "health"

	// structure: single Switchover
	pathCurrentSwitch = "switch"

	// structure: single Switchover
	pathLastSwitch = "last_switch"

	// structure: single Switchover
	pathLastRejectedSwitch = "last_rejected_switch"

	// structure: single Maintenance
	pathMaintenance = "maintenance"

	// structure: pathRecovery/hostname -> nil
	pathRecovery = "recovery"

	// List of HA nodes. May be modified by external tools (e.g. remove node from HA-cluster)
	// list of strings
	pathHANodes = "ha_nodes"

	// List of Cascade nodes. May be modified by external tools (e.g. on node removal)
	// structure: pathCascadeNodesPrefix/hostname -> CascadeNodeConfiguration
	pathCascadeNodesPrefix = "cascade_nodes"

	// low space flag
	// structure: single value boolean
	pathLowSpace = "low_space"

	// resetup status
	// structure: pathResetupStatus/hostname -> ResetupStatus
	pathResetupStatus = "resetup_status"

	pathLastShutdownNodeTime = "last_shutdown_node_time"

	// last known timestamp from repl_mon table
	pathMasterReplMonTS = "master_repl_mon_ts"

	// timing start timestamps, structure: pathTimings/<name> -> time.Time
	pathTimings = "timing"
)
