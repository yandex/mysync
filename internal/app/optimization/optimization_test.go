package optimization

import (
	"context"
	"database/sql"
	"testing"
	"time"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

func replicationSettingsAreDefault(rs mysql.ReplicationSettings) bool {
	return rs.InnodbFlushLogAtTrxCommit == mysql.DefaultInnodbFlushLogAtTrxCommitValue && rs.SyncBinlog == mysql.DefaultSyncBinlogValue
}

func initDefaultCluster(t *testing.T) (*Optimizer, *dcs.MockDCS, map[string]*MockNode) {
	logger := log.NewMockLogger()

	om := NewOptimizer(logger, config.OptimizationConfig{
		HighReplicationMark: time.Second * 10,
		LowReplicationMark:  time.Second * 5,
	})

	cluster := map[string]*MockNode{
		"mysql1": {
			hostname: "mysql1",
			replicationSettings: mysql.ReplicationSettings{
				SyncBinlog:                mysql.DefaultSyncBinlogValue,
				InnodbFlushLogAtTrxCommit: mysql.DefaultInnodbFlushLogAtTrxCommitValue,
			},
			replicationStatus: mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 0.0},
			},
		},
		"mysql2": {
			hostname: "mysql2",
			replicationSettings: mysql.ReplicationSettings{
				SyncBinlog:                mysql.DefaultSyncBinlogValue,
				InnodbFlushLogAtTrxCommit: mysql.DefaultInnodbFlushLogAtTrxCommitValue,
			},
			replicationStatus: mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 0.0},
			},
		},
		"mysql3": {
			hostname: "mysql3",
			replicationSettings: mysql.ReplicationSettings{
				SyncBinlog:                mysql.DefaultSyncBinlogValue,
				InnodbFlushLogAtTrxCommit: mysql.DefaultInnodbFlushLogAtTrxCommitValue,
			},
			replicationStatus: mysql.ReplicaStatusStruct{
				Lag: sql.NullFloat64{Valid: true, Float64: 0.0},
			},
		},
	}
	mdcs := dcs.NewMockDCS()
	err := om.Initialize(mdcs)
	if err != nil {
		t.Fatal(err)
	}

	return om, mdcs, cluster
}

func assertHostIsOptimized(
	t *testing.T,
	host *MockNode,
) {
	t.Helper()
	if rs := host.replicationSettings; replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is not optimized", host.Host())
	}
	if rs := host.replicationSettings; replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is not optimized", host.Host())
	}
}

func assertHostIsNotOptimized(
	t *testing.T,
	host *MockNode,
) {
	t.Helper()
	if rs := host.replicationSettings; !replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is optimized", host.Host())
	}
}

func assertSyncOptions(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	cluster map[string]*MockNode,
) {
	t.Helper()
	err := om.SyncState(master, convertClusterToNodeState(cluster), convertNodesToReplicationControllers(getNodes(cluster)))
	if err != nil {
		t.Fatal(err)
	}
}

func getNodes(cluster map[string]*MockNode) []*MockNode {
	var nodes []*MockNode
	for _, node := range cluster {
		nodes = append(nodes, node)
	}
	return nodes
}

func convertNodesToReplicationControllers(nodes []*MockNode) []NodeReplicationController {
	var ifaceNodes []NodeReplicationController
	for _, n := range nodes {
		ifaceNodes = append(ifaceNodes, n)
	}
	return ifaceNodes
}

func convertClusterToNodeState(nodes map[string]*MockNode) map[string]*nodestate.NodeState {
	states := map[string]*nodestate.NodeState{}
	for hostname, node := range nodes {
		n := states[hostname]

		if n == nil {
			n = new(nodestate.NodeState)
			n.ReplicationSettings = new(mysql.ReplicationSettings)
			n.SlaveState = new(nodestate.SlaveState)
		}

		if node != nil {
			n.SlaveState.ReplicationLag = &node.replicationStatus.Lag.Float64
			n.ReplicationSettings = &node.replicationSettings
		}
		states[hostname] = n
	}
	return states
}

func assertEnableOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	host *MockNode,
) {
	t.Helper()
	err := om.EnableNodeOptimization(host)
	if err != nil {
		t.Fatal(err)
	}
}

func assertHostPathExistsInDCS(
	t *testing.T,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	t.Helper()
	err := mdcs.Get(dcs.JoinPath(pathOptimizationNodes, host.Host()), &State{})
	if err != nil {
		t.Fatal(err)
	}
}

func assertHostPathDoesntExistInDCS(
	t *testing.T,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	t.Helper()
	err := mdcs.Get(dcs.JoinPath(pathOptimizationNodes, host.Host()), &State{})
	if err == dcs.ErrNotFound {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if err == nil {
		t.Fatal("an error is expected")
	}
}

func assertDisableOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	host *MockNode,
) {
	t.Helper()
	err := om.DisableNodeOptimization(master, host)
	if err != nil {
		t.Fatal(err)
	}
}

func assertDisableAllOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	nodes ...*MockNode,
) {
	t.Helper()
	// We have to do it explicitly
	var ifaceNodes []NodeReplicationController
	for _, n := range nodes {
		ifaceNodes = append(ifaceNodes, n)
	}

	err := om.DisableAllNodeOptimization(master, ifaceNodes)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOptimizationEnablingOnReplicaWithLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)
	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostPathExistsInDCS(t, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)
	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostPathExistsInDCS(t, cluster["mysql2"], mdcs)
}

func TestOptimizationEnablingOnReplicaWithoutLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)
}

func TestOptimizationEnablingOnMaster(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, master)
	assertHostPathDoesntExistInDCS(t, master, mdcs)

	assertEnableOptimization(t, om, master)
	assertHostPathExistsInDCS(t, master, mdcs)
	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, master)
	assertHostPathDoesntExistInDCS(t, master, mdcs)

	assertEnableOptimization(t, om, master)
	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, master)
	assertHostPathDoesntExistInDCS(t, master, mdcs)
}

func TestOptimizationDisabledOnReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)
	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostPathExistsInDCS(t, cluster["mysql2"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)
}

func TestOptimizationCannotBeEnabledOnMoreThanOneHost(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql3"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)

	assertEnableOptimization(t, om, cluster["mysql3"])
	assertSyncOptions(t, om, master, cluster)

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostPathExistsInDCS(t, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql3"])
	assertHostPathExistsInDCS(t, cluster["mysql3"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql2"])

	assertSyncOptions(t, om, master, cluster)
	assertSyncOptions(t, om, master, cluster)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql3"])
	assertHostPathExistsInDCS(t, cluster["mysql3"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql3"])

	assertSyncOptions(t, om, master, cluster)
	assertSyncOptions(t, om, master, cluster)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql3"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql3"], mdcs)
}

func TestOptimizationDisabledOnAllReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster)
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql3"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"])
	assertSyncOptions(t, om, master, cluster)

	assertEnableOptimization(t, om, cluster["mysql3"])
	assertSyncOptions(t, om, master, cluster)

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertHostPathExistsInDCS(t, cluster["mysql2"], mdcs)
	assertHostPathExistsInDCS(t, cluster["mysql3"], mdcs)

	assertDisableAllOptimization(t, om, master, master, master, cluster["mysql2"], cluster["mysql3"])

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertSyncOptions(t, om, master, cluster)
	assertSyncOptions(t, om, master, cluster)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertHostPathDoesntExistInDCS(t, cluster["mysql2"], mdcs)
	assertHostPathDoesntExistInDCS(t, cluster["mysql3"], mdcs)
}

func TestWaitingWorksOnOptimizedHost(t *testing.T) {
	om, _, cluster := initDefaultCluster(t)

	err := om.WaitOptimization(context.Background(), cluster["mysql2"], time.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitingDeadOnDeadline(t *testing.T) {
	om, _, cluster := initDefaultCluster(t)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql2"].replicationSettings = mysql.ReplicationSettings{
		InnodbFlushLogAtTrxCommit: 2,
		SyncBinlog:                1000,
	}
	assertEnableOptimization(t, om, cluster["mysql2"])

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Microsecond)
	defer cancel()

	err := om.WaitOptimization(ctx, cluster["mysql2"], time.Microsecond)
	if err == nil {
		t.Fatal("an error is expected")
	}
}
