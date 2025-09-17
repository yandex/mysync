package optimization

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

func replicationSettingsAreDefault(rs mysql.ReplicationSettings) bool {
	return rs.InnodbFlushLogAtTrxCommit == mysql.DefaultInnodbFlushLogAtTrxCommitValue && rs.SyncBinlog == mysql.DefaultSyncBinlogValue
}

func initDefaultCluster(t *testing.T) (*OptimizationModule, *dcs.MockDCS, map[string]*MockNode) {
	logger := log.NewMockLogger()

	f, err := os.CreateTemp(os.TempDir(), "optimization-file")
	if err != nil {
		t.Fatal(err)
	}

	om := NewOptimizationModule(logger, config.OptimizationConfig{
		Optimizationfile:                      f.Name(),
		OptimizeReplicationLagThreshold:       time.Second * 10,
		OptimizeReplicationConvergenceTimeout: time.Second * 30,
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
	err = om.Initialize(mdcs)
	if err != nil {
		t.Fatal(err)
	}

	return om, mdcs, cluster
}

func assertHostIsOptimized(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	if rs := host.replicationSettings; replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is not optimized", host.Host())
	}
	if rs := host.replicationSettings; replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is not optimized", host.Host())
	}
}

func assertHostIsNotOptimized(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	if rs := host.replicationSettings; !replicationSettingsAreDefault(rs) {
		t.Fatalf("%s is optimized", host.Host())
	}
}

func assertSyncOptions(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	mdcs.HostToLock = host.Host()
	err := om.SyncLocalOptimizationSettings(master, host, mdcs)
	if err != nil {
		t.Fatal(err)
	}
}

func assertEnableOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	mdcs.HostToLock = host.Host()
	err := om.EnableNodeOptimization(host, mdcs)
	if err != nil {
		t.Fatal(err)
	}
}

func assertDisableOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	host *MockNode,
	mdcs *dcs.MockDCS,
) {
	mdcs.HostToLock = host.Host()
	err := om.DisableNodeOptimization(master, host, mdcs)
	if err != nil {
		t.Fatal(err)
	}
}

func assertDisableAllOptimization(
	t *testing.T,
	om ReplicationOpitimizer,
	master *MockNode,
	mdcs *dcs.MockDCS,
	nodes ...*MockNode,
) {
	// We have to do it explicitly
	var ifaceNodes []NodeReplicationController
	for _, n := range nodes {
		ifaceNodes = append(ifaceNodes, n)
	}

	err := om.DisableAllNodeOptimization(master, mdcs, ifaceNodes...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOptimizationEnablingOnReplicaWithLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestOptimizationEnablingOnReplicaWithoutLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestOptimizationEnablingOnMaster(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, master, mdcs)

	assertEnableOptimization(t, om, master, mdcs)
	assertSyncOptions(t, om, master, master, mdcs)
	assertHostIsNotOptimized(t, om, master, master, mdcs)

	assertEnableOptimization(t, om, master, mdcs)
	assertSyncOptions(t, om, master, master, mdcs)
	assertHostIsNotOptimized(t, om, master, master, mdcs)
}

func TestOptimizationDisabledOnReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestOptimizationCannotBeEnabledOnMoreThanOneHost(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql3"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertEnableOptimization(t, om, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql2"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql3"], mdcs)

	assertDisableOptimization(t, om, master, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)
}

func TestOptimizationDisabledOnAllReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql3"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertEnableOptimization(t, om, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)

	assertDisableAllOptimization(t, om, master, mdcs, cluster["mysql2"], cluster["mysql3"])
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)
}

func TestHostMustUseSafeDefaultWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)

	mdcs.UnreachableCounter = 0
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestCannotEnableReplicationWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	mdcs.UnreachableCounter = 0
	err := om.EnableNodeOptimization(cluster["mysql2"], mdcs)
	if err == nil {
		t.Fatal("an error is expected")
	}
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestCanDisableReplicationWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)

	mdcs.UnreachableCounter = 0
	err := om.DisableNodeOptimization(master, cluster["mysql2"], mdcs)
	if err != nil {
		t.Fatal(err)
	}
	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
}

func TestCanDisableAllReplicationWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}
	cluster["mysql3"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertEnableOptimization(t, om, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)

	mdcs.UnreachableCounter = 0
	err := om.DisableAllNodeOptimization(master, mdcs, cluster["mysql2"], cluster["mysql3"])
	if err != nil {
		t.Fatal(err)
	}

	assertHostIsNotOptimized(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, om, master, cluster["mysql3"], mdcs)
}
