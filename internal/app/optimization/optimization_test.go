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

func initDefaultCluster(t *testing.T) (*Optimizer, *dcs.MockDCS, map[string]*MockNode) {
	logger := log.NewMockLogger()

	f, err := os.CreateTemp(os.TempDir(), "optimization-file")
	if err != nil {
		t.Fatal(err)
	}

	om := NewOptimizer(logger, config.OptimizationConfig{
		File:                    f.Name(),
		ReplicationLagThreshold: time.Second * 10,
		SyncInterval:            time.Second * 0,
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
	host *MockNode,
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
	host *MockNode,
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
	err := om.DisableNodeOptimization(master, host, mdcs, false)
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

	err := om.DisableAllNodeOptimization(master, mdcs, false, ifaceNodes...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOptimizationEnablingOnReplicaWithLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql2"])

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql2"])
}

func TestOptimizationEnablingOnReplicaWithoutLag(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])
}

func TestOptimizationEnablingOnMaster(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, master)

	assertEnableOptimization(t, om, master, mdcs)
	assertSyncOptions(t, om, master, master, mdcs)
	assertHostIsNotOptimized(t, master)

	assertEnableOptimization(t, om, master, mdcs)
	assertSyncOptions(t, om, master, master, mdcs)
	assertHostIsNotOptimized(t, master)
}

func TestOptimizationDisabledOnReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql2"])

	assertDisableOptimization(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])
}

func TestOptimizationCannotBeEnabledOnMoreThanOneHost(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

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

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertDisableOptimization(t, om, master, cluster["mysql2"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsOptimized(t, cluster["mysql3"])

	assertDisableOptimization(t, om, master, cluster["mysql3"], mdcs)

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])
}

func TestOptimizationDisabledOnAllReplicaImmediately(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])

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

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertDisableAllOptimization(t, om, master, mdcs, cluster["mysql2"], cluster["mysql3"])
	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql3"], mdcs)

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])
}

func TestHostMustUseSafeDefaultWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql2"])

	mdcs.UnreachableCounter = 0
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsNotOptimized(t, cluster["mysql2"])
}

func TestCannotEnableReplicationWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	mdcs.UnreachableCounter = 0
	err := om.EnableNodeOptimization(cluster["mysql2"], mdcs)
	if err == nil {
		t.Fatal("an error is expected")
	}
	assertHostIsNotOptimized(t, cluster["mysql2"])
}

func TestCanDisableReplicationWhenZKIsUnreachable(t *testing.T) {
	om, mdcs, cluster := initDefaultCluster(t)
	master := cluster["mysql1"]

	cluster["mysql2"].replicationStatus = mysql.ReplicaStatusStruct{
		Lag: sql.NullFloat64{Valid: true, Float64: 20.0},
	}

	assertEnableOptimization(t, om, cluster["mysql2"], mdcs)
	assertSyncOptions(t, om, master, cluster["mysql2"], mdcs)
	assertHostIsOptimized(t, cluster["mysql2"])

	mdcs.UnreachableCounter = 0
	err := om.DisableNodeOptimization(master, cluster["mysql2"], mdcs, false)
	if err == nil {
		t.Fatal("an error is expected")
	}
	assertHostIsOptimized(t, cluster["mysql2"])

	err = om.DisableNodeOptimization(master, cluster["mysql2"], mdcs, true)
	if err != nil {
		t.Fatal(err)
	}
	assertHostIsNotOptimized(t, cluster["mysql2"])
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

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	mdcs.UnreachableCounter = 0
	err := om.DisableAllNodeOptimization(master, mdcs, false, cluster["mysql2"], cluster["mysql3"])
	if err == nil {
		t.Fatal("an error is expected")
	}

	assertHostIsOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])

	err = om.DisableAllNodeOptimization(master, mdcs, true, cluster["mysql2"], cluster["mysql3"])
	if err != nil {
		t.Fatal(err)
	}

	assertHostIsNotOptimized(t, cluster["mysql2"])
	assertHostIsNotOptimized(t, cluster["mysql3"])
}
