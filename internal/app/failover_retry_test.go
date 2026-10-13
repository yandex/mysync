package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

func TestFailoverRetryReselectsOnlyBeforeTurningReplicas(t *testing.T) {
	const uuid = "5fb8588f-36ae-11ee-a7a9-7e5c538bbe1a"
	stopTurning := errors.New("temporary set-online error")
	for _, tc := range []struct {
		name          string
		phase         switchoverPhase
		manual        bool
		switchover    bool
		to            string
		survivingGTID string
		deadE         bool
		wantCandidate string
		wantPhase     switchoverPhase
		wantError     string
		wantFreeze    int
	}{
		{
			name: "automatic failover replaces dead B with C", phase: switchoverCatchUp,
			wantCandidate: "C", wantPhase: switchoverTurnReplicas, wantFreeze: 1, wantError: stopTurning.Error(),
		},
		{
			name: "reselection retains additional transactions", phase: switchoverCatchUp, survivingGTID: uuid + ":1-3",
			wantCandidate: "C", wantPhase: switchoverTurnReplicas, wantFreeze: 1, wantError: stopTurning.Error(),
		},
		{
			name: "reselection cannot lose the original target", phase: switchoverCatchUp, survivingGTID: uuid + ":1",
			wantCandidate: "B", wantPhase: switchoverFreeze, wantFreeze: 1, wantError: "does not contain previously frozen GTID set",
		},
		{
			name: "reselection still requires quorum", phase: switchoverCatchUp, deadE: true,
			wantCandidate: "B", wantPhase: switchoverCatchUp, wantError: "no quorum",
		},
		{
			name: "turning replicas has already started", phase: switchoverTurnReplicas,
			wantCandidate: "B", wantPhase: switchoverTurnReplicas, wantError: "new master B suddenly became not available",
		},
		{
			name: "promotion has already started", phase: switchoverPromote,
			wantCandidate: "B", wantPhase: switchoverPromote, wantError: "new master B is unavailable during promotion",
		},
		{
			name: "explicit destination remains fixed", phase: switchoverCatchUp, to: "B",
			wantCandidate: "B", wantPhase: switchoverCatchUp, wantError: "failed to get gtid executed from B",
		},
		{
			name: "manual failover replaces dead B with C", phase: switchoverCatchUp, manual: true,
			wantCandidate: "C", wantPhase: switchoverTurnReplicas, wantFreeze: 1, wantError: stopTurning.Error(),
		},
		{
			name: "manual failover with explicit destination remains fixed", phase: switchoverCatchUp, manual: true, to: "B",
			wantCandidate: "B", wantPhase: switchoverCatchUp, wantError: "failed to get gtid executed from B",
		},
		{
			name: "switchover remains fixed", phase: switchoverCatchUp, switchover: true,
			wantCandidate: "B", wantPhase: switchoverCatchUp, wantError: "failed to get gtid executed from B",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: A and the selected candidate B are down, while C/D/E retain quorum.
			executed := tc.survivingGTID
			if executed == "" {
				executed = uuid + ":1-2"
			}
			app, nodes := newFailoverRetryApp(t, executed, stopTurning)
			nodes["E"].available = !tc.deadE
			hosts := []string{"A", "B", "C", "D", "E"}
			sw := testSwitchover()
			sw.From, sw.To = "A", tc.to
			sw.Cause, sw.MasterTransition = CauseAuto, FailoverTransition
			if tc.manual {
				sw.Cause = CauseManual
			}
			if tc.switchover {
				sw.MasterTransition = SwitchoverTransition
			}
			sw.RunCount = 1
			target := gtids.ParseGtidSet(uuid + ":1-2")
			progress := &switchoverProgress{
				request: sw, phase: tc.phase, oldMaster: "A", newMaster: "B", mostRecent: "C",
				mostRecentGTIDSet: target, activeNodesWithOldMaster: hosts,
				frozenActiveNodes: []string{"B", "C", "D", "E"},
			}
			app.switchoverProgress = progress
			cs := clusterState("A", "B", "C", "D", "E")
			cs["A"].PingOk, cs["B"].PingOk, cs["E"].PingOk = false, false, !tc.deadE

			// Act: retry the saved operation against the surviving hosts.
			err := app.performSwitchover(cs, hosts, &sw, "A")

			// Assert: only a failover without a destination before turn-replicas repeats freeze and selection.
			require.ErrorContains(t, err, tc.wantError)
			require.Same(t, progress, app.switchoverProgress)
			require.Equal(t, tc.wantCandidate, progress.newMaster)
			require.Equal(t, tc.wantPhase, progress.phase)
			require.True(t, progress.mostRecentGTIDSet.Contain(target))
			for _, host := range []string{"C", "D", "E"} {
				require.Equal(t, tc.wantFreeze, nodes[host].freezes, host)
				require.Equal(t, tc.wantFreeze, nodes[host].stops, host)
			}
			if tc.wantCandidate == "C" {
				require.Equal(t, executed, progress.mostRecentGTIDSet.String())

				// Act: retry the set-online failure after entering turn-replicas.
				err = app.performSwitchover(cs, hosts, &sw, "A")

				// Assert: the new candidate stays fixed and freeze is not repeated again.
				require.ErrorIs(t, err, stopTurning)
				require.Equal(t, "C", progress.newMaster)
				require.Equal(t, switchoverTurnReplicas, progress.phase)
				for _, host := range []string{"C", "D", "E"} {
					require.Equal(t, 1, nodes[host].freezes, host)
				}
			}
		})
	}
}

func newFailoverRetryApp(t *testing.T, executed string, stopTurning error) (*App, map[string]*failoverRetrySQLNode) {
	t.Helper()
	cfg := minConfig()
	cfg.Hostname = "manager"
	cfg.DBTimeout, cfg.DBSetRoTimeout = time.Second, time.Second
	cfg.SemiSync = true
	cfg.RplSemiSyncMasterWaitForSlaveCount = 2
	mockDCS := NewMockIAppDCS(gomock.NewController(t))
	mockDCS.EXPECT().GetNodeConfiguration(gomock.Any()).DoAndReturn(func(host string) (mysql.NodeConfiguration, error) {
		priority := int64(0)
		if host == "C" {
			priority = 100
		}
		return mysql.NodeConfiguration{Priority: priority}, nil
	}).AnyTimes()
	app := newTestApp(t, cfg, mockDCS)
	app.dcs = &failoverRetryDCS{catchUpDCS: catchUpDCS{locked: true}}
	var err error
	app.cluster, err = mysql.NewCluster(cfg, app.logger, app.dcs)
	require.NoError(t, err)
	t.Cleanup(app.cluster.Close)
	require.NoError(t, app.cluster.UpdateHostsInfo())
	nodes := make(map[string]*failoverRetrySQLNode)
	for _, host := range []string{"A", "B", "C", "D", "E"} {
		node := &failoverRetrySQLNode{
			host: host, executed: executed, available: host != "A" && host != "B", stopTurning: stopTurning,
		}
		nodes[host] = node
		db, err := app.cluster.Get(host).GetDB()
		require.NoError(t, err)
		require.NoError(t, db.Close())
		*db = *sqlx.NewDb(sql.OpenDB(&failoverRetrySQLConnector{catchUpSQLConnector: &catchUpSQLConnector{}, node: node}), "mysql")
	}
	return app, nodes
}

type failoverRetryDCS struct{ catchUpDCS }

func (*failoverRetryDCS) GetChildren(path string) ([]string, error) {
	if path == dcs.PathHANodesPrefix {
		return []string{"A", "B", "C", "D", "E"}, nil
	}
	return nil, nil
}

type failoverRetrySQLNode struct {
	host        string
	executed    string
	available   bool
	freezes     int
	stops       int
	stopTurning error
}

type failoverRetrySQLConnector struct {
	*catchUpSQLConnector
	node *failoverRetrySQLNode
}

func (c *failoverRetrySQLConnector) Connect(context.Context) (driver.Conn, error) {
	return &failoverRetrySQLConn{catchUpSQLConn: &catchUpSQLConn{}, node: c.node}, nil
}

type failoverRetrySQLConn struct {
	*catchUpSQLConn
	node *failoverRetrySQLNode
}

func (c *failoverRetrySQLConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !c.node.available {
		return nil, fmt.Errorf("%s unavailable", c.node.host)
	}
	switch query {
	case mysql.DefaultQueries["ping"]:
		return failoverRetryRow([]string{"Ok"}, int64(1)), nil
	case mysql.DefaultQueries["get_version"]:
		return failoverRetryRow([]string{"MajorVersion", "MinorVersion", "PatchVersion"}, int64(5), int64(7), int64(44)), nil
	case mysql.DefaultQueries["is_readonly"]:
		return failoverRetryRow([]string{"ReadOnly", "SuperReadOnly"}, int64(1), int64(1)), nil
	case mysql.DefaultQueries["get_offline_mode"]:
		return failoverRetryRow([]string{"OfflineMode"}, int64(0)), nil
	case mysql.DefaultQueries["get_replication_settings"]:
		return failoverRetryRow([]string{"InnodbFlushLogAtTrxCommit", "SyncBinlog"}, int64(1), int64(1)), nil
	case mysql.DefaultQueries["semisync_plugins"]:
		return failoverRetryRow([]string{"PluginName"}), nil
	case mysql.DefaultQueries["gtid_executed"]:
		return failoverRetryRow([]string{"Executed_Gtid_Set"}, c.node.executed), nil
	case strings.Replace(mysql.DefaultQueries["slave_status"], ":channel", "''", 1):
		return failoverRetryRow(
			[]string{"Master_Host", "Executed_Gtid_Set", "Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master"},
			"A", c.node.executed, "No", "Yes", float64(0),
		), nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *failoverRetrySQLConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if !c.node.available {
		return nil, fmt.Errorf("%s unavailable", c.node.host)
	}
	switch query {
	case strings.Replace(mysql.DefaultQueries["set_lock_timeout"], "?", "1", 1):
	case mysql.DefaultQueries["set_readonly"]:
		c.node.freezes++
	case strings.Replace(mysql.DefaultQueries["stop_slave_io_thread"], ":channel", "''", 1):
		c.node.stops++
	case mysql.DefaultQueries["disable_offline_mode"]:
		// Stop on the first turn-replicas action, before promotion or semi-sync adjustment.
		return nil, c.node.stopTurning
	default:
		return nil, fmt.Errorf("unexpected statement: %s", query)
	}
	return driver.RowsAffected(0), nil
}

type failoverRetryRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func failoverRetryRow(columns []string, values ...driver.Value) *failoverRetryRows {
	return &failoverRetryRows{columns: columns, values: values}
}

func (r *failoverRetryRows) Columns() []string { return r.columns }
func (*failoverRetryRows) Close() error        { return nil }

func (r *failoverRetryRows) Next(values []driver.Value) error {
	if r.read || len(r.values) == 0 {
		return io.EOF
	}
	copy(values, r.values)
	r.read = true
	return nil
}
