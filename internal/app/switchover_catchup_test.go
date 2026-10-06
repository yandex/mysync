package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
)

func TestSwitchoverCatchUpWithUnavailableDonor(t *testing.T) {
	const uuid = "5fb8588f-36ae-11ee-a7a9-7e5c538bbe1a"
	donorUnavailable := errors.New("donor unavailable")
	candidateUnavailable := errors.New("candidate unavailable")
	for _, tc := range []struct {
		name          string
		executed      string
		queryErr      error
		locked        bool
		wantReset     bool
		wantPhase     switchoverPhase
		wantError     string
		wantCause     error
		wantDonorExec int
		wantLocks     int
	}{
		{
			name: "target applied", executed: uuid + ":1-2", locked: true,
			wantPhase: switchoverTurnReplicas, wantLocks: 1,
			wantError: "new master new suddenly became not available",
		},
		{
			name: "target contained", executed: uuid + ":1-3", locked: true,
			wantPhase: switchoverTurnReplicas, wantLocks: 1,
			wantError: "new master new suddenly became not available",
		},
		{
			name: "manager lock lost", executed: uuid + ":1-2",
			wantReset: true, wantLocks: 1, wantError: "manger lock lost",
		},
		{
			name: "target not applied", executed: uuid + ":1", locked: true,
			wantPhase: switchoverCatchUp, wantDonorExec: 1, wantCause: donorUnavailable,
		},
		{
			name: "candidate GTID unavailable", queryErr: candidateUnavailable, locked: true,
			wantPhase: switchoverCatchUp, wantCause: candidateUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: resume catch-up with the donor unavailable and a surviving quorum.
			cfg := minConfig()
			cfg.Hostname = "manager"
			cfg.DBTimeout = time.Second
			cfg.SemiSync = true
			cfg.RplSemiSyncMasterWaitForSlaveCount = 1
			mockDCS := NewMockIAppDCS(gomock.NewController(t))
			app := newTestApp(t, cfg, mockDCS)
			app.state = stateManager
			fakeDCS := &catchUpDCS{locked: tc.locked}
			app.dcs = fakeDCS
			var err error
			app.cluster, err = mysql.NewCluster(cfg, app.logger, fakeDCS)
			require.NoError(t, err)
			t.Cleanup(app.cluster.Close)
			require.NoError(t, app.cluster.UpdateHostsInfo())

			donor := &catchUpSQLConnector{execErr: donorUnavailable}
			for host, connector := range map[string]*catchUpSQLConnector{
				"old": {}, "donor": donor,
				"new": {executed: tc.executed, queryErr: tc.queryErr},
			} {
				// Install an in-memory SQL driver without opening network connections.
				db, err := app.cluster.Get(host).GetDB()
				require.NoError(t, err)
				require.NoError(t, db.Close())
				*db = *sqlx.NewDb(sql.OpenDB(connector), "mysql")
			}

			sw := testSwitchover()
			sw.RunCount = 1
			hosts := []string{"old", "new", "donor"}
			app.switchoverProgress = &switchoverProgress{
				request: sw, phase: switchoverCatchUp,
				oldMaster: "old", newMaster: "new", mostRecent: "donor",
				mostRecentGTIDSet:        gtids.ParseGtidSet(uuid + ":1-2"),
				activeNodesWithOldMaster: hosts, frozenActiveNodes: hosts,
			}
			cs := clusterState("old", "new", "donor")
			cs["donor"].PingOk = false // The remaining two hosts still form a quorum.

			// Act.
			err = app.performSwitchover(cs, hosts, &sw, "old")

			// Assert.
			if tc.wantCause != nil {
				require.ErrorIs(t, err, tc.wantCause)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
			require.Equal(t, tc.wantDonorExec, donor.execCalls)
			require.Equal(t, tc.wantLocks, fakeDCS.lockCalls)
			if tc.wantReset {
				require.Nil(t, app.switchoverProgress)
			} else {
				require.Equal(t, tc.wantPhase, app.switchoverProgress.phase)
			}

			if tc.wantReset {
				// Arrange: another manager changed the master and released the lock.
				// Ownership returns before the next tick, without a state transition.
				fakeDCS.locked = true
				currentState := clusterState("current-master")
				mockDCS.EXPECT().GetMasterHostFromDcs().Return("current-master", nil)

				// Act.
				locked := app.AcquireLock(pathManagerLock)
				app.setState(stateManager)
				master, err := app.getMasterForSwitchover(currentState, &sw)

				// Assert: the master is read from DCS instead of the discarded progress.
				require.True(t, locked)
				require.Nil(t, app.switchoverProgress)
				require.NoError(t, err)
				require.Equal(t, "current-master", master)
			}
		})
	}
}

type catchUpDCS struct {
	dcs.DCS
	locked    bool
	lockCalls int
}

func (*catchUpDCS) GetChildren(path string) ([]string, error) {
	if path == dcs.PathHANodesPrefix {
		return []string{"old", "new", "donor"}, nil
	}
	return nil, nil
}

func (d *catchUpDCS) AcquireLock(string) bool {
	d.lockCalls++
	return d.locked
}

type catchUpSQLConnector struct {
	executed  string
	queryErr  error
	execErr   error
	execCalls int
}

func (c *catchUpSQLConnector) Connect(context.Context) (driver.Conn, error) {
	return &catchUpSQLConn{connector: c}, nil
}

func (c *catchUpSQLConnector) Driver() driver.Driver { return c }

func (*catchUpSQLConnector) Open(string) (driver.Conn, error) {
	return nil, errors.New("use Connect")
}

type catchUpSQLConn struct{ connector *catchUpSQLConnector }

func (*catchUpSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}

func (*catchUpSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}

func (*catchUpSQLConn) Close() error { return nil }

func (c *catchUpSQLConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != mysql.DefaultQueries["gtid_executed"] {
		// Stop at phase 5's health check, so this test only exercises catch-up.
		return nil, errors.New("health check unavailable")
	}
	if c.connector.queryErr != nil {
		return nil, c.connector.queryErr
	}
	return &catchUpGTIDRows{executed: c.connector.executed}, nil
}

func (c *catchUpSQLConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.connector.execCalls++
	return nil, c.connector.execErr
}

type catchUpGTIDRows struct {
	executed string
	read     bool
}

func (*catchUpGTIDRows) Columns() []string { return []string{"Executed_Gtid_Set"} }
func (*catchUpGTIDRows) Close() error      { return nil }

func (r *catchUpGTIDRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	values[0] = r.executed
	r.read = true
	return nil
}
