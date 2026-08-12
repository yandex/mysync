package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/yandex/mysync/internal/config"
)

type semiSyncClientsTestConnector struct {
	mu                     sync.Mutex
	queries                []string
	legacyStatusQueryError error
}

func (c *semiSyncClientsTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &semiSyncClientsTestConn{connector: c}, nil
}

func (c *semiSyncClientsTestConnector) Driver() driver.Driver {
	return c
}

func (c *semiSyncClientsTestConnector) Open(string) (driver.Conn, error) {
	return &semiSyncClientsTestConn{connector: c}, nil
}

func (c *semiSyncClientsTestConnector) recordQuery(query string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
}

type semiSyncClientsTestConn struct {
	connector *semiSyncClientsTestConnector
}

func (c *semiSyncClientsTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *semiSyncClientsTestConn) Close() error {
	return nil
}

func (c *semiSyncClientsTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c *semiSyncClientsTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.connector.recordQuery(query)

	switch query {
	case DefaultQueries[querySemiSyncStatus]:
		if c.connector.legacyStatusQueryError != nil {
			return nil, c.connector.legacyStatusQueryError
		}
		return &semiSyncClientsTestRows{
			columns: []string{"MasterEnabled", "SlaveEnabled", "WaitSlaveCount"},
			values:  [][]driver.Value{{1, 0, 1}},
		}, nil
	case DefaultQueries[querySemiSyncSourceReplicaStatus]:
		return &semiSyncClientsTestRows{
			columns: []string{"SourceEnabled", "ReplicaEnabled", "WaitReplicaCount"},
			values:  [][]driver.Value{{1, 0, 2}},
		}, nil
	case DefaultQueries[querySemiSyncMasterClients]:
		return &semiSyncClientsTestRows{columns: []string{"Clients"}}, nil
	case DefaultQueries[querySemiSyncPlugins]:
		return &semiSyncClientsTestRows{
			columns: []string{"PluginName"},
			values: [][]driver.Value{
				{semiSyncPluginSource},
				{semiSyncPluginReplica},
			},
		}, nil
	case DefaultQueries[querySemiSyncSourceClients]:
		return &semiSyncClientsTestRows{
			columns: []string{"Clients"},
			values:  [][]driver.Value{{"2"}},
		}, nil
	default:
		return nil, errors.New("unexpected query")
	}
}

func newSemiSyncTestNode(t *testing.T, connector *semiSyncClientsTestConnector, dialect semiSyncDialect) *Node {
	t.Helper()

	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	logger := zerolog.Nop()
	node := &Node{
		config: &config.Config{
			DBTimeout: time.Second,
			Queries:   map[string]string{},
		},
		logger:               &logger,
		db:                   sqlx.NewDb(db, "semisync-test"),
		semiSyncDialectCache: &dialect,
	}
	node.done.Store(1)
	return node
}

func TestSemiSyncStatusScansSourceSlaveImplementation(t *testing.T) {
	connector := new(semiSyncClientsTestConnector)
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceSlave)

	status, err := node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncMasterSlaveStatusStruct), status)
	require.True(t, status.MasterEnabled())
	require.False(t, status.SlaveEnabled())
	require.Equal(t, 1, status.GetWaitSlaveCount())
	require.Equal(t, []string{DefaultQueries[querySemiSyncStatus]}, connector.queries)
}

func TestSemiSyncStatusRedetectsSourceReplicaImplementation(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		legacyStatusQueryError: &mysqldriver.MySQLError{Number: 1193, Message: "Unknown system variable"},
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceSlave)

	status, err := node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncSourceReplicaStatusStruct), status)
	require.True(t, status.MasterEnabled())
	require.False(t, status.SlaveEnabled())
	require.Equal(t, 2, status.GetWaitSlaveCount())
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectSourceReplica, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncStatus],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncSourceReplicaStatus],
	}, connector.queries)
}

type semiSyncClientsTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *semiSyncClientsTestRows) Columns() []string {
	return r.columns
}

func (r *semiSyncClientsTestRows) Close() error {
	return nil
}

func (r *semiSyncClientsTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestSemiSyncClientsRedetectsDialectAfterNoRows(t *testing.T) {
	connector := new(semiSyncClientsTestConnector)
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceSlave)

	clients, err := node.SemiSyncClients()
	require.NoError(t, err)
	require.Equal(t, 2, clients)
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectSourceReplica, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncMasterClients],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncSourceClients],
	}, connector.queries)
}
