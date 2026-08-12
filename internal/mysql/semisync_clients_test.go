package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
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
	mu                  sync.Mutex
	queries             []string
	queryErrors         map[string][]error
	execErrors          map[string][]error
	pluginResponses     [][]string
	pluginResponseIndex int
	legacyClients       string
	sourceClients       string
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

func (c *semiSyncClientsTestConnector) popQueryError(query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return popSemiSyncTestError(c.queryErrors, query)
}

func (c *semiSyncClientsTestConnector) popExecError(query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return popSemiSyncTestError(c.execErrors, query)
}

func popSemiSyncTestError(errorsByQuery map[string][]error, query string) error {
	queryErrors := errorsByQuery[query]
	if len(queryErrors) == 0 {
		return nil
	}
	errorsByQuery[query] = queryErrors[1:]
	return queryErrors[0]
}

func (c *semiSyncClientsTestConnector) nextPluginResponse() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pluginResponseIndex >= len(c.pluginResponses) {
		return nil
	}
	plugins := c.pluginResponses[c.pluginResponseIndex]
	c.pluginResponseIndex++
	return plugins
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
	if err := c.connector.popQueryError(query); err != nil {
		return nil, err
	}

	switch query {
	case DefaultQueries[querySemiSyncStatus]:
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
		rows := &semiSyncClientsTestRows{columns: []string{"Clients"}}
		if c.connector.legacyClients != "" {
			rows.values = [][]driver.Value{{c.connector.legacyClients}}
		}
		return rows, nil
	case DefaultQueries[querySemiSyncPlugins]:
		plugins := c.connector.nextPluginResponse()
		values := make([][]driver.Value, 0, len(plugins))
		for _, plugin := range plugins {
			values = append(values, []driver.Value{plugin})
		}
		return &semiSyncClientsTestRows{
			columns: []string{"PluginName"},
			values:  values,
		}, nil
	case DefaultQueries[querySemiSyncSourceClients]:
		rows := &semiSyncClientsTestRows{columns: []string{"Clients"}}
		if c.connector.sourceClients != "" {
			rows.values = [][]driver.Value{{c.connector.sourceClients}}
		}
		return rows, nil
	default:
		return nil, errors.New("unexpected query")
	}
}

func (c *semiSyncClientsTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.connector.recordQuery(query)
	if err := c.connector.popExecError(query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
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
		db:                   sqlx.NewDb(db, "mysql"),
		semiSyncDialectCache: &dialect,
	}
	node.done.Store(1)
	return node
}

func newSemiSyncTestNodeWithoutCache(t *testing.T, connector *semiSyncClientsTestConnector) *Node {
	t.Helper()

	node := newSemiSyncTestNode(t, connector, semiSyncDialectDisabled)
	node.semiSyncDialectCache = nil
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
		queryErrors: map[string][]error{
			DefaultQueries[querySemiSyncStatus]: {
				&mysqldriver.MySQLError{Number: 1193, Message: "Unknown system variable"},
			},
		},
		pluginResponses: [][]string{{semiSyncPluginSource, semiSyncPluginReplica}},
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

func TestSemiSyncStatusRedetectsSourceSlaveImplementation(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		queryErrors: map[string][]error{
			DefaultQueries[querySemiSyncSourceReplicaStatus]: {
				&mysqldriver.MySQLError{Number: 1193, Message: "Unknown system variable"},
			},
		},
		pluginResponses: [][]string{{semiSyncPluginMaster, semiSyncPluginSlave}},
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceReplica)

	status, err := node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncMasterSlaveStatusStruct), status)
	require.True(t, status.MasterEnabled())
	require.False(t, status.SlaveEnabled())
	require.Equal(t, 1, status.GetWaitSlaveCount())
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectSourceSlave, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncSourceReplicaStatus],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncStatus],
	}, connector.queries)
}

func TestSemiSyncStatusRedetectsAfterDisabled(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		pluginResponses: [][]string{
			nil,
			{semiSyncPluginSource, semiSyncPluginReplica},
		},
	}
	node := newSemiSyncTestNodeWithoutCache(t, connector)

	status, err := node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncDisabledStatusStruct), status)
	require.Nil(t, node.semiSyncDialectCache)

	status, err = node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncSourceReplicaStatusStruct), status)
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectSourceReplica, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncPlugins],
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
	connector := &semiSyncClientsTestConnector{
		pluginResponses: [][]string{{semiSyncPluginSource, semiSyncPluginReplica}},
		sourceClients:   "2",
	}
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

func TestSemiSyncClientsRedetectsSourceSlaveAfterNoRows(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		pluginResponses: [][]string{{semiSyncPluginMaster, semiSyncPluginSlave}},
		legacyClients:   "3",
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceReplica)

	clients, err := node.SemiSyncClients()
	require.NoError(t, err)
	require.Equal(t, 3, clients)
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectSourceSlave, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncSourceClients],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncMasterClients],
	}, connector.queries)
}

func TestSemiSyncMutationsRedetectDialect(t *testing.T) {
	type operation struct {
		name               string
		call               func(*Node) error
		sourceSlaveQuery   string
		sourceReplicaQuery string
	}

	bindQuery := func(queryName string) string {
		return strings.ReplaceAll(DefaultQueries[queryName], ":wait_slave_count", "?")
	}
	operations := []operation{
		{
			name:               "set master",
			call:               (*Node).SemiSyncSetMaster,
			sourceSlaveQuery:   bindQuery(querySemiSyncSetMaster),
			sourceReplicaQuery: bindQuery(querySemiSyncSetSource),
		},
		{
			name:               "set slave",
			call:               (*Node).SemiSyncSetSlave,
			sourceSlaveQuery:   bindQuery(querySemiSyncSetSlave),
			sourceReplicaQuery: bindQuery(querySemiSyncSetReplica),
		},
		{
			name:               "disable",
			call:               (*Node).SemiSyncDisable,
			sourceSlaveQuery:   bindQuery(querySemiSyncMasterSlaveDisable),
			sourceReplicaQuery: bindQuery(querySemiSyncSourceReplicaDisable),
		},
		{
			name: "set wait count",
			call: func(node *Node) error {
				return node.SetSemiSyncWaitSlaveCount(2)
			},
			sourceSlaveQuery:   bindQuery(querySetSemiSyncWaitSlaveCount),
			sourceReplicaQuery: bindQuery(querySetSemiSyncWaitReplicaCount),
		},
	}
	directions := []struct {
		name          string
		cached        semiSyncDialect
		plugins       []string
		wrongQuery    func(operation) string
		expectedQuery func(operation) string
		expected      semiSyncDialect
	}{
		{
			name:          "source-slave to source-replica",
			cached:        semiSyncDialectSourceSlave,
			plugins:       []string{semiSyncPluginSource, semiSyncPluginReplica},
			wrongQuery:    func(op operation) string { return op.sourceSlaveQuery },
			expectedQuery: func(op operation) string { return op.sourceReplicaQuery },
			expected:      semiSyncDialectSourceReplica,
		},
		{
			name:          "source-replica to source-slave",
			cached:        semiSyncDialectSourceReplica,
			plugins:       []string{semiSyncPluginMaster, semiSyncPluginSlave},
			wrongQuery:    func(op operation) string { return op.sourceReplicaQuery },
			expectedQuery: func(op operation) string { return op.sourceSlaveQuery },
			expected:      semiSyncDialectSourceSlave,
		},
	}

	for _, direction := range directions {
		t.Run(direction.name, func(t *testing.T) {
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					wrongQuery := direction.wrongQuery(op)
					expectedQuery := direction.expectedQuery(op)
					connector := &semiSyncClientsTestConnector{
						execErrors: map[string][]error{
							wrongQuery: {
								&mysqldriver.MySQLError{Number: 1193, Message: "Unknown system variable"},
							},
						},
						pluginResponses: [][]string{direction.plugins},
					}
					node := newSemiSyncTestNode(t, connector, direction.cached)

					require.NoError(t, op.call(node))
					require.NotNil(t, node.semiSyncDialectCache)
					require.Equal(t, direction.expected, *node.semiSyncDialectCache)
					require.Equal(t, []string{
						DefaultQueries[querySetLockTimeout],
						wrongQuery,
						DefaultQueries[querySemiSyncPlugins],
						DefaultQueries[querySetLockTimeout],
						expectedQuery,
					}, connector.queries)
				})
			}
		})
	}
}
