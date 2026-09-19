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
	version             Version
	perfSchemaEnabled   bool
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
	case DefaultQueries[queryGetVersion]:
		v := c.connector.version
		return &semiSyncClientsTestRows{
			columns: []string{"MajorVersion", "MinorVersion", "PatchVersion"},
			values:  [][]driver.Value{{v.MajorVersion, v.MinorVersion, v.PatchVersion}},
		}, nil
	case DefaultQueries[queryGetPerfSchema]:
		return &semiSyncClientsTestRows{
			columns: []string{"PerformanceSchema"},
			values:  [][]driver.Value{{c.connector.perfSchemaEnabled}},
		}, nil
	case DefaultQueries[queryHasWaitingSemiSyncAck], DefaultQueries[queryHasWaitingAckPerfSchema], customWaitingAckQuery:
		return &semiSyncClientsTestRows{
			columns: []string{"IsWaiting"},
			values:  [][]driver.Value{{true}},
		}, nil
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

func TestSemiSyncStatusScansMasterSlaveImplementation(t *testing.T) {
	connector := new(semiSyncClientsTestConnector)
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

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
				&mysqldriver.MySQLError{Number: unknownSystemVariable, Message: "Unknown system variable"},
			},
		},
		pluginResponses: [][]string{{semiSyncPluginSource, semiSyncPluginReplica}},
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

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

func TestSemiSyncStatusRedetectsMasterSlaveImplementation(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		queryErrors: map[string][]error{
			DefaultQueries[querySemiSyncSourceReplicaStatus]: {
				&mysqldriver.MySQLError{Number: unknownSystemVariable, Message: "Unknown system variable"},
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
	require.Equal(t, semiSyncDialectMasterSlave, *node.semiSyncDialectCache)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncSourceReplicaStatus],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncStatus],
	}, connector.queries)
}

func TestTrySemiSyncHonorsAttempts(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		queryErrors: map[string][]error{
			DefaultQueries[querySemiSyncStatus]: {
				&mysqldriver.MySQLError{Number: unknownSystemVariable, Message: "Unknown system variable"},
				&mysqldriver.MySQLError{Number: unknownSystemVariable, Message: "Unknown system variable"},
			},
		},
		pluginResponses: [][]string{
			{semiSyncPluginMaster, semiSyncPluginSlave},
			{semiSyncPluginMaster, semiSyncPluginSlave},
		},
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

	semiSync, err := node.trySemiSync(func(semiSync SemiSync) error {
		return node.queryRow(semiSync.GetStatusQuery(), nil, semiSync)
	}, 3)
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncMasterSlaveStatusStruct), semiSync)
	require.Equal(t, []string{
		DefaultQueries[querySemiSyncStatus],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncStatus],
		DefaultQueries[querySemiSyncPlugins],
		DefaultQueries[querySemiSyncStatus],
	}, connector.queries)
}

func TestTrySemiSyncDoesNotRetryUnrelatedError(t *testing.T) {
	queryErr := &mysqldriver.MySQLError{Number: 1146, Message: "Table does not exist"}
	connector := &semiSyncClientsTestConnector{
		queryErrors: map[string][]error{
			DefaultQueries[querySemiSyncStatus]: {queryErr},
		},
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

	semiSync, err := node.trySemiSync(func(semiSync SemiSync) error {
		return node.queryRow(semiSync.GetStatusQuery(), nil, semiSync)
	}, 3)
	require.ErrorIs(t, err, queryErr)
	require.IsType(t, new(SemiSyncMasterSlaveStatusStruct), semiSync)
	require.Equal(t, []string{DefaultQueries[querySemiSyncStatus]}, connector.queries)
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
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

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

func TestSemiSyncClientsRedetectsMasterSlaveAfterNoRows(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		pluginResponses: [][]string{{semiSyncPluginMaster, semiSyncPluginSlave}},
		legacyClients:   "3",
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectSourceReplica)

	clients, err := node.SemiSyncClients()
	require.NoError(t, err)
	require.Equal(t, 3, clients)
	require.NotNil(t, node.semiSyncDialectCache)
	require.Equal(t, semiSyncDialectMasterSlave, *node.semiSyncDialectCache)
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
		masterSlaveQuery   string
		sourceReplicaQuery string
	}

	bindQuery := func(queryName string) string {
		return strings.ReplaceAll(DefaultQueries[queryName], ":wait_slave_count", "?")
	}
	operations := []operation{
		{
			name:               "set master",
			call:               (*Node).SemiSyncSetMaster,
			masterSlaveQuery:   bindQuery(querySemiSyncSetMaster),
			sourceReplicaQuery: bindQuery(querySemiSyncSetSource),
		},
		{
			name:               "set slave",
			call:               (*Node).SemiSyncSetSlave,
			masterSlaveQuery:   bindQuery(querySemiSyncSetSlave),
			sourceReplicaQuery: bindQuery(querySemiSyncSetReplica),
		},
		{
			name:               "disable",
			call:               (*Node).SemiSyncDisable,
			masterSlaveQuery:   bindQuery(querySemiSyncMasterSlaveDisable),
			sourceReplicaQuery: bindQuery(querySemiSyncSourceReplicaDisable),
		},
		{
			name: "set wait count",
			call: func(node *Node) error {
				return node.SetSemiSyncWaitSlaveCount(2)
			},
			masterSlaveQuery:   bindQuery(querySetSemiSyncWaitSlaveCount),
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
			name:          "master-slave to source-replica",
			cached:        semiSyncDialectMasterSlave,
			plugins:       []string{semiSyncPluginSource, semiSyncPluginReplica},
			wrongQuery:    func(op operation) string { return op.masterSlaveQuery },
			expectedQuery: func(op operation) string { return op.sourceReplicaQuery },
			expected:      semiSyncDialectSourceReplica,
		},
		{
			name:          "source-replica to master-slave",
			cached:        semiSyncDialectSourceReplica,
			plugins:       []string{semiSyncPluginMaster, semiSyncPluginSlave},
			wrongQuery:    func(op operation) string { return op.sourceReplicaQuery },
			expectedQuery: func(op operation) string { return op.masterSlaveQuery },
			expected:      semiSyncDialectMasterSlave,
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
								&mysqldriver.MySQLError{Number: unknownSystemVariable, Message: "Unknown system variable"},
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

const customWaitingAckQuery = "SELECT 1 AS IsWaiting"

func TestIsWaitingSemiSyncAckChoosesProcesslistTable(t *testing.T) {
	versionQuery := DefaultQueries[queryGetVersion]
	perfSchemaQuery := DefaultQueries[queryGetPerfSchema]
	infoSchemaAckQuery := DefaultQueries[queryHasWaitingSemiSyncAck]
	perfSchemaAckQuery := DefaultQueries[queryHasWaitingAckPerfSchema]

	testCases := []struct {
		name              string
		version           Version
		perfSchemaEnabled bool
		expectedQueries   []string
	}{
		{
			name:              "5.7",
			version:           Version{MajorVersion: 5, MinorVersion: 7, PatchVersion: 44},
			perfSchemaEnabled: true,
			expectedQueries:   []string{versionQuery, infoSchemaAckQuery},
		},
		{
			name:              "8.0.21",
			version:           Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 21},
			perfSchemaEnabled: true,
			expectedQueries:   []string{versionQuery, infoSchemaAckQuery},
		},
		{
			name:              "8.0.22",
			version:           Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 22},
			perfSchemaEnabled: true,
			expectedQueries:   []string{versionQuery, perfSchemaQuery, perfSchemaAckQuery},
		},
		{
			name:              "8.0.22 with performance_schema disabled",
			version:           Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 22},
			perfSchemaEnabled: false,
			expectedQueries:   []string{versionQuery, perfSchemaQuery, infoSchemaAckQuery},
		},
		{
			name:              "8.4",
			version:           Version{MajorVersion: 8, MinorVersion: 4, PatchVersion: 0},
			perfSchemaEnabled: true,
			expectedQueries:   []string{versionQuery, perfSchemaQuery, perfSchemaAckQuery},
		},
		{
			name:              "9.7",
			version:           Version{MajorVersion: 9, MinorVersion: 7, PatchVersion: 0},
			perfSchemaEnabled: true,
			expectedQueries:   []string{versionQuery, perfSchemaQuery, perfSchemaAckQuery},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			connector := &semiSyncClientsTestConnector{
				version:           testCase.version,
				perfSchemaEnabled: testCase.perfSchemaEnabled,
			}
			node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

			waiting, err := node.IsWaitingSemiSyncAck()
			require.NoError(t, err)
			require.True(t, waiting)
			require.Equal(t, testCase.expectedQueries, connector.queries)

			// cached: only the processlist query runs
			connector.queries = nil
			_, err = node.IsWaitingSemiSyncAck()
			require.NoError(t, err)
			require.Equal(t, testCase.expectedQueries[len(testCase.expectedQueries)-1:], connector.queries)
		})
	}
}

func TestIsWaitingSemiSyncAckFallsBackOnPerfSchemaCheckError(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		queryErrors: map[string][]error{
			DefaultQueries[queryGetPerfSchema]: {errors.New("connection reset")},
		},
		version:           Version{MajorVersion: 8, MinorVersion: 4, PatchVersion: 0},
		perfSchemaEnabled: true,
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)

	_, err := node.IsWaitingSemiSyncAck()
	require.NoError(t, err)
	require.Equal(t, []string{
		DefaultQueries[queryGetVersion],
		DefaultQueries[queryGetPerfSchema],
		DefaultQueries[queryHasWaitingSemiSyncAck],
	}, connector.queries)

	// failed check is not cached
	connector.queries = nil
	_, err = node.IsWaitingSemiSyncAck()
	require.NoError(t, err)
	require.Equal(t, []string{
		DefaultQueries[queryGetPerfSchema],
		DefaultQueries[queryHasWaitingAckPerfSchema],
	}, connector.queries)
}

func TestIsWaitingSemiSyncAckKeepsQueryOverride(t *testing.T) {
	connector := &semiSyncClientsTestConnector{
		version:           Version{MajorVersion: 8, MinorVersion: 4, PatchVersion: 0},
		perfSchemaEnabled: true,
	}
	node := newSemiSyncTestNode(t, connector, semiSyncDialectMasterSlave)
	node.config.Queries[queryHasWaitingSemiSyncAck] = customWaitingAckQuery

	_, err := node.IsWaitingSemiSyncAck()
	require.NoError(t, err)
	require.Equal(t, []string{customWaitingAckQuery}, connector.queries)
}
