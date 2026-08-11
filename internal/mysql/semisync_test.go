package mysql

import (
	"errors"
	"fmt"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestSemiSyncDialectValues(t *testing.T) {
	require.Equal(t, semiSyncDialect("disabled"), semiSyncDialectDisabled)
	require.Equal(t, semiSyncDialect("sourceslave"), semiSyncDialectSourceSlave)
	require.Equal(t, semiSyncDialect("sourceReplica"), semiSyncDialectSourceReplica)
}

func TestSemiSyncMasterSlaveDisableQueryName(t *testing.T) {
	require.Equal(t, "semisync_disable", querySemiSyncMasterSlaveDisable)
}

func TestDetectSemiSyncDialect(t *testing.T) {
	testCases := []struct {
		name      string
		plugins   []string
		expected  semiSyncDialect
		expectErr bool
	}{
		{name: "no plugins", expected: semiSyncDialectDisabled},
		{name: "irrelevant plugin", plugins: []string{"validate_password"}, expected: semiSyncDialectDisabled},
		{name: "master plugin", plugins: []string{semiSyncPluginMaster}, expected: semiSyncDialectSourceSlave},
		{name: "slave plugin", plugins: []string{semiSyncPluginSlave}, expected: semiSyncDialectSourceSlave},
		{name: "master and slave plugins", plugins: []string{semiSyncPluginMaster, semiSyncPluginSlave}, expected: semiSyncDialectSourceSlave},
		{name: "source plugin", plugins: []string{semiSyncPluginSource}, expected: semiSyncDialectSourceReplica},
		{name: "replica plugin", plugins: []string{semiSyncPluginReplica}, expected: semiSyncDialectSourceReplica},
		{name: "source and replica plugins", plugins: []string{semiSyncPluginSource, semiSyncPluginReplica}, expected: semiSyncDialectSourceReplica},
		{name: "mixed dialects", plugins: []string{semiSyncPluginMaster, semiSyncPluginReplica}, expectErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := detectSemiSyncDialect(testCase.plugins)
			if testCase.expectErr {
				require.ErrorIs(t, err, errMixedSemiSyncDialects)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestSemiSyncQueryGetters(t *testing.T) {
	type queryGetter func(*Node) (string, error)

	testCases := []struct {
		name                  string
		getter                queryGetter
		sourceSlaveExpected   string
		sourceReplicaExpected string
	}{
		{name: "status", getter: (*Node).GetSemiSyncStatusQuery, sourceSlaveExpected: querySemiSyncStatus, sourceReplicaExpected: querySemiSyncSourceReplicaStatus},
		{name: "clients", getter: (*Node).GetSemiSyncClientsQuery, sourceSlaveExpected: querySemiSyncMasterClients, sourceReplicaExpected: querySemiSyncSourceClients},
		{name: "set master", getter: (*Node).GetSemiSyncSetMasterQuery, sourceSlaveExpected: querySemiSyncSetMaster, sourceReplicaExpected: querySemiSyncSetSource},
		{name: "set slave", getter: (*Node).GetSemiSyncSetSlaveQuery, sourceSlaveExpected: querySemiSyncSetSlave, sourceReplicaExpected: querySemiSyncSetReplica},
		{name: "disable", getter: (*Node).GetSemiSyncDisableQuery, sourceSlaveExpected: querySemiSyncMasterSlaveDisable, sourceReplicaExpected: querySemiSyncSourceReplicaDisable},
		{name: "wait count", getter: (*Node).GetSemiSyncSetWaitSlaveCountQuery, sourceSlaveExpected: querySetSemiSyncWaitSlaveCount, sourceReplicaExpected: querySetSemiSyncWaitReplicaCount},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, dialectCase := range []struct {
				name      string
				dialect   semiSyncDialect
				expected  string
				expectErr bool
			}{
				{name: "disabled", dialect: semiSyncDialectDisabled, expectErr: true},
				{name: "source-slave", dialect: semiSyncDialectSourceSlave, expected: testCase.sourceSlaveExpected},
				{name: "source-replica", dialect: semiSyncDialectSourceReplica, expected: testCase.sourceReplicaExpected},
			} {
				t.Run(dialectCase.name, func(t *testing.T) {
					dialect := dialectCase.dialect
					node := &Node{semiSyncDialectCache: &dialect}
					actual, err := testCase.getter(node)
					if dialectCase.expectErr {
						require.ErrorIs(t, err, errSemiSyncDisabled)
						return
					}
					require.NoError(t, err)
					require.Equal(t, dialectCase.expected, actual)
				})
			}
		})
	}
}

func TestSourceReplicaSemiSyncQueries(t *testing.T) {
	testCases := []struct {
		queryName string
		contains  []string
	}{
		{
			queryName: querySemiSyncSourceReplicaStatus,
			contains: []string{
				"@@rpl_semi_sync_source_enabled",
				"@@rpl_semi_sync_replica_enabled",
				"@@rpl_semi_sync_source_wait_for_replica_count",
			},
		},
		{queryName: querySemiSyncSourceClients, contains: []string{"Rpl_semi_sync_source_clients"}},
		{queryName: querySemiSyncSetSource, contains: []string{"rpl_semi_sync_source_enabled = 1", "rpl_semi_sync_replica_enabled = 0"}},
		{queryName: querySemiSyncSetReplica, contains: []string{"rpl_semi_sync_replica_enabled = 1", "rpl_semi_sync_source_enabled = 0"}},
		{queryName: querySemiSyncSourceReplicaDisable, contains: []string{"rpl_semi_sync_replica_enabled = 0", "rpl_semi_sync_source_enabled = 0"}},
		{queryName: querySetSemiSyncWaitReplicaCount, contains: []string{"rpl_semi_sync_source_wait_for_replica_count"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.queryName, func(t *testing.T) {
			query := DefaultQueries[testCase.queryName]
			require.NotEmpty(t, query)
			for _, expected := range testCase.contains {
				require.Contains(t, query, expected)
			}
		})
	}
}

func TestIsUnknownSystemVariable(t *testing.T) {
	require.True(t, isUnknownSystemVariable(&mysqldriver.MySQLError{Number: 1193}))
	require.True(t, isUnknownSystemVariable(fmt.Errorf("wrapped: %w", &mysqldriver.MySQLError{Number: 1193})))
	require.False(t, isUnknownSystemVariable(&mysqldriver.MySQLError{Number: 1146}))
	require.False(t, isUnknownSystemVariable(errors.New("not a MySQL error")))
}
