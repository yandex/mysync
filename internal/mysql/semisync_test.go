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

func TestSemiSyncQueryName(t *testing.T) {
	testCases := []struct {
		name      string
		dialect   semiSyncDialect
		operation semiSyncOperation
		expected  string
		ok        bool
	}{
		{name: "disabled status", dialect: semiSyncDialectDisabled, operation: semiSyncOperationStatus},
		{name: "disabled set master", dialect: semiSyncDialectDisabled, operation: semiSyncOperationSetMaster},
		{name: "disabled set slave", dialect: semiSyncDialectDisabled, operation: semiSyncOperationSetSlave},
		{name: "disabled disable", dialect: semiSyncDialectDisabled, operation: semiSyncOperationDisable},
		{name: "disabled wait count", dialect: semiSyncDialectDisabled, operation: semiSyncOperationSetWaitCount},
		{name: "source-slave status", dialect: semiSyncDialectSourceSlave, operation: semiSyncOperationStatus, expected: querySemiSyncStatus, ok: true},
		{name: "source-slave set master", dialect: semiSyncDialectSourceSlave, operation: semiSyncOperationSetMaster, expected: querySemiSyncSetMaster, ok: true},
		{name: "source-slave set slave", dialect: semiSyncDialectSourceSlave, operation: semiSyncOperationSetSlave, expected: querySemiSyncSetSlave, ok: true},
		{name: "source-slave disable", dialect: semiSyncDialectSourceSlave, operation: semiSyncOperationDisable, expected: querySemiSyncMasterSlaveDisable, ok: true},
		{name: "source-slave wait count", dialect: semiSyncDialectSourceSlave, operation: semiSyncOperationSetWaitCount, expected: querySetSemiSyncWaitSlaveCount, ok: true},
		{name: "source-replica status", dialect: semiSyncDialectSourceReplica, operation: semiSyncOperationStatus, expected: querySemiSyncSourceReplicaStatus, ok: true},
		{name: "source-replica set source", dialect: semiSyncDialectSourceReplica, operation: semiSyncOperationSetMaster, expected: querySemiSyncSetSource, ok: true},
		{name: "source-replica set replica", dialect: semiSyncDialectSourceReplica, operation: semiSyncOperationSetSlave, expected: querySemiSyncSetReplica, ok: true},
		{name: "source-replica disable", dialect: semiSyncDialectSourceReplica, operation: semiSyncOperationDisable, expected: querySemiSyncSourceReplicaDisable, ok: true},
		{name: "source-replica wait count", dialect: semiSyncDialectSourceReplica, operation: semiSyncOperationSetWaitCount, expected: querySetSemiSyncWaitReplicaCount, ok: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, ok := semiSyncQueryName(testCase.dialect, testCase.operation)
			require.Equal(t, testCase.ok, ok)
			require.Equal(t, testCase.expected, actual)
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
