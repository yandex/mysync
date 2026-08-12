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

func TestGetSemiSync(t *testing.T) {
	testCases := []struct {
		name                   string
		dialect                semiSyncDialect
		expected               SemiSync
		statusQuery            string
		clientsQuery           string
		setMasterQuery         string
		setSlaveQuery          string
		disableQuery           string
		setWaitSlaveCountQuery string
		expectErr              bool
	}{
		{name: "disabled", dialect: semiSyncDialectDisabled, expectErr: true},
		{
			name:                   "source-slave",
			dialect:                semiSyncDialectSourceSlave,
			expected:               new(SemiSyncMasterSlaveStatusStruct),
			statusQuery:            querySemiSyncStatus,
			clientsQuery:           querySemiSyncMasterClients,
			setMasterQuery:         querySemiSyncSetMaster,
			setSlaveQuery:          querySemiSyncSetSlave,
			disableQuery:           querySemiSyncMasterSlaveDisable,
			setWaitSlaveCountQuery: querySetSemiSyncWaitSlaveCount,
		},
		{
			name:                   "source-replica",
			dialect:                semiSyncDialectSourceReplica,
			expected:               new(SemiSyncSourceReplicaStatusStruct),
			statusQuery:            querySemiSyncSourceReplicaStatus,
			clientsQuery:           querySemiSyncSourceClients,
			setMasterQuery:         querySemiSyncSetSource,
			setSlaveQuery:          querySemiSyncSetReplica,
			disableQuery:           querySemiSyncSourceReplicaDisable,
			setWaitSlaveCountQuery: querySetSemiSyncWaitReplicaCount,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dialect := testCase.dialect
			node := &Node{semiSyncDialectCache: &dialect}
			actual, err := node.GetSemiSync()
			if testCase.expectErr {
				require.ErrorIs(t, err, errSemiSyncDisabled)
				require.Nil(t, actual)
				return
			}
			require.NoError(t, err)
			require.IsType(t, testCase.expected, actual)
			require.Equal(t, testCase.statusQuery, actual.GetStatusQuery())
			require.Equal(t, testCase.clientsQuery, actual.GetClientsQuery())
			require.Equal(t, testCase.setMasterQuery, actual.GetSetMasterQuery())
			require.Equal(t, testCase.setSlaveQuery, actual.GetSetSlaveQuery())
			require.Equal(t, testCase.disableQuery, actual.GetDisableQuery())
			require.Equal(t, testCase.setWaitSlaveCountQuery, actual.GetSetWaitSlaveCountQuery())
		})
	}
}

func TestSemiSyncStatusImplementations(t *testing.T) {
	testCases := []struct {
		name           string
		status         SemiSyncStatus
		masterEnabled  bool
		slaveEnabled   bool
		waitSlaveCount int
	}{
		{name: "disabled", status: new(SemiSyncDisabledStatusStruct)},
		{
			name: "master-slave",
			status: &SemiSyncMasterSlaveStatusStruct{
				MasterEnabledValue:  1,
				SlaveEnabledValue:   0,
				WaitSlaveCountValue: 2,
			},
			masterEnabled:  true,
			waitSlaveCount: 2,
		},
		{
			name: "source-replica",
			status: &SemiSyncSourceReplicaStatusStruct{
				SourceEnabledValue:  0,
				ReplicaEnabledValue: 1,
				WaitReplicaCount:    3,
			},
			slaveEnabled:   true,
			waitSlaveCount: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.masterEnabled, testCase.status.MasterEnabled())
			require.Equal(t, testCase.slaveEnabled, testCase.status.SlaveEnabled())
			require.Equal(t, testCase.waitSlaveCount, testCase.status.GetWaitSlaveCount())
		})
	}
}

func TestSemiSyncStatusReturnsDisabledImplementation(t *testing.T) {
	dialect := semiSyncDialectDisabled
	node := &Node{semiSyncDialectCache: &dialect}

	status, err := node.SemiSyncStatus()
	require.NoError(t, err)
	require.IsType(t, new(SemiSyncDisabledStatusStruct), status)
	require.False(t, status.MasterEnabled())
	require.False(t, status.SlaveEnabled())
	require.Zero(t, status.GetWaitSlaveCount())
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
				"AS SourceEnabled",
				"AS ReplicaEnabled",
				"as WaitReplicaCount",
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
