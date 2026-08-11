package mysql

import (
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type semiSyncDialect string

const (
	semiSyncDialectDisabled      semiSyncDialect = "disabled"
	semiSyncDialectSourceSlave   semiSyncDialect = "sourceslave"
	semiSyncDialectSourceReplica semiSyncDialect = "sourceReplica"
)

const (
	semiSyncPluginMaster  = "rpl_semi_sync_master"
	semiSyncPluginSlave   = "rpl_semi_sync_slave"
	semiSyncPluginSource  = "rpl_semi_sync_source"
	semiSyncPluginReplica = "rpl_semi_sync_replica"
)

var (
	errSemiSyncDisabled      = errors.New("semisync plugins are not loaded")
	errMixedSemiSyncDialects = errors.New("mixed semisync plugin dialects are not supported")
)

type semiSyncPlugin struct {
	Name string `db:"PluginName"`
}

func detectSemiSyncDialect(plugins []string) (semiSyncDialect, error) {
	hasSourceSlave := false
	hasSourceReplica := false

	for _, plugin := range plugins {
		switch plugin {
		case semiSyncPluginMaster, semiSyncPluginSlave:
			hasSourceSlave = true
		case semiSyncPluginSource, semiSyncPluginReplica:
			hasSourceReplica = true
		}
	}

	if hasSourceSlave && hasSourceReplica {
		return "", errMixedSemiSyncDialects
	}
	if hasSourceReplica {
		return semiSyncDialectSourceReplica, nil
	}
	if hasSourceSlave {
		return semiSyncDialectSourceSlave, nil
	}
	return semiSyncDialectDisabled, nil
}

func (n *Node) getSemiSyncDialect() (semiSyncDialect, error) {
	n.semiSyncMu.Lock()
	defer n.semiSyncMu.Unlock()

	if n.semiSyncDialectCache != nil {
		return *n.semiSyncDialectCache, nil
	}

	plugins := make([]string, 0, 2)
	err := n.queryRows(querySemiSyncPlugins, nil, func(rows *sqlx.Rows) error {
		var plugin semiSyncPlugin
		if err := rows.StructScan(&plugin); err != nil {
			return err
		}
		plugins = append(plugins, plugin.Name)
		return nil
	})
	if err != nil {
		return "", err
	}

	dialect, err := detectSemiSyncDialect(plugins)
	if err != nil {
		return "", err
	}
	n.semiSyncDialectCache = &dialect
	return dialect, nil
}

func (n *Node) resetSemiSyncDialect() {
	n.semiSyncMu.Lock()
	defer n.semiSyncMu.Unlock()
	n.semiSyncDialectCache = nil
}

func isUnknownSystemVariable(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1193
}

func (n *Node) getSemiSyncQuery(sourceSlaveQuery string, sourceReplicaQuery string) (string, error) {
	dialect, err := n.getSemiSyncDialect()
	if err != nil {
		return "", err
	}

	switch dialect {
	case semiSyncDialectDisabled:
		return "", errSemiSyncDisabled
	case semiSyncDialectSourceSlave:
		return sourceSlaveQuery, nil
	case semiSyncDialectSourceReplica:
		return sourceReplicaQuery, nil
	default:
		return "", fmt.Errorf("unsupported semisync dialect: %s", dialect)
	}
}

func (n *Node) GetSemiSyncStatusQuery() (string, error) {
	return n.getSemiSyncQuery(querySemiSyncStatus, querySemiSyncSourceReplicaStatus)
}

func (n *Node) GetSemiSyncClientsQuery() (string, error) {
	return n.getSemiSyncQuery(querySemiSyncMasterClients, querySemiSyncSourceClients)
}

func (n *Node) GetSemiSyncSetMasterQuery() (string, error) {
	return n.getSemiSyncQuery(querySemiSyncSetMaster, querySemiSyncSetSource)
}

func (n *Node) GetSemiSyncSetSlaveQuery() (string, error) {
	return n.getSemiSyncQuery(querySemiSyncSetSlave, querySemiSyncSetReplica)
}

func (n *Node) GetSemiSyncDisableQuery() (string, error) {
	return n.getSemiSyncQuery(querySemiSyncMasterSlaveDisable, querySemiSyncSourceReplicaDisable)
}

func (n *Node) GetSemiSyncSetWaitSlaveCountQuery() (string, error) {
	return n.getSemiSyncQuery(querySetSemiSyncWaitSlaveCount, querySetSemiSyncWaitReplicaCount)
}
