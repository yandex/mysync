package mysql

import (
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type semiSyncDialect string

const (
	semiSyncDialectDisabled      semiSyncDialect = "disabled"
	semiSyncDialectSourceSlave   semiSyncDialect = "sourceslave"
	semiSyncDialectSourceReplica semiSyncDialect = "sourceReplica"
)

type semiSyncOperation uint8

const (
	semiSyncOperationStatus semiSyncOperation = iota
	semiSyncOperationSetMaster
	semiSyncOperationSetSlave
	semiSyncOperationDisable
	semiSyncOperationSetWaitCount
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

func semiSyncQueryName(dialect semiSyncDialect, operation semiSyncOperation) (string, bool) {
	if dialect == semiSyncDialectDisabled {
		return "", false
	}

	switch dialect {
	case semiSyncDialectSourceSlave:
		switch operation {
		case semiSyncOperationStatus:
			return querySemiSyncStatus, true
		case semiSyncOperationSetMaster:
			return querySemiSyncSetMaster, true
		case semiSyncOperationSetSlave:
			return querySemiSyncSetSlave, true
		case semiSyncOperationDisable:
			return querySemiSyncMasterSlaveDisable, true
		case semiSyncOperationSetWaitCount:
			return querySetSemiSyncWaitSlaveCount, true
		}
	case semiSyncDialectSourceReplica:
		switch operation {
		case semiSyncOperationStatus:
			return querySemiSyncSourceReplicaStatus, true
		case semiSyncOperationSetMaster:
			return querySemiSyncSetSource, true
		case semiSyncOperationSetSlave:
			return querySemiSyncSetReplica, true
		case semiSyncOperationDisable:
			return querySemiSyncSourceReplicaDisable, true
		case semiSyncOperationSetWaitCount:
			return querySetSemiSyncWaitReplicaCount, true
		}
	}

	return "", false
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

func (n *Node) semiSyncStatus() (*SemiSyncStatus, error) {
	status := new(SemiSyncStatus)
	for attempt := 0; attempt < 2; attempt++ {
		dialect, err := n.getSemiSyncDialect()
		if err != nil {
			return status, err
		}

		queryName, ok := semiSyncQueryName(dialect, semiSyncOperationStatus)
		if !ok {
			return status, nil
		}

		err = n.queryRow(queryName, nil, status)
		if err == nil {
			return status, nil
		}
		if !isUnknownSystemVariable(err) {
			return status, err
		}
		n.resetSemiSyncDialect()
		if attempt == 0 {
			status = new(SemiSyncStatus)
			continue
		}
		return status, err
	}

	return status, nil
}

func (n *Node) execSemiSync(operation semiSyncOperation, arg map[string]any) error {
	for attempt := 0; attempt < 2; attempt++ {
		dialect, err := n.getSemiSyncDialect()
		if err != nil {
			return err
		}

		queryName, ok := semiSyncQueryName(dialect, operation)
		if !ok {
			if operation == semiSyncOperationDisable {
				return nil
			}
			return errSemiSyncDisabled
		}

		err = n.exec(queryName, arg)
		if !isUnknownSystemVariable(err) {
			return err
		}
		n.resetSemiSyncDialect()
		if attempt == 1 {
			return err
		}
	}

	return nil
}
