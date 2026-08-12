package mysql

import (
	"database/sql"
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type semiSyncDialect string

const (
	semiSyncDialectDisabled      semiSyncDialect = "disabled"
	semiSyncDialectMasterSlave   semiSyncDialect = "masterSlave"
	semiSyncDialectSourceReplica semiSyncDialect = "sourceReplica"
)

const (
	semiSyncPluginMaster  = "rpl_semi_sync_master"
	semiSyncPluginSlave   = "rpl_semi_sync_slave"
	semiSyncPluginSource  = "rpl_semi_sync_source"
	semiSyncPluginReplica = "rpl_semi_sync_replica"
)

const (
	semiSyncOperationAttempts = 2
	unknownSystemVariable     = 1193 // Symbol: ER_UNKNOWN_SYSTEM_VARIABLE; SQLSTATE: HY000
)

var (
	errSemiSyncDisabled          = errors.New("semisync plugins are not loaded")
	errIncompleteSemiSyncDialect = errors.New("incomplete semisync plugin dialect")
	errMixedSemiSyncDialects     = errors.New("mixed semisync plugin dialects are not supported")
)

type semiSyncPlugin struct {
	Name string `db:"PluginName"`
}

func detectSemiSyncDialect(plugins []string) (semiSyncDialect, error) {
	hasMaster := false
	hasSlave := false
	hasSource := false
	hasReplica := false

	for _, plugin := range plugins {
		switch plugin {
		case semiSyncPluginMaster:
			hasMaster = true
		case semiSyncPluginSlave:
			hasSlave = true
		case semiSyncPluginSource:
			hasSource = true
		case semiSyncPluginReplica:
			hasReplica = true
		}
	}

	hasMasterSlave := hasMaster || hasSlave
	hasSourceReplica := hasSource || hasReplica
	if hasMasterSlave && hasSourceReplica {
		return "", errMixedSemiSyncDialects
	}
	if hasMaster != hasSlave {
		return "", fmt.Errorf("%w: both %s and %s must be loaded", errIncompleteSemiSyncDialect, semiSyncPluginMaster, semiSyncPluginSlave)
	}
	if hasSource != hasReplica {
		return "", fmt.Errorf("%w: both %s and %s must be loaded", errIncompleteSemiSyncDialect, semiSyncPluginSource, semiSyncPluginReplica)
	}
	if hasMaster {
		return semiSyncDialectMasterSlave, nil
	}
	if hasSource {
		return semiSyncDialectSourceReplica, nil
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
	// The disabled state can change while mysync is running after plugins are
	// installed or mysqld is restarted with a different plugin configuration.
	// Keep probing in that state so it cannot become a permanent stale cache.
	if dialect != semiSyncDialectDisabled {
		n.semiSyncDialectCache = &dialect
	}
	return dialect, nil
}

func (n *Node) resetSemiSyncDialect() {
	n.semiSyncMu.Lock()
	defer n.semiSyncMu.Unlock()
	n.semiSyncDialectCache = nil
}

func isUnknownSystemVariable(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == unknownSystemVariable
}

func isStaleSemiSyncDialectError(err error) bool {
	return isUnknownSystemVariable(err) || errors.Is(err, sql.ErrNoRows)
}

func (n *Node) trySemiSync(operation func(SemiSync) error, attempts int) (SemiSync, error) {
	if attempts < 1 {
		return nil, errors.New("semisync operation attempts must be positive")
	}

	var semiSync SemiSync
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		semiSync, err = n.getSemiSync()
		if err != nil {
			return nil, err
		}

		err = operation(semiSync)
		if err == nil || !isStaleSemiSyncDialectError(err) || attempt == attempts-1 {
			return semiSync, err
		}

		n.resetSemiSyncDialect()
	}

	return semiSync, err
}

func (n *Node) getSemiSync() (SemiSync, error) {
	dialect, err := n.getSemiSyncDialect()
	if err != nil {
		return nil, err
	}

	switch dialect {
	case semiSyncDialectDisabled:
		return nil, errSemiSyncDisabled
	case semiSyncDialectMasterSlave:
		return new(SemiSyncMasterSlaveStatusStruct), nil
	case semiSyncDialectSourceReplica:
		return new(SemiSyncSourceReplicaStatusStruct), nil
	default:
		return nil, fmt.Errorf("unsupported semisync dialect: %s", dialect)
	}
}
