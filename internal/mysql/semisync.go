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

	hasSourceSlave := hasMaster || hasSlave
	hasSourceReplica := hasSource || hasReplica
	if hasSourceSlave && hasSourceReplica {
		return "", errMixedSemiSyncDialects
	}
	if hasMaster != hasSlave {
		return "", fmt.Errorf("%w: both %s and %s must be loaded", errIncompleteSemiSyncDialect, semiSyncPluginMaster, semiSyncPluginSlave)
	}
	if hasSource != hasReplica {
		return "", fmt.Errorf("%w: both %s and %s must be loaded", errIncompleteSemiSyncDialect, semiSyncPluginSource, semiSyncPluginReplica)
	}
	if hasMaster {
		return semiSyncDialectSourceSlave, nil
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
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1193
}

func (n *Node) GetSemiSync() (SemiSync, error) {
	dialect, err := n.getSemiSyncDialect()
	if err != nil {
		return nil, err
	}

	switch dialect {
	case semiSyncDialectDisabled:
		return nil, errSemiSyncDisabled
	case semiSyncDialectSourceSlave:
		return new(SemiSyncMasterSlaveStatusStruct), nil
	case semiSyncDialectSourceReplica:
		return new(SemiSyncSourceReplicaStatusStruct), nil
	default:
		return nil, fmt.Errorf("unsupported semisync dialect: %s", dialect)
	}
}
