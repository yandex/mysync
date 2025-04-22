package mysql

import (
	"database/sql"

	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/util"
)

type IExternalReplication interface {
	IsSupported(*Node) (bool, error)
	Set(*Node) error
	Reset(*Node) error
	Start(*Node) error
	GetReplicaStatus(*Node) (ReplicaStatus, error)
	Stop(*Node) error
	IsRunningByUser(*Node) bool
}

type UnimplementedExternalReplication struct{}

func (d *UnimplementedExternalReplication) IsSupported(*Node) (bool, error) {
	return false, nil
}

func (d *UnimplementedExternalReplication) IsRunningByUser(*Node) bool {
	return false
}

func (d *UnimplementedExternalReplication) Set(*Node) error {
	return nil
}

func (d *UnimplementedExternalReplication) Reset(*Node) error {
	return nil
}

func (d *UnimplementedExternalReplication) Start(*Node) error {
	return nil
}

func (d *UnimplementedExternalReplication) GetReplicaStatus(*Node) (ReplicaStatus, error) {
	return nil, nil
}

func (d *UnimplementedExternalReplication) Stop(*Node) error {
	return nil
}

type ExternalReplication struct {
	logger *log.Logger
}

func NewExternalReplication(replicationType util.ExternalReplicationType, logger *log.Logger) (IExternalReplication, error) {
	switch replicationType {
	case util.MyExternalReplication:
		logger.Info("external replication is enabled")
		return &ExternalReplication{
			logger: logger,
		}, nil
	default:
		logger.Info("external replication is disabled")
		return &UnimplementedExternalReplication{}, nil
	}
}

func (er *ExternalReplication) IsSupported(n *Node) (bool, error) {
	version, err := n.GetVersion()
	if err != nil {
		return false, err
	}
	return version.CheckIfExternalReplicationSupported(), nil
}

func (er *ExternalReplication) Set(n *Node) error {
	var replSettings replicationSettings
	err := n.queryRow(queryGetExternalReplicationSettings, nil, &replSettings)
	if err != nil {
		// If no table in scheme then we consider external replication not existing so we do nothing
		if IsErrorTableDoesNotExists(err) {
			return nil
		}
		// If there is no rows in table for external replication - do nothing
		if err == sql.ErrNoRows {
			n.logger.Infof("no external replication records found in replication table on host %s", n.host)
			return nil
		}
		return err
	}
	useSsl := 0
	sslCa := ""
	if replSettings.SourceSslCa != "" && n.config.MySQL.ExternalReplicationSslCA != "" {
		useSsl = 1
		sslCa = n.config.MySQL.ExternalReplicationSslCA
	}
	err = er.Stop(n)
	if err != nil {
		return err
	}
	err = er.Reset(n)
	if err != nil {
		return err
	}
	err = n.execMogrify(queryChangeSource, map[string]any{
		"host":            replSettings.SourceHost,
		"port":            replSettings.SourcePort,
		"user":            replSettings.SourceUser,
		"password":        replSettings.SourcePassword,
		"ssl":             useSsl,
		"sslCa":           sslCa,
		"sourceDelay":     replSettings.SourceDelay,
		"retryCount":      n.config.MySQL.ReplicationRetryCount,
		"connectRetry":    n.config.MySQL.ReplicationConnectRetry,
		"heartbeatPeriod": n.config.MySQL.ReplicationHeartbeatPeriod,
		"channel":         "external",
	})
	if err != nil {
		return err
	}
	err = n.execMogrify(queryIgnoreDB, map[string]any{
		"ignoreList": schemaname("mysql"),
		"channel":    "external",
	})
	if err != nil {
		return err
	}
	if replSettings.ShouldBeRunning() {
		return er.Start(n)
	}
	return nil
}

func (er *ExternalReplication) IsRunningByUser(n *Node) bool {
	var replSettings replicationSettings
	err := n.queryRow(queryGetExternalReplicationSettings, nil, &replSettings)
	if err != nil {
		return false
	}
	if replSettings.ShouldBeRunning() {
		return true
	}
	return false
}

// GetExternalReplicaStatus returns slave/replica status or nil if node is master for external channel
func (er *ExternalReplication) GetReplicaStatus(n *Node) (ReplicaStatus, error) {
	checked, err := er.IsSupported(n)
	if err != nil {
		return nil, err
	}
	if !(checked) {
		return nil, nil
	}

	return n.ReplicaStatusWithTimeout(n.config.DBTimeout, n.config.ExternalReplicationChannel)
}

// StartExternalReplication starts external replication
func (er *ExternalReplication) Start(n *Node) error {
	checked, err := er.IsSupported(n)
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStartReplica, map[string]any{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// StopExternalReplication stops external replication
func (er *ExternalReplication) Stop(n *Node) error {
	checked, err := er.IsSupported(n)
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStopReplica, map[string]any{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}

// ResetExternalReplicationAll resets external replication
func (er *ExternalReplication) Reset(n *Node) error {
	checked, err := er.IsSupported(n)
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryResetReplicaAll, map[string]any{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}
