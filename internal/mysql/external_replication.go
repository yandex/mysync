package mysql

import (
	"database/sql"
)

type IExternalReplication interface {
	StartExternalReplication() error
	StopExternalReplication() error
	GetExternalReplicaStatus() (ReplicaStatus, error)
	SetExternalReplication() error
	IsExternalReplicationSupported() (bool, error)
	ResetExternalReplicationAll() error
}

type DummyExternalReplication struct {
	isExternalReplicationSupported bool
}

func (d *DummyExternalReplication) StartExternalReplication() error {
	return nil
}

func (d *DummyExternalReplication) StopExternalReplication() error {
	return nil
}

func (d *DummyExternalReplication) GetExternalReplicaStatus() (ReplicaStatus, error) {
	return nil, nil
}

func (d *DummyExternalReplication) SetExternalReplication() error {
	return nil
}

func (d *DummyExternalReplication) IsExternalReplicationSupported() (bool, error) {
	return d.isExternalReplicationSupported, nil
}

func (d *DummyExternalReplication) ResetExternalReplicationAll() error {
	return nil
}

type ExternalReplication struct {
	*dbWorker
}

// StartExternalReplication starts external replication
func (n *ExternalReplication) StartExternalReplication() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStartReplica, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// StopExternalReplication stops external replication
func (n *ExternalReplication) StopExternalReplication() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryStopReplica, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}

// GetExternalReplicaStatus returns slave/replica status or nil if node is master for external channel
func (n *ExternalReplication) GetExternalReplicaStatus() (ReplicaStatus, error) {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return nil, err
	}
	if !(checked) {
		return nil, nil
	}
	status := new(ReplicaStatusStruct)
	err = n.queryRowMogrifyWithTimeout(queryReplicaStatus, map[string]interface{}{
		"channel": n.config.ExternalReplicationChannel,
	}, status, n.config.DBTimeout)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return status, err
}

func (n *ExternalReplication) SetExternalReplication() error {
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
	err = n.StopExternalReplication()
	if err != nil {
		return err
	}
	err = n.ResetExternalReplicationAll()
	if err != nil {
		return err
	}
	err = n.execMogrify(queryChangeSource, map[string]interface{}{
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
	err = n.execMogrify(queryIgnoreDB, map[string]interface{}{
		"ignoreList": schemaname("mysql"),
		"channel":    "external",
	})
	if err != nil {
		return err
	}
	return n.StartExternalReplication()
}

func (n *ExternalReplication) IsExternalReplicationSupported() (bool, error) {
	if !n.config.IsExternalReplicationSupported {
		return false, nil
	}
	version, err := n.GetVersion()
	if err != nil {
		return false, err
	}
	return version.CheckIfExternalReplicationSupported(), nil
}

// ResetExternalReplicationAll resets external replication
func (n *ExternalReplication) ResetExternalReplicationAll() error {
	checked, err := n.IsExternalReplicationSupported()
	if err != nil {
		return err
	}
	if checked {
		err := n.execMogrify(queryResetReplicaAll, map[string]interface{}{
			"channel": n.config.ExternalReplicationChannel,
		})
		if err != nil && !IsErrorChannelDoesNotExists(err) {
			return err
		}
	}
	return nil
}
