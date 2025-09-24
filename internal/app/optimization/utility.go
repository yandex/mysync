package optimization

import (
	"errors"

	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

func disableHost(
	node NodeReplicationController,
	rs mysql.ReplicationSettings,
) error {
	return node.SetReplicationSettings(rs)
}

func enableHost(
	node NodeReplicationController,
	DCS dcs.DCS,
) error {
	err := DCS.Set(dcs.JoinPath(pathOptimizationNodes, node.Host()), StatusEnabled)
	if err != nil {
		return err
	}
	return node.OptimizeReplication()
}

func disableHostWithDCS(
	node NodeReplicationController,
	rs mysql.ReplicationSettings,
	DCS dcs.DCS,
) error {
	err := DCS.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
	if err != nil {
		return err
	}
	return node.SetReplicationSettings(rs)
}

func isOptimal(
	node NodeReplicationController,
	lagThreshold float64,
) (bool, float64, error) {
	replicationStatus, err := node.GetReplicaStatus()
	if err != nil {
		return false, 0.0, err
	}

	lag := replicationStatus.GetReplicationLag()
	if lag.Valid && lag.Float64 < lagThreshold {
		return true, lag.Float64, nil
	}
	if !lag.Valid {
		return false, lag.Float64, errors.New("replication lag is not valid")
	}

	return false, lag.Float64, nil
}

func nodeExist(
	node NodeReplicationController,
	DCS dcs.DCS,
) (bool, error) {
	var status string
	err := DCS.Get(dcs.JoinPath(pathOptimizationNodes, node.Host()), &status)
	if err != nil && err != dcs.ErrNotFound && err != dcs.ErrMalformed {
		return false, err
	}
	if err != nil && (err == dcs.ErrNotFound || err == dcs.ErrMalformed) {
		return false, nil
	}
	return true, nil
}
