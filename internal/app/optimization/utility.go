package optimization

import (
	"errors"
	"strings"

	"github.com/go-zookeeper/zk"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/mysql"
)

func disableHostWithDCS(
	node NodeReplicationController,
	rs mysql.ReplicationSettings,
	DCS DCS,
) error {
	err := node.SetReplicationSettings(rs)
	if err != nil {
		return err
	}
	return DCS.Delete(dcs.JoinPath(pathOptimizationNodes, node.Host()))
}

func isOptimized(
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

func isNodeOptimizing(
	node NodeReplicationController,
	DCS DCS,
) (bool, error) {
	var state DCSState
	err := DCS.Get(dcs.JoinPath(pathOptimizationNodes, node.Host()), &state)
	if err != nil && err != zk.ErrNoNode && err != dcs.ErrMalformed {
		return false, err
	}
	if err != nil && (err == zk.ErrNoNode || err == dcs.ErrMalformed) {
		return false, nil
	}
	return true, nil
}

func Union[T any](slices ...[]T) []T {
	res := make([]T, 0)
	for _, s := range slices {
		res = append(res, s...)
	}
	return res
}

func Ptr[T any](v T) *T {
	return &v
}

func JoinErrors(errors []error, sep string) string {
	strs := make([]string, 0, len(errors))
	for _, err := range errors {
		strs = append(strs, err.Error())
	}
	return strings.Join(strs, sep)
}
