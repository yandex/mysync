package resetup

import (
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
)

const (
	prefix = "lag check"
)

type LagResetupperDcs interface {
	GetMasterHostFromDcs() (string, error)
}

type LagResetupper struct {
	logger         *log.Logger
	dcs            LagResetupperDcs
	resetupHostLag float64
}

func NewLagResetupper(l *log.Logger, dcs LagResetupperDcs, lag float64) *LagResetupper {
	return &LagResetupper{
		logger:         l,
		dcs:            dcs,
		resetupHostLag: lag,
	}
}

func (r *LagResetupper) CheckNeedResetup(cluster *mysql.Cluster) bool {
	err := cluster.UpdateHostsInfo()
	if err != nil {
		r.logger.Error().Err(err).Msgf("%s: failed to update hosts info", prefix)
		return false
	}
	localNode := cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		r.logger.Error().Err(err).Msgf("%s: host %s failed to get slave status", prefix, localNode.Host())
		return false
	}

	// definitely not a replica
	if sstatus == nil {
		return false
	}

	var master string
	master, err = r.dcs.GetMasterHostFromDcs()
	if err != nil {
		r.logger.Error().Err(err).Msgf("%s: failed to get current master from dcs", prefix)
		return false
	}

	// probably not a replica, we need to wait
	if master == localNode.Host() {
		return false
	}

	lag := sstatus.GetReplicationLag()
	// We have another problems, or small lag
	if !lag.Valid || lag.Float64 <= r.resetupHostLag {
		return false
	}

	// so, we clearly have replica with large lag
	ifOffline, err := localNode.IsOffline()
	if err != nil {
		r.logger.Error().Err(err).Msgf("%s: failed to check local node if offline", prefix)
		return false
	}

	masterNode := cluster.Get(master)
	masterRO, _, err := masterNode.IsReadOnly()
	if err != nil {
		r.logger.Error().Err(err).Msgf("%s: failed to check master (%s) if RO", prefix, master)
		return false
	}

	if ifOffline && !masterRO {
		r.logger.Info().Msgf("%s: local host set to resetup, because ReplicationLag (%f s) > ResetupHostLag (%v)",
			prefix, lag.Float64, r.resetupHostLag)

		return true
	}

	return false
}
