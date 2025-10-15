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
		r.logger.Errorf("%s: failed to update hosts info: %v", prefix, err)
		return false
	}
	localNode := cluster.Local()
	sstatus, err := localNode.GetReplicaStatus()
	if err != nil {
		r.logger.Errorf("%s: host %s failed to get slave status %v", prefix, localNode.Host(), err)
		return false
	}

	// definitely not a replica
	if sstatus == nil {
		return false
	}

	var master string
	master, err = r.dcs.GetMasterHostFromDcs()
	if err != nil {
		r.logger.Errorf("%s: failed to get current master from dcs: %v", prefix, err)
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
		r.logger.Errorf("%s: failed to check local node if offline: %v", prefix, err)
		return false
	}

	masterNode := cluster.Get(master)
	masterRO, _, err := masterNode.IsReadOnly()
	if err != nil {
		r.logger.Errorf("%s: failed to check master (%s) if RO: %v", prefix, master, err)
		return false
	}

	if ifOffline && !masterRO {
		r.logger.Infof("%s: local host set to resetup, because ReplicationLag (%f s) > ResetupHostLag (%v)",
			prefix, lag.Float64, r.resetupHostLag)

		return true
	}

	return false
}
