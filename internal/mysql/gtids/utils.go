package gtids

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
)

func IsSlaveBehindOrEqual(slaveGtidSet, masterGtidSet GTIDSet) bool {
	return masterGtidSet.Contain(slaveGtidSet) || masterGtidSet.Equal(slaveGtidSet)
}

func IsSlaveAhead(slaveGtidSet, masterGtidSet GTIDSet) bool {
	return !IsSlaveBehindOrEqual(slaveGtidSet, masterGtidSet)
}

func IsSplitBrained(slaveGtidSet, masterGtidSet GTIDSet, masterUUID uuid.UUID) bool {
	mysqlSlaveGtidSet := slaveGtidSet.(*gomysql.MysqlGTIDSet)
	mysqlMasterGtidSet := masterGtidSet.(*gomysql.MysqlGTIDSet)
	for slaveUUID := range *mysqlSlaveGtidSet {
		masterTagMap, ok := (*mysqlMasterGtidSet)[slaveUUID]
		if !ok {
			return true
		}

		slaveTagMap := (*mysqlSlaveGtidSet)[slaveUUID]
		for tag, slaveIntervals := range slaveTagMap {
			masterIntervals, ok := masterTagMap[tag]
			if !ok {
				return true
			}

			if masterIntervals.Contain(slaveIntervals) {
				continue
			}

			if slaveUUID == masterUUID {
				continue
			}

			return true
		}
	}

	return false
}
