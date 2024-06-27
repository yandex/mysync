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
	for _, slaveSet := range mysqlSlaveGtidSet.Sets {
		masterSet, ok := mysqlMasterGtidSet.Sets[slaveSet.SID.String()]
		if !ok {
			return true
		}

		if masterSet.Contain(slaveSet) {
			continue
		}

		if masterSet.SID == masterUUID {
			continue
		}

		return true
	}

	return false
}
