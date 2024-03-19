package gtids

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

type GTIDSet = mysql.GTIDSet

func ParseGtidSet(gtidset string) *mysql.MysqlGTIDSet {
	parsed, err := mysql.ParseMysqlGTIDSet(gtidset)
	if err != nil {
		panic(err)
	}
	return parsed.(*mysql.MysqlGTIDSet)
}
