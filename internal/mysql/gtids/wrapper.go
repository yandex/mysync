package gtids

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

type GTIDSet = mysql.GTIDSet

func ParseGtidSet(gtidset string) GTIDSet {
	parsed, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidset)
	if err != nil {
		panic(err)
	}
	return parsed
}
