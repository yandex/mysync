package gtids

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type GTIDSet = mysql.GTIDSet

func ParseGtidSet(gtidset string) mysql.GTIDSet {
	parsed, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidset)
	if err != nil {
		panic(err)
	}
	return parsed
}

func GTIDDiff(replicaGTIDSet, sourceGTIDSet mysql.GTIDSet) (string, error) {
	mysqlReplicaGTIDSet := replicaGTIDSet.(*mysql.MysqlGTIDSet)
	mysqlSourceGTIDSet := sourceGTIDSet.(*mysql.MysqlGTIDSet)
	// check standard case
	diffWithSource := mysqlSourceGTIDSet.Clone().(*mysql.MysqlGTIDSet)
	err := diffWithSource.Minus(*mysqlReplicaGTIDSet)
	if err != nil {
		return "", err
	}

	// check reverse case
	diffWithReplica := mysqlReplicaGTIDSet.Clone().(*mysql.MysqlGTIDSet)
	err = diffWithReplica.Minus(*mysqlSourceGTIDSet)
	if err != nil {
		return "", err
	}

	if diffWithSource.String() == "" && diffWithReplica.String() == "" {
		return "replica gtid equal source", nil
	}

	if diffWithSource.String() != "" && diffWithReplica.String() == "" {
		return fmt.Sprintf("source ahead on: %s", diffWithSource.String()), nil
	}

	if diffWithSource.String() != "" && diffWithReplica.String() != "" {
		return fmt.Sprintf("split brain! source ahead on: %s; replica ahead on: %s", diffWithSource.String(), diffWithReplica.String()), nil
	}

	if diffWithSource.String() == "" && diffWithReplica.String() != "" {
		return fmt.Sprintf("replica ahead on: %s", diffWithReplica.String()), nil
	}

	return "", fmt.Errorf("an indefinite case was obtained")
}
