package gtids

import (
	"fmt"

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

// intervalSliceMinus returns intervals that are in 'a' but not in 'b' (a \ b).
// Both slices must be normalized (sorted, non-overlapping).
func intervalSliceMinus(a, b mysql.IntervalSlice) mysql.IntervalSlice {
	var result mysql.IntervalSlice
	bi := 0
	for _, iv := range a {
		cur := iv.Start
		for cur < iv.Stop {
			// advance b past intervals that end before cur
			for bi < len(b) && b[bi].Stop <= cur {
				bi++
			}
			if bi >= len(b) || b[bi].Start >= iv.Stop {
				// no more b intervals overlap [cur, iv.Stop)
				result = append(result, mysql.Interval{Start: cur, Stop: iv.Stop})
				break
			}
			// b[bi] overlaps [cur, iv.Stop)
			if b[bi].Start > cur {
				result = append(result, mysql.Interval{Start: cur, Stop: b[bi].Start})
			}
			cur = b[bi].Stop
		}
	}
	return result
}

// mysqlGTIDSetMinus returns a new MysqlGTIDSet containing intervals that are
// in 'a' but not in 'b' (i.e. a \ b).
func mysqlGTIDSetMinus(a, b *mysql.MysqlGTIDSet) *mysql.MysqlGTIDSet {
	result := mysql.NewMysqlGTIDSet()
	for uid, aTagMap := range *a {
		bTagMap := (*b)[uid]
		for tag, aIntervals := range aTagMap {
			var diff mysql.IntervalSlice
			if bTagMap == nil {
				diff = aIntervals
			} else {
				bIntervals := bTagMap[tag]
				if bIntervals == nil {
					diff = aIntervals
				} else {
					diff = intervalSliceMinus(aIntervals, bIntervals)
				}
			}
			if len(diff) > 0 {
				if result[uid] == nil {
					result[uid] = make(map[mysql.Tag]mysql.IntervalSlice)
				}
				result[uid][tag] = diff
			}
		}
	}
	return &result
}

func GTIDDiff(replicaGTIDSet, sourceGTIDSet mysql.GTIDSet) (string, error) {
	mysqlReplicaGTIDSet := replicaGTIDSet.(*mysql.MysqlGTIDSet)
	mysqlSourceGTIDSet := sourceGTIDSet.(*mysql.MysqlGTIDSet)

	// intervals in source but not in replica
	diffWithSource := mysqlGTIDSetMinus(mysqlSourceGTIDSet, mysqlReplicaGTIDSet)
	// intervals in replica but not in source
	diffWithReplica := mysqlGTIDSetMinus(mysqlReplicaGTIDSet, mysqlSourceGTIDSet)

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
