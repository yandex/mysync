package mysql

import "github.com/go-sql-driver/mysql"

var dubiousErrorNumbers = []uint16{
	1040, // Symbol: ER_CON_COUNT_ERROR; SQLSTATE: 08004
	1129, // Symbol: ER_HOST_IS_BLOCKED; SQLSTATE: HY000
	1130, // Symbol: ER_HOST_NOT_PRIVILEGED; SQLSTATE: HY000
	1203, // Symbol: ER_TOO_MANY_USER_CONNECTIONS; SQLSTATE: 42000
	3159, // Symbol: ER_SECURE_TRANSPORT_REQUIRED; SQLSTATE: HY000
	1045, // Symbol: ER_ACCESS_DENIED_ERROR; SQLSTATE: 28000
	1044, // Symbol: ER_DBACCESS_DENIED_ERROR; SQLSTATE: 42000
	1698, // Symbol: ER_ACCESS_DENIED_NO_PASSWORD_ERROR; SQLSTATE: 28000
}

const channelDoesNotExists = 3074 // Symbol: ER_REPLICA_CHANNEL_DOES_NOT_EXIST; SQLSTATE: HY000
const tableDoesNotExists = 1146   // Symbol: ER_NO_SUCH_TABLE; SQLSTATE: 42S02

// IsErrorDubious check that error may be caused by misconfiguration, mysync/scripts bugs
// and not related to MySQL/network failure
func IsErrorDubious(err error) bool {
	if err == nil {
		return false
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	for _, errno := range dubiousErrorNumbers {
		if mysqlErr.Number == errno {
			return true
		}
	}
	return false
}

func IsErrorChannelDoesNotExists(err error) bool {
	if err == nil {
		return false
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	if mysqlErr.Number == channelDoesNotExists {
		return true
	}
	return false
}

func IsErrorTableDoesNotExists(err error) bool {
	if err == nil {
		return false
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	if mysqlErr.Number == tableDoesNotExists {
		return true
	}
	return false
}
