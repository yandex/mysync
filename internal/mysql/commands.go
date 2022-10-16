package mysql

const (
	commandStatus = "status"
)

var defaultCommands = map[string]string{
	commandStatus: `service mysql status`,
}
