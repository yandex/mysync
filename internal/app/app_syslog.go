package app

import "log/syslog"

func writeSysLogInfo(syslog *syslog.Writer, msg string) {
	// nolint: errcheck
	syslog.Info(msg)
}
