package log

import (
	"log/syslog"
	"os"
)

func WriteSysLogInfo(syslog *syslog.Writer, msg string) {
	if syslog != nil {
		// nolint: errcheck
		syslog.Info(msg)
	} else {
		os.Stderr.WriteString(msg)
	}
}

func WriteSysLogError(syslog *syslog.Writer, msg string) {
	if syslog != nil {
		// nolint: errcheck
		syslog.Err(msg)
	} else {
		os.Stderr.WriteString(msg)
	}
}
