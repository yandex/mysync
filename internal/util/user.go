package util

import (
	"os"
	"slices"

	"github.com/shirou/gopsutil/v3/process"
)

var notInformativeUsernames = []string{"root", "mysql"}

func GuessWhoRunning() string {
	pid := os.Getppid()

	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return ""
	}

	for i := 0; i < 50; i++ {
		if p == nil {
			return "unknown_dolphin"
		}

		p, err = p.Parent()
		if err != nil {
			return "unknown_sakila"
		}

		// Known issue: cross-compiled builds by default uses CGO_ENABLED="0" (aka static builds)
		//     this may break user.LookupId() for LDAP/NIS users (user.UnknownUserError returned)
		username, err := p.Username()
		if err != nil {
			return ""
		}
		if !slices.Contains(notInformativeUsernames, username) {
			return username
		}
	}
	return "unknown"
}
