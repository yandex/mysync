package app

import (
	"testing"

	"github.com/stretchr/testify/require"

	nodestate "github.com/yandex/mysync/internal/app/node_state"
	"github.com/yandex/mysync/internal/config"
)

func ns(isOffline bool) *nodestate.NodeState {
	return &nodestate.NodeState{IsOffline: isOffline}
}

func TestGetAvailabilityZone(t *testing.T) {
	cases := []struct {
		fqdn      string
		separator string
		want      string
	}{
		{"vla-mydb-1.db.yandex.net", "-", "vla"},
		{"rc1a-mydb-2.db.yandex.net", "-", "rc1a"},
		{"mydb-1", "-", "mydb"},
		// no separator in hostname means same zone as all others
		{"mysql1", "-", ""},
		{"standalone", "-", ""},
		// empty separator means all hosts in one zone
		{"vla-host-1", "", ""},
		{"", "-", ""},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, getAvailabilityZone(tc.fqdn, tc.separator), tc.fqdn)
	}
}

func TestNewOfflineModeFilter(t *testing.T) {
	require.IsType(t, &neverAllowOfflineFilter{}, NewOfflineModeFilter(&config.Config{OfflineModeMaxOfflinePct: 0}, nil))
	require.IsType(t, &neverAllowOfflineFilter{}, NewOfflineModeFilter(&config.Config{OfflineModeMaxOfflinePct: -1}, nil))
	require.IsType(t, &alwaysAllowOfflineFilter{}, NewOfflineModeFilter(&config.Config{OfflineModeMaxOfflinePct: 100}, nil))
	require.IsType(t, &alwaysAllowOfflineFilter{}, NewOfflineModeFilter(&config.Config{OfflineModeMaxOfflinePct: 110}, nil))
	require.IsType(t, &azLimitedOfflineFilter{}, NewOfflineModeFilter(&config.Config{OfflineModeMaxOfflinePct: 50}, nil))
}

func TestAlwaysAllowOfflineFilter(t *testing.T) {
	f := &alwaysAllowOfflineFilter{}
	require.True(t, f.CanSetOffline("any", nil, nil))
	require.True(t, f.CanSetOffline("any", map[string]*nodestate.NodeState{
		"vla-host-1": ns(true),
		"vla-host-2": ns(true),
	}, nil))
}

func TestAzLimitedOfflineFilter_CanSetOffline(t *testing.T) {
	const sep = "-"

	cases := []struct {
		name  string
		pct   int
		host  string
		state map[string]*nodestate.NodeState
		want  bool
	}{
		{
			name:  "empty state fail-open",
			pct:   50,
			host:  "vla-host-1",
			state: map[string]*nodestate.NodeState{},
			want:  false,
		},
		{
			name: "no-dash hostnames same zone pct=50 first allowed",
			pct:  50,
			host: "mysql2",
			state: map[string]*nodestate.NodeState{
				"mysql2": ns(false),
				"mysql3": ns(false),
			},
			want: true,
		},
		{
			name: "no-dash hostnames same zone pct=50 second blocked",
			pct:  50,
			host: "mysql3",
			state: map[string]*nodestate.NodeState{
				"mysql2": ns(true),
				"mysql3": ns(false),
			},
			want: false,
		},
		{
			name: "pct=50 two hosts none offline first allowed",
			pct:  50,
			host: "vla-host-1",
			state: map[string]*nodestate.NodeState{
				"vla-host-1": ns(false),
				"vla-host-2": ns(false),
			},
			want: true,
		},
		{
			name: "pct=50 two hosts one offline second blocked",
			pct:  50,
			host: "vla-host-2",
			state: map[string]*nodestate.NodeState{
				"vla-host-1": ns(true),
				"vla-host-2": ns(false),
			},
			want: false,
		},
		{
			name: "pct=32 three online hosts no one allowed to go offline",
			pct:  32,
			host: "vla-host-1",
			state: map[string]*nodestate.NodeState{
				"vla-host-1": ns(false),
				"vla-host-2": ns(false),
				"vla-host-3": ns(false),
			},
			want: false,
		},
		{
			name: "pct=33 three online hosts 1 allowed to go offline",
			pct:  33,
			host: "vla-host-1",
			state: map[string]*nodestate.NodeState{
				"vla-host-1": ns(false),
				"vla-host-2": ns(false),
				"vla-host-3": ns(false),
			},
			want: true,
		},
		{
			name: "other AZ offline does not affect own AZ",
			pct:  50,
			host: "vla-host-1",
			state: map[string]*nodestate.NodeState{
				"sas-host-1": ns(true),
				"sas-host-2": ns(true),
				"vla-host-1": ns(false),
				"vla-host-2": ns(false),
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &azLimitedOfflineFilter{maxOfflinePct: tc.pct, azSeparator: sep}
			require.Equal(t, tc.want, f.CanSetOffline(tc.host, tc.state, map[string]int{}))
		})
	}
}

func TestAzLimitedOfflineFilter_EmptySeparator_AllHostsSameZone(t *testing.T) {
	f := &azLimitedOfflineFilter{maxOfflinePct: 50, azSeparator: ""}
	state := map[string]*nodestate.NodeState{
		"vla-host-1": ns(false),
		"sas-host-1": ns(false),
	}
	require.True(t, f.CanSetOffline("vla-host-1", state, map[string]int{}))

	state["vla-host-1"] = ns(true)
	require.False(t, f.CanSetOffline("sas-host-1", state, map[string]int{}))
}

func TestAzLimitedOfflineFilter_PendingOfflineByAZ(t *testing.T) {
	const sep = "-"
	f := &azLimitedOfflineFilter{maxOfflinePct: 50, azSeparator: sep}

	state := map[string]*nodestate.NodeState{
		"vla-host-1": ns(false),
		"vla-host-2": ns(false),
	}

	require.True(t, f.CanSetOffline("vla-host-1", state, map[string]int{}))

	pending := map[string]int{"vla": 1}
	require.False(t, f.CanSetOffline("vla-host-2", state, pending))

	pendingOtherAZ := map[string]int{"sas": 1}
	require.True(t, f.CanSetOffline("vla-host-1", state, pendingOtherAZ))
}
