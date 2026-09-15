package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yandex/mysync/internal/config"
)

func TestRelayLogCheckInterval(t *testing.T) {
	app := newTestApp(t, &config.Config{
		RelayLogOptimizationEnabled:  true,
		RelayLogOptimizationInterval: 5 * time.Minute,
	}, nil)

	app.optimizeReplicasByRelayLog(nil, "master")
	require.False(t, app.t.Get(RelayLogCheckedAt, "").IsZero())

	checkedAt := time.Now().Add(-time.Minute)
	app.t.Set(RelayLogCheckedAt, "", checkedAt)
	app.optimizeReplicasByRelayLog(nil, "master")
	require.Equal(t, checkedAt, app.t.Get(RelayLogCheckedAt, ""))

	app.t.Set(RelayLogCheckedAt, "", time.Now().Add(-6*time.Minute))
	app.optimizeReplicasByRelayLog(nil, "master")
	require.True(t, app.t.Get(RelayLogCheckedAt, "").After(checkedAt))
}
