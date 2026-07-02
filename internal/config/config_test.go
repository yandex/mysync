package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfigRelayLogOptimization(t *testing.T) {
	cfg, err := DefaultConfig()
	require.NoError(t, err)

	require.False(t, cfg.RelayLogOptimizationEnabled)
	require.Equal(t, 5*time.Minute, cfg.RelayLogOptimizationInterval)
	require.NotEmpty(t, cfg.RelayLogStateFile)
	require.Equal(t, DefaultRelayLogMaxBytes, cfg.RelayLogMaxBytes)
	require.Equal(t, int64(1<<30), cfg.RelayLogMaxBytes)
}
