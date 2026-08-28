package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultSwitchoverTimeout(t *testing.T) {
	cfg, err := DefaultConfig()
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, cfg.SwitchoverTimeout)
}
