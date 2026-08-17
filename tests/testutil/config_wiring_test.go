package testutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const replicationRepairMaxAttemptsEnv = "MYSYNC_REPLICATION_REPAIR_MAX_ATTEMPTS"

func TestReplicationRepairMaxAttemptsEnvironmentIsWired(t *testing.T) {
	type serviceConfig struct {
		Environment map[string]*string `yaml:"environment"`
	}
	type composeConfig struct {
		Services map[string]serviceConfig `yaml:"services"`
	}

	composePath := filepath.Join("..", "images", "docker-compose.yaml")
	composeData, err := os.ReadFile(composePath)
	require.NoError(t, err)

	var compose composeConfig
	require.NoError(t, yaml.Unmarshal(composeData, &compose))
	for _, serviceName := range []string{"mysql1", "mysql2", "mysql3"} {
		service, ok := compose.Services[serviceName]
		require.Truef(t, ok, "service %s is missing from %s", serviceName, composePath)
		_, ok = service.Environment[replicationRepairMaxAttemptsEnv]
		require.Truef(t, ok, "%s is not passed to service %s", replicationRepairMaxAttemptsEnv, serviceName)
	}

	configPath := filepath.Join("..", "images", "mysql", "mysync.yaml")
	configData, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Contains(t, string(configData),
		"replication_repair_max_attempts: ${MYSYNC_REPLICATION_REPAIR_MAX_ATTEMPTS:-3}")
}
