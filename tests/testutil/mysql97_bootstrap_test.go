package testutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQL97BootstrapLeavesSystemViewsWritable(t *testing.T) {
	startScriptPath := filepath.Join("..", "images", "mysql", "start_mysql_97.sh")
	startScript, err := os.ReadFile(startScriptPath)
	require.NoError(t, err)
	require.NotContains(t, string(startScript), "SET GLOBAL super_read_only = 1;",
		"MySQL 9.7 creates system views after init_file, so bootstrap must remain writable")

	configPath := filepath.Join("..", "images", "mysql", "my.cnf.9.7")
	config, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Contains(t, string(config), "super_read_only = ON",
		"normal MySQL 9.7 startup must still begin in super-read-only mode")
}
