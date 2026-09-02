package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// relayLogHostStatus is the per-replica snapshot the manager writes to RelayLogStateFile.
type relayLogHostStatus struct {
	RelayLogSpace  int64  `json:"relay_log_space"`
	ShouldOptimize bool   `json:"should_optimize"`
	Error          string `json:"error,omitempty"`
}

// optimizeReplicasByRelayLog enables turbo mode on replicas whose relay logs are larger than
// RelayLogMaxBytes, at most once per RelayLogOptimizationInterval. Turbo is switched off by
// the regular optimization syncer once the replica converges.
func (app *App) optimizeReplicasByRelayLog(activeNodes []string, master string) {
	if !app.config.RelayLogOptimizationEnabled {
		return
	}
	if fi, err := os.Stat(app.config.RelayLogStateFile); err == nil {
		if time.Since(fi.ModTime()) < app.config.RelayLogOptimizationInterval {
			return
		}
	}

	statuses := make(map[string]relayLogHostStatus, len(activeNodes))
	var toOptimize []string
	for _, host := range activeNodes {
		if host == master {
			continue
		}
		node := app.cluster.Get(host)
		if node == nil {
			continue
		}
		var st relayLogHostStatus
		relaySpace, err := node.GetRelayLogSpace()
		if err != nil {
			st.Error = err.Error()
			statuses[host] = st
			app.logger.Warn().Err(err).Msgf("relay log optimization: failed to get relay log space for %s", host)
			continue
		}
		st.RelayLogSpace = relaySpace
		st.ShouldOptimize = relaySpace > app.config.RelayLogMaxBytes
		statuses[host] = st
		if st.ShouldOptimize {
			toOptimize = append(toOptimize, host)
		}
	}

	app.writeRelayLogState(statuses)

	for _, host := range toOptimize {
		node := app.cluster.Get(host)
		if node == nil {
			continue
		}
		app.logger.Info().Msgf("relay log optimization: enabling turbo on %s (relay_log_space %d > %d)",
			host, statuses[host].RelayLogSpace, app.config.RelayLogMaxBytes)
		if err := app.optController.Enable(node); err != nil {
			app.logger.Error().Err(err).Msgf("relay log optimization: failed to enable turbo on %s", host)
		}
	}
}

func (app *App) writeRelayLogState(statuses map[string]relayLogHostStatus) {
	data, err := json.MarshalIndent(statuses, "", "  ")
	if err != nil {
		app.logger.Error().Err(err).Msg("relay log optimization: failed to marshal state")
		return
	}
	if err := os.MkdirAll(filepath.Dir(app.config.RelayLogStateFile), 0o755); err != nil {
		app.logger.Error().Err(err).Msg("relay log optimization: failed to create state file dir")
		return
	}
	if err := os.WriteFile(app.config.RelayLogStateFile, data, 0o644); err != nil {
		app.logger.Error().Err(err).Msg("relay log optimization: failed to write state file")
	}
}
