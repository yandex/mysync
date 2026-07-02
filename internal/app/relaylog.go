package app

import "time"

// optimizeReplicasByRelayLog enables turbo mode on replicas whose relay logs are larger than
// RelayLogMaxBytes, at most once per RelayLogOptimizationInterval. Turbo is switched off by
// the regular optimization syncer once both relay-log size and replication lag converge.
func (app *App) optimizeReplicasByRelayLog(activeNodes []string, master string) {
	if !app.config.RelayLogOptimizationEnabled {
		return
	}
	if time.Since(app.t.Get(RelayLogCheckedAt, "")) < app.config.RelayLogOptimizationInterval {
		return
	}
	app.t.Set(RelayLogCheckedAt, "", time.Now())

	for _, host := range activeNodes {
		if host == master {
			continue
		}
		node := app.cluster.Get(host)
		if node == nil {
			continue
		}
		relaySpace, err := node.GetRelayLogSpace()
		if err != nil {
			app.logger.Warn().Err(err).Msgf("relay log optimization: failed to get relay log space for %s", host)
			continue
		}
		if relaySpace <= app.config.RelayLogMaxBytes {
			continue
		}
		app.logger.Info().Msgf("relay log optimization: enabling turbo on %s (relay_log_space %d > %d)",
			host, relaySpace, app.config.RelayLogMaxBytes)
		if err := app.optController.EnableForRelayLog(node); err != nil {
			app.logger.Error().Err(err).Msgf("relay log optimization: failed to enable turbo on %s", host)
		}
	}
}
