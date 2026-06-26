package app

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/yandex/mysync/internal/mysql"
)

// separate goroutine performing health checks
func (app *App) healthChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.HealthCheckInterval)
	var logFile string
	var maxLogPos int64
	for {
		select {
		case <-ticker.C:
			hc := app.getLocalNodeState()
			logFile, maxLogPos = hc.UpdateBinlogStatus(logFile, maxLogPos)
			app.logger.Info().Msgf("healthcheck: %v", hc)
			err := app.SetHealthState(app.config.Hostname, hc)
			if err != nil {
				app.logger.Error().Err(err).Msg("healthcheck: failed to set status to dcs")
			}
		case <-ctx.Done():
			return
		}
	}
}

// separate gorutine performing info file management
func (app *App) stateFileHandler(ctx context.Context) {
	ticker := time.NewTicker(app.config.InfoFileHandlerInterval)
	for {
		select {
		case <-ticker.C:

			tree, err := app.dcs.GetTree("")
			if err != nil {
				app.logger.Error().Err(err).Msg("stateFileHandler: failed to get current zk tree")
				_ = os.Remove(app.config.InfoFile)
				continue
			}
			data, err := json.Marshal(tree)
			if err != nil {
				app.logger.Error().Err(err).Msg("stateFileHandler: failed to marshal zk node data")
				_ = os.Remove(app.config.InfoFile)
				continue
			}
			err = os.WriteFile(app.config.InfoFile, data, 0o666)
			if err != nil {
				app.logger.Error().Err(err).Msg("stateFileHandler: failed to write info file")
				_ = os.Remove(app.config.InfoFile)
				continue
			}

		case <-ctx.Done():
			return
		}
	}
}

// checks if update of external CA file required
func (app *App) externalCAFileChecker(ctx context.Context) {
	ticker := time.NewTicker(app.config.ExternalCAFileCheckInterval)
	for {
		select {
		case <-ticker.C:
			localNode := app.cluster.Local()
			replicaStatus, err := app.externalReplication.GetReplicaStatus(localNode)
			if err != nil {
				if !mysql.IsErrorChannelDoesNotExists(err) {
					app.logger.Error().Err(err).Msgf("external CA file checker: host %s failed to get external replica status", localNode.Host())
				}
				continue
			}
			if replicaStatus == nil {
				app.logger.Info().Msgf("external CA file checker: no external replication found on host %v", localNode.Host())
				continue
			}
			err = localNode.UpdateExternalCAFile()
			if err != nil {
				app.logger.Error().Err(err).Msg("external CA file checker: failed check and update CA file")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (app *App) replMonWriter(ctx context.Context) {
	ticker := time.NewTicker(app.config.ReplMonWriteInterval)
	for {
		select {
		case <-ticker.C:
			localNode := app.cluster.Local()
			sstatus, err := localNode.GetReplicaStatus()
			if err != nil {
				app.logger.Error().Err(err).Msg("repl mon writer: error while checking replica status")
				time.Sleep(app.config.ReplMonErrorWaitInterval)
				continue
			}
			if sstatus != nil {
				app.logger.Info().Msgf("repl mon writer: host is replica")
				time.Sleep(app.config.ReplMonSlaveWaitInterval)
				continue
			}
			readOnly, _, err := localNode.IsReadOnly()
			if err != nil {
				app.logger.Error().Err(err).Msg("repl mon writer: error while checking read only status")
				time.Sleep(app.config.ReplMonErrorWaitInterval)
				continue
			}
			if readOnly {
				app.logger.Info().Msgf("repl mon writer: host is read only")
				time.Sleep(app.config.ReplMonSlaveWaitInterval)
				continue
			}
			err = localNode.UpdateReplMonTable(app.config.ReplMonSchemeName, app.config.ReplMonTableName)
			if err != nil {
				if mysql.IsErrorTableDoesNotExists(err) {
					err = localNode.CreateReplMonTable(app.config.ReplMonSchemeName, app.config.ReplMonTableName)
					if err != nil {
						app.logger.Error().Err(err).Msg("repl mon writer: error while creating repl mon table")
					}
					continue
				}
				app.logger.Error().Err(err).Msg("repl mon writer: error while writing in repl mon table")
			}
		case <-ctx.Done():
			return
		}
	}
}
