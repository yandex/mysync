package app

import "os"

func (app *App) writeEmergeFile(msg string) {
	app.logger.Warn().Msg("touch emerge file")
	err := os.WriteFile(app.config.Emergefile, []byte(msg), 0o644)
	if err != nil {
		app.logger.Error().Err(err).Msg("failed to write emerge file")
	}
}

func (app *App) writeResetupFile() {
	app.logger.Warn().Msg("touch resetup file")
	err := os.WriteFile(app.config.Resetupfile, []byte{}, 0o644)
	if err != nil {
		app.logger.Error().Err(err).Msg("failed to write resetup file")
	}
}

func (app *App) doesResetupFileExist() bool {
	_, err := os.Stat(app.config.Resetupfile)
	return err == nil
}

func (app *App) writeMaintenanceFile() {
	app.logger.Warn().Msg("touch maintenance file")
	err := os.WriteFile(app.config.Maintenancefile, []byte(""), 0o644)
	if err != nil {
		app.logger.Error().Err(err).Msg("failed to write maintenance file")
	}
}

func (app *App) doesMaintenanceFileExist() bool {
	_, err := os.Stat(app.config.Maintenancefile)
	return err == nil
}

func (app *App) removeMaintenanceFile() {
	err := os.Remove(app.config.Maintenancefile)
	if err != nil && !os.IsNotExist(err) {
		app.logger.Error().Err(err).Msg("failed to remove maintenance file")
	}
}
