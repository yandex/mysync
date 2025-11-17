package app

// cliInitApp consolidates initialization logic for CLI commands to reduce boilerplate code.
// The returned cleanup function closes the dcs and cluster connections to prevent leaks.
func (app *App) cliInitApp() (func(), error) {
	err := app.connectDCS()
	if err != nil {
		return nil, err
	}

	app.dcs.Initialize()
	err = app.newDBCluster()
	if err != nil {
		app.dcs.Close()
		return nil, err
	}
	app.initializeOptimizationModule()
	err = app.cluster.UpdateHostsInfo()
	if err != nil {
		app.dcs.Close()
		app.cluster.Close()
		return nil, err
	}

	return func() {
		app.dcs.Close()
		app.cluster.Close()
	}, nil
}
