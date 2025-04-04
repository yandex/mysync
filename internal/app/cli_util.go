package app

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

	err = app.newDBCluster()
	if err != nil {
		app.dcs.Close()
		return nil, err
	}

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
