package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var (
	infoZone string
	infoLag  string
	infoHost string
)

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Print information from DCS",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliInfo(short, infoZone, infoLag, infoHost))
	},
}

func init() {
	infoCmd.Flags().StringVar(&infoZone, "zone", "", "show only hosts from the specified zone")
	infoCmd.Flags().StringVar(&infoLag, "lag", "", "filter health block by replication lag, e.g. >10 or <5")
	infoCmd.Flags().StringVar(&infoHost, "host", "", "show only hosts containing the specified substring")
	rootCmd.AddCommand(infoCmd)
}
