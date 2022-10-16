package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var stateCmd = &cobra.Command{
	Use:   "state",
	Short: "Print cluster nodes state by querying databases",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliState(short))
	},
}

func init() {
	rootCmd.AddCommand(stateCmd)
}
