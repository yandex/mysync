package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var abortCmd = &cobra.Command{
	Use:   "abort",
	Short: "Clear switchover command from DCS",
	Long:  "It does NOT rollback performed actions. You should manually repair cluster after it.",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliAbort())
	},
}

func init() {
	rootCmd.AddCommand(abortCmd)
}
