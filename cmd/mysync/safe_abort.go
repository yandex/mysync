package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var safeAbortCmd = &cobra.Command{
	Use:   "safe-abort",
	Short: "Request abort before the switchover safe-abort boundary",
	Long: "Atomically marks the current switchover for abort only while it is still abortable. " +
		"The current manager records the rejection and performs normal cleanup.",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		code := app.CliSafeAbort()
		app.CloseLogger()
		os.Exit(code)
	},
}

func init() {
	rootCmd.AddCommand(safeAbortCmd)
}
