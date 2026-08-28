package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var safeAbortCmd = &cobra.Command{
	Use:   "safe-abort",
	Short: "Abort a switchover only before the safe-abort boundary",
	Long: "Atomically removes the current switchover only while it is marked abortable. " +
		"It never force-aborts a switchover that has started changing the replication topology.",
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
