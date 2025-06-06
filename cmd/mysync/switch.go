package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var switchTo string
var switchFrom string
var switchWait time.Duration
var failover bool

var switchCmd = &cobra.Command{
	Use:   "switch",
	Short: "Move the master to (from) specified host",
	Long:  "If master is already on (not on) specified host it will be ignored",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliSwitch(switchFrom, switchTo, switchWait, failover))
	},
}

func init() {
	rootCmd.AddCommand(switchCmd)
	switchCmd.Flags().StringVar(&switchFrom, "from", "", "switch master from specific (or current master if empty) host")
	switchCmd.Flags().StringVar(&switchTo, "to", "", "switch master to specific (or most up-to-date if empty) host")
	switchCmd.Flags().DurationVarP(&switchWait, "wait", "w", 5*time.Minute, "how long wait for switchover to complete, 0s to return immediately")
	switchCmd.Flags().BoolVar(&failover, "failover", false, "ignore the master's liveness probe during switchover")
}
