package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var maintWait time.Duration
var maintReason string
var mode app.MaintenanceMode
var maintLight bool

var maintCmd = &cobra.Command{
	Use:     "maintenance",
	Aliases: []string{"maint", "mnt"},
	Short:   "Enables or disables maintenance mode",
	Long: ("When maintenance is enabled, MySync manager will not perform any actions.\n" +
		"Light maintenance mode keeps MySync running, but suppresses automatic failover and switchover.\n" +
		"When maintenance is disabled, MySync will analyze cluster state and remember it as correct."),
}

var maintOnCmd = &cobra.Command{
	Use:     "on",
	Aliases: []string{"enable"},
	Short:   "Enable maintenance mode",
	Long: "Enable maintenance mode.\n\n" +
		"By default this enables normal maintenance mode, which pauses MySync manager actions.\n" +
		"Use --light to enable light maintenance mode, which keeps MySync running but blocks automatic failover and switchover.",
	Run: func(cmd *cobra.Command, args []string) {
		if maintLight {
			mode = app.LightMode
		} else {
			mode = app.FullMode
		}
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliEnableMaintenance(maintWait, maintReason, mode))
	},
}

var maintOffCmd = &cobra.Command{
	Use:     "off",
	Aliases: []string{"disable"},
	Short:   "Disable maintenance mode",
	Long:    "Disable maintenance mode and wait until MySync resumes normal cluster management.",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliDisableMaintenance(maintWait))
	},
}

var maintGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Show whether maintenance mode is enabled",
	Long:  "Print current maintenance mode state from DCS.",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliGetMaintenance())
	},
}

func init() {
	rootCmd.AddCommand(maintCmd)
	maintCmd.AddCommand(maintOnCmd)
	maintCmd.AddCommand(maintOffCmd)
	maintCmd.AddCommand(maintGetCmd)
	maintCmd.PersistentFlags().DurationVarP(&maintWait, "wait", "w", 30*time.Second, "how long to wait for maintenance activation, 0s to return immediately")

	maintOnCmd.Flags().StringVarP(&maintReason, "reason", "r", "", "reason for maintenance (e.g. ticket number)")
	maintOnCmd.Flags().BoolVar(&maintLight, "light", false, "enable light maintenance mode: keeps mysync running but suppresses automatic failover and switchover")
}
