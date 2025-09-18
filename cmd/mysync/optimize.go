package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var force bool

var optimizeCmd = &cobra.Command{
	Use:     "optimize",
	Aliases: []string{"turbo"},
	Short:   "Enables or disables optimization mode",
	Long: ("When optimization mode is enabled, MySync turns on potentially dangerous options to reduce disk usage.\n" +
		"When optimization mode is disabled, MySync restores safe defaults, and the host operates in normal mode.\n" +
		"Optimization works only on replica hosts and cannot be enabled on the master host."),
}

var optimizeOnCmd = &cobra.Command{
	Use:     "on",
	Aliases: []string{"enable"},
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliEnableOptimization(force))
	},
}

var optimizeOffCmd = &cobra.Command{
	Use:     "off",
	Aliases: []string{"disable"},
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliDisableOptimization(force))
	},
}

var optimizeOffAllCmd = &cobra.Command{
	Use:     "off-all",
	Aliases: []string{"disable-all"},
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliDisableAllOptimization(force))
	},
}

var optimizeGetCmd = &cobra.Command{
	Use: "get",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliGetOptimization())
	},
}

func init() {
	rootCmd.AddCommand(optimizeCmd)

	optimizeCmd.AddCommand(optimizeOnCmd)
	optimizeOnCmd.PersistentFlags().BoolVarP(&force, "force", "f", false, "force optimization process")

	optimizeCmd.AddCommand(optimizeOffCmd)
	optimizeCmd.AddCommand(optimizeGetCmd)
	optimizeCmd.AddCommand(optimizeOffAllCmd)
}
