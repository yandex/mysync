package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yandex/mysync/internal/app"
)

var configFile string
var logLevel string
var short bool

var rootCmd = &cobra.Command{
	Use:   "mysync",
	Short: "Mysync is MySQL HA cluster coordination tool",
	Long:  `Running without additional arguments will start mysync agent for current node.`,
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, false)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.Run())
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "/etc/mysync.yaml", "config file")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "loglevel", "l", "Warn", "logging level (Trace|Debug|Info|Warn|Error|Fatal)")
	rootCmd.PersistentFlags().BoolVarP(&short, "short", "s", false, "short output")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
