package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/yandex/mysync/internal/app"
)

var streamFrom string
var priority int64
var dryRun bool
var skipMySQLCheck bool

var hostCmd = &cobra.Command{
	Use:     "host",
	Aliases: []string{"hosts"},
	Short:   "manage hosts in cluster",
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliHostList())
	},
}

var hostAddCmd = &cobra.Command{
	Use:   "add",
	Short: "add host to cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var priorityVal *int64
		var streamFromVar *string
		cmd.Flags().Visit(func(f *pflag.Flag) {
			switch f.Name {
			case "priority":
				priorityVal = &priority
			case "stream-from":
				streamFromVar = &streamFrom
			}
		})

		os.Exit(app.CliHostAdd(args[0], streamFromVar, priorityVal, dryRun, skipMySQLCheck))
	},
}

var hostRemoveCmd = &cobra.Command{
	Use:   "remove",
	Short: "remove host from cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		app, err := app.NewApp(configFile, logLevel, true)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(app.CliHostRemove(args[0]))
	},
}

func init() {
	hostAddCmd.Flags().StringVar(&streamFrom, "stream-from", "", "host to stream from")
	hostAddCmd.Flags().Int64Var(&priority, "priority", 0, "host priority")
	hostAddCmd.Flags().BoolVar(&dryRun, "dry-run", false, "tests suggested changes."+
		" Exits codes:"+
		" 0 - when no changes detected,"+
		" 1 - when some error happened or changes prohibited,"+
		" 2 - when changes detected and some changes will be performed during usual run")
	hostAddCmd.Flags().BoolVar(&skipMySQLCheck, "skip-mysql-check", false, "skip mysql availability check")
	hostCmd.AddCommand(hostAddCmd)
	hostCmd.AddCommand(hostRemoveCmd)
	rootCmd.AddCommand(hostCmd)
}
