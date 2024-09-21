package main

import (
	"log"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "remarcli",
	Short: "ReMarkable cli",
}

var debugLog = false

func main() {

	rootCmd.PersistentFlags().BoolVarP(&debugLog, "debug", "", false, "Log debug messages to stderr")

	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(treeCommand())
	rootCmd.AddCommand(putCommand())
	rootCmd.AddCommand(putBlobCommand())
	rootCmd.AddCommand(mkDirCommand())
	rootCmd.AddCommand(rmCommand())
	rootCmd.AddCommand(getBlobCommand())

	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if debugLog {
			remarkablecloud.DebugLogFunc = log.Printf
		}
	}

	rootCmd.Execute()

}
