package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "remarcli",
	Short: "ReMarkable cli",
}

func main() {
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(putCommand())
	rootCmd.AddCommand(rmCommand())

	rootCmd.Execute()

}
