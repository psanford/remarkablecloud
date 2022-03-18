package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "remarcli",
	Short: "ReMarkable cli",
}

func main() {
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(treeCommand())
	rootCmd.AddCommand(putCommand())
	rootCmd.AddCommand(putBlobCommand())
	rootCmd.AddCommand(setRootCommand())
	rootCmd.AddCommand(rmCommand())
	rootCmd.AddCommand(getBlobCommand())

	rootCmd.Execute()

}
