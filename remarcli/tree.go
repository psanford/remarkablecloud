package main

import (
	"fmt"
	"io/fs"
	"log"

	"github.com/spf13/cobra"
)

func treeCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "tree",
		Short: "Print fs tree",
		Run:   treeAction,
	}
	return &cmd
}

func treeAction(cmd *cobra.Command, args []string) {
	client, err := newClient()
	if err != nil {
		panic(err)
	}

	tree, err := client.FSSnapshot()
	if err != nil {
		log.Fatalf("FSSnapshot err: %s", err)
	}

	err = fs.WalkDir(tree, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", path)
		return nil
	})
	if err != nil {
		log.Fatalf("WalkDir err: %s", err)
	}
}
