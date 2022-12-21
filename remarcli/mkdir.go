package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

func mkDirCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "mkdir <path>",
		Short: "Make directory",
		Run:   mkDirAction,
	}

	return &cmd
}

func mkDirAction(cmd *cobra.Command, args []string) {
	client, err := newClient()
	if err != nil {
		panic(err)
	}

	if len(args) < 1 {
		log.Fatalf("usage: mkdir <path>")
	}

	path := args[0]

	batcher, err := client.NewBatch()
	if err != nil {
		log.Fatalf("new batch err: %s", err)
	}

	_, err = batcher.Mkdir(path)
	if err != nil {
		log.Fatalf("mkdir err: %s", err)
	}

	result, err := batcher.Commit()
	if err != nil {
		log.Fatalf("batch commit err: %s", err)
	}

	fmt.Printf("mkdir success: %+v\n", result)
}
