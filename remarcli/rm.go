package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

func rmCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "rm",
		Short: "Rm file",
		Run:   rmAction,
	}
	return &cmd
}

func rmAction(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Printf("filename argument is required")
		cmd.Usage()
		os.Exit(1)
	}

	fileName := args[0]

	client, err := newClient()
	if err != nil {
		panic(err)
	}

	batcher, err := client.NewBatch()
	if err != nil {
		log.Fatalf("new batch err: %s", err)
	}

	_, err = batcher.Remove(fileName)
	if err != nil {
		log.Fatalf("rm file err: %s", err)
	}

	result, err := batcher.Commit()
	if err != nil {
		log.Fatalf("batch commit err: %s", err)
	}

	fmt.Printf("rm success: %+v\n", result)
}
