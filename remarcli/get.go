package main

import (
	"io"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func getBlobCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "get-blob <id>",
		Short: "Get blob by id",
		Run:   getBlobAction,
	}

	return &cmd
}

func getBlobAction(cmd *cobra.Command, args []string) {
	client, err := newClient()
	if err != nil {
		panic(err)
	}

	if len(args) < 1 {
		log.Fatalf("usage: get-blob <id>")
	}

	id := args[0]

	// allow pasting a full entry here for convenience
	fields := strings.Split(id, ":")
	id = fields[0]

	resp, err := client.GetBlob(id)
	if err != nil {
		log.Fatalf("GetBlob err: %s", err)
	}
	defer resp.Body.Close()

	io.Copy(os.Stdout, resp.Body)
}
