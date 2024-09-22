package main

import (
	"io"
	"io/fs"
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

func getDocCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "get <cloud_path> <local_dst>",
		Short: "Get document by path",
		Run:   getDocAction,
	}

	return &cmd
}

func getDocAction(cmd *cobra.Command, args []string) {
	client, err := newClient()
	if err != nil {
		panic(err)
	}

	if len(args) < 2 {
		log.Fatalf("usage: get <cloud_path> <local_dst>")
	}

	dstPath := args[1]
	f, err := os.Create(dstPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	rootFS, err := client.FSSnapshot()
	if err != nil {
		log.Fatalf("get fs snapshot err: %s", err)
	}

	cloudPath := args[0]

	content, err := fs.ReadFile(rootFS, cloudPath)
	if err != nil {
		log.Fatalf("read file err: %s", err)
	}

	_, err = f.Write(content)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Wrote: %s", dstPath)
}
