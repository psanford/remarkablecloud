package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
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
	content, err := ioutil.ReadFile("/home/psanford/.rmapi")
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

	var tokens AuthTokens
	err = yaml.Unmarshal(content, &tokens)
	if err != nil {
		panic(err)
	}

	creds := remarkablecloud.NewStaticTokenProvider(tokens.DeviceToken, tokens.UserToken)

	err = creds.(refreshable).Refresh()
	if err != nil {
		panic(err)
	}

	client := remarkablecloud.New(creds)

	resp, err := client.GetBlob(id)
	if err != nil {
		log.Fatalf("GetBlob err: %s", err)
	}
	defer resp.Body.Close()

	io.Copy(os.Stdout, resp.Body)
}
