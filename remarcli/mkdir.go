package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
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
	content, err := ioutil.ReadFile("/home/psanford/.rmapi")
	if err != nil {
		panic(err)
	}

	if len(args) < 1 {
		log.Fatalf("usage: mkdir <path>")
	}

	path := args[0]

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

	result, err := client.Mkdir(path)
	if err != nil {
		log.Fatalf("mkdir err: %s", err)
	}

	fmt.Printf("mkdir success: %+v\n", result)
}
