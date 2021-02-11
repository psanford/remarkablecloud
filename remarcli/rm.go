package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
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

	content, err := ioutil.ReadFile("/home/psanford/.rmapi")
	if err != nil {
		panic(err)
	}

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

	err = client.Remove(fileName)
	if err != nil {
		log.Fatalf("rm file err: %s", err)
	}

	fmt.Printf("rm success!\n")
}
