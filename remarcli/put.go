package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func putCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "put",
		Short: "Put file",
		Run:   putAction,
	}
	return &cmd
}

func putAction(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Printf("filename argument is required")
		cmd.Usage()
		os.Exit(1)
	}

	fileName := args[0]

	_, name := filepath.Split(fileName)
	ext := filepath.Ext(name)
	name = strings.TrimSuffix(name, ext)
	ext = strings.TrimPrefix(ext, ".")

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("open file err: %s", err)
	}

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

	id, err := client.Put(name, ext, f)
	if err != nil {
		log.Fatalf("put file err: %s", err)
	}

	fmt.Printf("put success! id=%s\n", id)
}
