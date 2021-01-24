package main

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"

	"github.com/psanford/remarkablecloud"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func listCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list",
		Short: "List FS",
		Run:   listAction,
	}
	return &cmd
}

func listAction(cmd *cobra.Command, args []string) {
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

	items, err := client.List()
	if err != nil {
		log.Fatalf("List items err: %s", err)
	}

	for _, item := range items {
		fmt.Printf("%+v\n", item)
	}

	fmt.Println()

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

type refreshable interface {
	Refresh() error
}

type AuthTokens struct {
	DeviceToken string
	UserToken   string
}
