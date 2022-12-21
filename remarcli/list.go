package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
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
	client, err := newClient()
	if err != nil {
		panic(err)
	}

	batch, err := client.NewBatch()
	if err != nil {
		log.Fatalf("new batch err: %s", err)
	}

	items, err := batch.List()
	if err != nil {
		log.Fatalf("List items err: %s", err)
	}

	for _, item := range items {
		fmt.Printf("%+v\n", item)
	}

	fmt.Println()

	// tree, err := client.FSSnapshot()
	// if err != nil {
	// 	log.Fatalf("FSSnapshot err: %s", err)
	// }

	// err = fs.WalkDir(tree, ".", func(path string, d fs.DirEntry, err error) error {
	// 	if err != nil {
	// 		return err
	// 	}
	// 	fmt.Printf("%s\n", path)
	// 	return nil
	// })
	// if err != nil {
	// 	log.Fatalf("WalkDir err: %s", err)
	// }
}

type refreshable interface {
	Refresh() error
}

type AuthTokens struct {
	DeviceToken string
	UserToken   string
}
