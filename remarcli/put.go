package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
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

	client, err := newClient()
	if err != nil {
		panic(err)
	}

	batcher, err := client.NewBatch()
	if err != nil {
		log.Fatalf("new batch err: %s", err)
	}

	putResult, err := batcher.Put(name, ext, f)
	if err != nil {
		log.Fatalf("put file err: %s", err)
	}

	result, err := batcher.Commit()
	if err != nil {
		log.Fatalf("put file batch commit err: %s", err)
	}

	fmt.Printf("put success! id=%s %+v\n", putResult.DocID, result)
}

func setRootCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "set-root <hash>",
		Short: "set root to specified hash",
		Run:   setRootAction,
	}
	return &cmd
}

func setRootAction(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Printf("hash is required")
		cmd.Usage()
		os.Exit(1)
	}

	client, err := newClient()
	if err != nil {
		panic(err)
	}

	hash := args[0]
	if len(hash) != 64 {
		log.Fatalf("expected hash to be 64 chars but was %d", len(hash))
	}

	err = client.PutBlob("root", bytes.NewReader([]byte(hash)))
	if err != nil {
		log.Fatalf("put blob err: %s", err)
	}

	fmt.Printf("putBlob success!\n")
}

func putBlobCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "put-blob",
		Short: "PutBlob file",
		Run:   putBlobAction,
	}
	return &cmd
}

func putBlobAction(cmd *cobra.Command, args []string) {
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

	client, err := newClient()
	if err != nil {
		panic(err)
	}

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("open file err: %s", err)
	}

	h := sha256.New()

	_, err = io.Copy(h, f)
	if err != nil {
		log.Fatalf("read file err: %s", err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalf("seek err: %s", err)
	}

	key := hex.EncodeToString(h.Sum(nil))

	err = client.PutBlob(key, f)
	if err != nil {
		log.Fatalf("PutBlob file err: %s", err)
	}

	fmt.Printf("putBlob success! id=%s\n", key)
}
