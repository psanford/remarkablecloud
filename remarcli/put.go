package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/psanford/remarkablecloud"
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

var (
	fileUUIDFlag string
	fileExtFlag  string
)

func putBlobCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "put-blob",
		Short: "PutBlob file",
		Run:   putBlobAction,
	}

	cmd.Flags().StringVarP(&fileUUIDFlag, "uuid", "", "", "Set file uuid")
	cmd.Flags().StringVarP(&fileExtFlag, "ext", "", "", "Set file extension")

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

	uuid, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	uuidStr := uuid.String()
	if fileUUIDFlag != "" {
		uuidStr = fileUUIDFlag
	}

	if fileExtFlag != "" {
		uuidStr += "." + fileExtFlag
	} else if ext == "pdf" || ext == "epub" {
		uuidStr += "." + ext
	}

	req := remarkablecloud.RawPubBlobRequest{
		Key:      key,
		Filename: uuidStr,
		Content:  f,
	}
	err = client.RawPutBlob(req)
	if err != nil {
		log.Fatalf("PutBlob file err: %s", err)
	}

	fmt.Printf("putBlob success! id=%s\n", key)
}
