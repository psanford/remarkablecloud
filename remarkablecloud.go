package remarkablecloud

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
)

var (
	RootURL       = "https://internal.cloud.remarkable.com/sync/v3/root"
	FileURLPrefix = "https://internal.cloud.remarkable.com/sync/v3/files/"

	DebugLogFunc func(string, ...interface{})

	schemaVersion = "3"
)

func New(creds CredentialProvider) *Client {
	return &Client{
		creds: creds,
	}
}

func (c *Client) Do(op string, req *http.Request) (*http.Response, error) {
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpRequestOut(req, true)
		DebugLogFunc("%s req\n<%s>\n", op, debugReq)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if DebugLogFunc != nil {
		debugResp, err := httputil.DumpResponse(resp, true)
		if err != nil {
			log.Fatalf("DumpResponse err: %s", err)
		}
		DebugLogFunc("%s resp\n<%s>\n", op, debugResp)
	}

	return resp, err
}

func (c *Client) GetBlob(id string) (*http.Response, error) {
	req, err := http.NewRequest("GET", FileURLPrefix+id, nil)
	if err != nil {
		return nil, err
	}

	return c.Do("GetBlob", req)
}

type rawListResult struct {
	items        []Item
	blobMetadata []blobMetadata
	generation   int
	rootHash     string
}

func (c *Client) getRoot() (*RootMetadata, error) {
	req, err := http.NewRequest("GET", RootURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.Do("getRoot", req)
	if err != nil {
		return nil, err
	}

	rootTxt, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var root RootMetadata
	err = json.Unmarshal(rootTxt, &root)
	if err != nil {
		return nil, err
	}

	return &root, nil
}

func (c *Client) rawList() (*rawListResult, error) {
	root, err := c.getRoot()

	resp, err := c.GetBlob(root.Hash)
	if err != nil {
		return nil, err
	}

	entries, err := readBlobIndex(resp.Body)
	if err != nil {
		return nil, err
	}

	var (
		wg           sync.WaitGroup
		fetcherCount = 50
		fetchChan    = make(chan blobMetadata)
		itemChan     = make(chan Item)
		errChan      = make(chan error)
		doneChan     = make(chan struct{})
	)

	for i := 0; i < fetcherCount; i++ {
		wg.Add(1)
		go func() {
			for meta := range fetchChan {
				resp, err := c.GetBlob(meta.hash)
				if err != nil {
					errChan <- err
					continue
				}

				fileEntries, err := readBlobIndex(resp.Body)
				if err != nil {
					errChan <- err
					continue
				}

				for _, entry := range fileEntries {
					if strings.HasSuffix(entry.id, ".metadata") && !strings.ContainsRune(entry.id, '/') {
						resp, err = c.GetBlob(entry.hash)
						if err != nil {
							errChan <- fmt.Errorf("fetch item %s err: %w", entry.id, err)
							break
						}

						var item Item
						err = json.NewDecoder(resp.Body).Decode(&item)
						if err != nil {
							errChan <- fmt.Errorf("decode item %s metadata err: %w", entry.id, err)
							break
						}

						item.ID = meta.id
						item.Hash = meta.hash

						itemChan <- item
					}
				}

			}
			wg.Done()
		}()
	}

	go func() {
		for _, entry := range entries {
			fetchChan <- entry
		}
		close(fetchChan)

		wg.Wait()
		close(doneChan)
	}()

	var items []Item
	var errors []string
OUTER:
	for {
		select {
		case item := <-itemChan:
			items = append(items, item)
		case err := <-errChan:
			errors = append(errors, err.Error())

		case <-doneChan:
			break OUTER
		}
	}

	if len(errors) > 0 {
		errStr := strings.Join(errors, ";")
		return nil, fmt.Errorf("fetch errors: %s", errStr)
	}

	return &rawListResult{
		items:        items,
		blobMetadata: entries,
		generation:   int(root.Generation),
		rootHash:     root.Hash,
	}, nil
}

func (c *Client) NewBatch() (*Batch, error) {
	b := &Batch{
		blobs: make(map[string]batchBlob),
		c:     c,
	}

	_, err := b.loadTree()
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (c *Client) List() ([]Item, error) {
	raw, err := c.rawList()
	return raw.items, err
}

func readBlobIndex(r io.Reader) ([]blobMetadata, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return nil, fmt.Errorf("index metadata not found")
	}

	version := scanner.Text()
	if version != schemaVersion {
		return nil, fmt.Errorf("unsupported index metadata version: %s, expected %s", version, schemaVersion)
	}

	var entries []blobMetadata
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.FieldsFunc(line, func(r rune) bool { return r == ':' })

		count, _ := strconv.Atoi(fields[3])
		size, _ := strconv.Atoi(fields[4])

		entry := blobMetadata{
			hash:      fields[0],
			blobType:  fields[1],
			id:        fields[2],
			fileCount: count,
			size:      size,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

type putBlobOptions struct {
	generation    int
	currentHash   string
	isRootListing bool
	xGoogHash     string
}

type PutBlobOption func(opt *putBlobOptions)

type RawPubBlobRequest struct {
	// Sha256 of the content
	Key string
	// Remarkable filename (uuid.extension)
	Filename   string
	ParentHash string
	Content    io.ReadSeeker
}

func (c *Client) RawPutBlob(r RawPubBlobRequest, opts ...PutBlobOption) error {
	var options putBlobOptions
	for _, opt := range opts {
		opt(&options)
	}

	req, err := http.NewRequest("PUT", FileURLPrefix+r.Key, r.Content)
	if err != nil {
		return err
	}

	if r.Filename != "" {
		req.Header.Add("Rm-Filename", r.Filename)
	}
	if r.ParentHash != "" {
		req.Header.Add("Rm-Parent-Hash", r.ParentHash)
	}

	crc32cHash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	size, err := io.Copy(crc32cHash, r.Content)
	if err != nil {
		return err
	}

	sum := crc32cHash.Sum(nil)
	crc32cBase64 := base64.StdEncoding.EncodeToString(sum)
	req.Header.Set("X-Goog-Hash", fmt.Sprintf("crc32c=%s", crc32cBase64))
	req.ContentLength = size

	_, err = r.Content.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	resp, err := c.Do("PutBlob", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("put blob non-200 response: %d %s", resp.StatusCode, body)
	}

	return err
}

func (c *Client) PutRoot(root RootMetadata) (*RootMetadata, error) {
	reqJson, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PUT", RootURL, bytes.NewBuffer(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Rm-Filename", "roothash")
	req.ContentLength = int64(len(reqJson))

	resp, err := c.Do("PutRoot", req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out RootMetadata
	err = json.Unmarshal(body, &out)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

func hashKey(r io.ReadSeeker) (string, error) {
	h := sha256.New()

	_, err := io.Copy(h, r)
	if err != nil {
		return "", fmt.Errorf("read file err: %w", err)
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("seek err: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

type PutResult struct {
	OldRootHash string
	NewRootHash string
	DocID       string
}

func (c *Client) FSSnapshot() (fs.FS, error) {
	items, err := c.List()
	if err != nil {
		return nil, err
	}
	return c.fsSnapshotFromList(items, true)
}

const (
	collectionType = "CollectionType"
	documentType   = "DocumentType"
)

type Item struct {
	ID               string `json:"__pms_id"`
	Hash             string `json:"__pms_hash"`
	Deleted          bool   `json:"deleted"`
	LastModified     string `json:"lastModified"`
	LastOpened       string `json:"lastOpened"`
	LastOpenedPage   int    `json:"lastOpenedPage"`
	Metadatamodified bool   `json:"metadatamodified"`
	Modified         bool   `json:"modified"`
	Parent           string `json:"parent"`
	Pinned           bool   `json:"pinned"`
	Synced           bool   `json:"synced"`
	Type             string `json:"type"`
	Version          int    `json:"version"`
	Name             string `json:"visibleName"`
}

type metadataDoc struct {
	Deleted          bool   `json:"deleted"`
	LastModified     string `json:"lastModified"`
	LastOpened       string `json:"lastOpened"`
	LastOpenedPage   int64  `json:"lastOpenedPage"`
	MetadataModified bool   `json:"metadatamodified"`
	Modified         bool   `json:"modified"`
	Parent           string `json:"parent"`
	Pinned           bool   `json:"pinned"`
	Synced           bool   `json:"synced"`
	Type             string `json:"type"`
	Version          int64  `json:"version"`
	VisibleName      string `json:"visibleName"`
}

type contentDoc struct {
	DummyDocument  bool                    `json:"dummyDocument"`
	ExtraMetadata  contentDocExtraMetadata `json:"extraMetadata"`
	FileType       string                  `json:"fileType"`
	FontName       string                  `json:"fontName"`
	LastOpenedPage int                     `json:"lastOpenedPage"`
	LineHeight     int                     `json:"lineHeight"`
	Margins        int                     `json:"margins"`
	Orientation    string                  `json:"orientation"`
	PageCount      int                     `json:"pageCount"`
	Pages          []string                `json:"pages"`
	TextScale      int                     `json:"textScale"`
	Transform      contentDocTransform     `json:"transform"`
}

type contentDocExtraMetadata struct {
	LastBrushColor           string `json:"LastBrushColor"`
	LastBrushThicknessScale  string `json:"LastBrushThicknessScale"`
	LastColor                string `json:"LastColor"`
	LastEraserThicknessScale string `json:"LastEraserThicknessScale"`
	LastEraserTool           string `json:"LastEraserTool"`
	LastPen                  string `json:"LastPen"`
	LastPenColor             string `json:"LastPenColor"`
	LastPenThicknessScale    string `json:"LastPenThicknessScale"`
	LastPencil               string `json:"LastPencil"`
	LastPencilColor          string `json:"LastPencilColor"`
	LastPencilThicknessScale string `json:"LastPencilThicknessScale"`
	LastTool                 string `json:"LastTool"`
	ThicknessScale           string `json:"ThicknessScale"`
	LastFinelinerv2Size      string `json:"LastFinelinerv2Size"`
}

type contentDocTransform struct {
	M11 float32 `json:"m11"`
	M12 float32 `json:"m12"`
	M13 float32 `json:"m13"`
	M21 float32 `json:"m21"`
	M22 float32 `json:"m22"`
	M23 float32 `json:"m23"`
	M31 float32 `json:"m31"`
	M32 float32 `json:"m32"`
	M33 float32 `json:"m33"`
}

type blobMetadata struct {
	hash      string
	id        string
	blobType  string
	fileCount int
	size      int
}

type RootMetadata struct {
	Hash          string `json:"hash"`
	Generation    int64  `json:"generation"`
	SchemaVersion int64  `json:"schemaVersion,omitempty"`
	Broadcast     bool   `json:"broadcast,omitempty"`
}

func (m *blobMetadata) String() string {
	return fmt.Sprintf("%s:%s:%s:%d:%d", m.hash, m.blobType, m.id, m.fileCount, m.size)
}
