package remarkablecloud

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/psanford/memfs"
)

var (
	APIHost      = "https://document-storage-production-dot-remarkable-production.appspot.com"
	ListAPI      = "/document-storage/json/2/docs"
	UpdateAPI    = "/document-storage/json/2/upload/update-status"
	DeleteAPI    = "/document-storage/json/2/delete"
	UploadReqAPI = "/document-storage/json/2/upload/request"

	DownloadURL     = "https://internal.cloud.remarkable.com/sync/v2/signed-urls/downloads"
	UploadURL       = "https://internal.cloud.remarkable.com/sync/v2/signed-urls/uploads"
	SyncCompleteURL = "https://internal.cloud.remarkable.com/sync/v2/sync-complete"

	DebugLogFunc func(string, ...interface{})

	schemaVersion = "3"
)

func New(creds CredentialProvider) *Client {
	return &Client{
		creds: creds,
	}
}

func (c *Client) GetBlob(id string) (*http.Response, error) {
	var metaReq = storageRequest{
		Method:       "GET",
		RelativePath: id,
	}
	reqJson, _ := json.Marshal(metaReq)
	req, err := http.NewRequest("POST", DownloadURL, bytes.NewReader(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())

	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpRequest(req, true)
		DebugLogFunc("GetBlogMeta req <%s>", debugReq)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if DebugLogFunc != nil {
		debugResp, _ := httputil.DumpResponse(resp, true)
		DebugLogFunc("GetBlob resp: %s", debugResp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data storageResp
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("Invalid response json: %q, %s", err, body)
	}

	resp, err = http.Get(data.URL)
	if DebugLogFunc != nil {
		debugResp, _ := httputil.DumpResponse(resp, true)
		DebugLogFunc("GetBlob Data %s resp: %s", data.URL, debugResp)
	}
	return resp, err
}

type rawListResult struct {
	items        []Item
	blobMetadata []blobMetadata
	generation   int
	rootHash     string
}

func (c *Client) rawList() (*rawListResult, error) {
	resp, err := c.GetBlob("root")
	if err != nil {
		return nil, err
	}

	genStr := resp.Header.Get("x-goog-generation")
	generation, _ := strconv.Atoi(genStr)

	rootBlobID, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	resp, err = c.GetBlob(string(rootBlobID))
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
		generation:   generation,
		rootHash:     string(rootBlobID),
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
	generation        int
	currentHash       string
	captureGeneration *int
	isRootListing     bool
}

type PutBlobOption func(opt *putBlobOptions)

func (c *Client) PutBlob(key string, r io.Reader, opts ...PutBlobOption) error {
	var options putBlobOptions
	for _, opt := range opts {
		opt(&options)
	}
	putReq := storageRequest{
		Method:       "PUT",
		RelativePath: key,
		Generation:   options.generation,
		RootSchema:   options.currentHash,
	}

	putReqJson, err := json.Marshal(putReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", UploadURL, bytes.NewBuffer(putReqJson))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())

	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpRequest(req, true)
		DebugLogFunc("PutBlobMeta <%s>", debugReq)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if DebugLogFunc != nil {
		debugResp, _ := httputil.DumpResponse(resp, true)
		DebugLogFunc("PutBlobMeta Result <%s>", debugResp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var data storageResp
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("Invalid response json: %q, %s", err, body)
	}

	if data.Error != "" {
		return fmt.Errorf("Put err for req=%q, err=%w", putReqJson, err)
	}

	req, err = http.NewRequest("PUT", data.URL, r)
	if err != nil {
		return fmt.Errorf("PUT req err: %w, body=%s", err, body)
	}

	if options.generation != 0 {
		req.Header.Add("x-goog-if-generation-match", strconv.Itoa(options.generation))
	}

	req.Header.Add("x-goog-content-length-range", fmt.Sprintf("0,%d", data.MaxuploadsizeBytes))

	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpRequest(req, true)
		DebugLogFunc("PutBlobData <%s>", debugReq)
	}

	resp, err = http.DefaultClient.Do(req)

	if DebugLogFunc != nil {
		debugResp, _ := httputil.DumpResponse(resp, true)
		DebugLogFunc("PutBlobData Result <%s>", debugResp)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Non-200 error: %d body: %s", resp.StatusCode, body)
	}

	if options.captureGeneration != nil {
		genStr := resp.Header.Get("x-goog-generation")
		generation, _ := strconv.Atoi(genStr)
		*options.captureGeneration = generation
	}

	return err
}

func (c *Client) SyncRoot(generationID int) error {
	syncCompleteReq := syncCompleteRequest{
		Generation: generationID,
	}

	reqJson, err := json.Marshal(syncCompleteReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", SyncCompleteURL, bytes.NewBuffer(reqJson))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())

	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpRequest(req, true)
		DebugLogFunc("SyncRoot Req <%s>", debugReq)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if DebugLogFunc != nil {
		debugReq, _ := httputil.DumpResponse(resp, true)
		DebugLogFunc("SyncRoot Resp <%s>", debugReq)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if string(body) != "OK" {
		return fmt.Errorf("SyncRoot unexpected response=%q", body)
	}
	return nil
}

func WithGeneration(gen int) PutBlobOption {
	return func(opt *putBlobOptions) {
		opt.generation = gen
	}
}

func WithCaptureGeneration(gen *int) PutBlobOption {
	return func(opt *putBlobOptions) {
		opt.captureGeneration = gen
	}
}

func WithCurrentHash(currentHash string) PutBlobOption {
	return func(opt *putBlobOptions) {
		opt.currentHash = currentHash
	}
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

func (c *Client) fsSnapshotFromList(items []Item) (fs.FS, error) {
	seen := make(map[string]Item)
	paths := make(map[string]string)
	rootFS := memfs.New()

	prevSeen := len(seen)
	for len(seen) < len(items) {
		for _, item := range items {
			if _, exists := seen[item.ID]; exists {
				continue
			}
			if item.Parent == "trash" {
				seen[item.ID] = item
				continue
			}
			if _, parentExists := seen[item.Parent]; parentExists || item.Parent == "" {
				pp := item.Name
				if item.Parent != "" {
					pp = path.Join(paths[item.Parent], item.Name)
				}

				paths[item.ID] = pp

				if item.Type == collectionType {
					rootFS.MkdirAll(pp, 0777)
					data, _ := json.Marshal(item)
					err := rootFS.WriteFile(path.Join(pp, ".meta"), data, 0777)
					if err != nil {
						panic(err)
					}
				} else {
					data, _ := json.Marshal(item)
					err := rootFS.WriteFile(pp, data, 0777)
					if err != nil {
						panic(err)
					}
				}

				seen[item.ID] = item
			}
		}

		if len(seen) < len(items) && prevSeen == len(seen) {
			return nil, fmt.Errorf("failed to make progress building fs tree %d %d %d", prevSeen, len(seen), len(items))

		}
		prevSeen = len(seen)
	}

	return rootFS, nil
}

func (c *Client) FSSnapshot() (fs.FS, error) {
	items, err := c.List()
	if err != nil {
		return nil, err
	}
	return c.fsSnapshotFromList(items)
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

type storageResp struct {
	Expires            string `json:"expires"`
	Method             string `json:"method"`
	RelativePath       string `json:"relative_path"`
	URL                string `json:"url"`
	Error              string `json:"error"`
	MaxuploadsizeBytes int    `json:"maxuploadsize_bytes"`
}

type storageRequest struct {
	RelativePath string `json:"relative_path"`
	Method       string `json:"http_method"`
	Generation   int    `json:"generation,omitempty"`
	RootSchema   string `json:"root_schema,omitempty"`
}

type syncCompleteRequest struct {
	Generation int `json:"generation"`
}

type syncCompleteResponse struct {
	Message    string `json:"message"`
	StatusCode int    `json:"code"`
}

type blobMetadata struct {
	hash      string
	id        string
	blobType  string
	fileCount int
	size      int
}

func (m *blobMetadata) String() string {
	return fmt.Sprintf("%s:%s:%s:%d:%d", m.hash, m.blobType, m.id, m.fileCount, m.size)
}
