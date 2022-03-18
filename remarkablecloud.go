package remarkablecloud

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/psanford/memfs"
)

var (
	APIHost      = "https://document-storage-production-dot-remarkable-production.appspot.com"
	ListAPI      = "/document-storage/json/2/docs"
	UpdateAPI    = "/document-storage/json/2/upload/update-status"
	DeleteAPI    = "/document-storage/json/2/delete"
	UploadReqAPI = "/document-storage/json/2/upload/request"

	DownloadURL = "https://rm-blob-storage-prod.appspot.com/api/v1/signed-urls/downloads"
	UploadURL   = "https://rm-blob-storage-prod.appspot.com/api/v1/signed-urls/uploads"

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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data storageResp
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("Invalid response json: %q, %s", err, body)
	}

	return http.Get(data.URL)
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

	rootBlobID, err := ioutil.ReadAll(resp.Body)
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
	generation    string
	isRootListing bool
}

type PutBlobOption func(opt *putBlobOptions)

func WithGeneration(gen int) PutBlobOption {
	return func(opt *putBlobOptions) {
		opt.generation = strconv.Itoa(gen)
	}
}

func (c *Client) PutBlob(key string, r io.Reader, opts ...PutBlobOption) error {
	var options putBlobOptions
	for _, opt := range opts {
		opt(&options)
	}
	putReq := storageRequest{
		Method:       "PUT",
		RelativePath: key,
		Generation:   options.generation,
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var data storageResp
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("Invalid response json: %q, %s", err, body)
	}

	req, err = http.NewRequest("PUT", data.URL, r)
	if err != nil {
		return fmt.Errorf("PUT req err: %w", err)
	}

	if options.generation != "" {
		req.Header.Add("x-goog-if-generation-match", options.generation)
	}

	_, err = http.DefaultClient.Do(req)
	return err
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

func (c *Client) putBlobWithHashedContent(id string, r io.ReadSeeker) (*blobMetadata, error) {
	key, err := hashKey(r)
	if err != nil {
		return nil, err
	}

	err = c.PutBlob(key, r)
	if err != nil {
		return nil, err
	}

	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	meta := blobMetadata{
		hash:      key,
		id:        id,
		blobType:  "0",
		fileCount: 0,
		size:      int(size),
	}

	return &meta, nil
}

func (c *Client) putContentDoc(id, ext string) (*blobMetadata, error) {
	cdoc := contentDoc{
		DummyDocument: false,
		ExtraMetadata: contentDocExtraMetadata{
			LastPen:             "Finelinerv2",
			LastTool:            "Finelinerv2",
			LastFinelinerv2Size: "1",
		},
		FileType:   ext,
		LineHeight: -1,
		Margins:    180,
		TextScale:  1,
		Transform: contentDocTransform{
			M11: 1,
			M22: 1,
			M33: 1,
		},
	}
	cdocJson, err := json.Marshal(cdoc)
	if err != nil {
		return nil, err
	}

	cdocID := fmt.Sprintf("%s.content", id)
	return c.putBlobWithHashedContent(cdocID, bytes.NewReader(cdocJson))
}

func (c *Client) putEmptyContentDoc(id string) (*blobMetadata, error) {
	cdocJson := []byte("{}")

	cdocID := fmt.Sprintf("%s.content", id)
	return c.putBlobWithHashedContent(cdocID, bytes.NewReader(cdocJson))
}

func (c *Client) putMetaDoc(parentID, id, fileName string) (*blobMetadata, error) {
	item := metadataDoc{
		Parent:       parentID,
		Synced:       true,
		VisibleName:  fileName,
		LastModified: strconv.Itoa(int(time.Now().UnixNano())),
		Type:         documentType,
	}

	itemJson, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	itemID := fmt.Sprintf("%s.metadata", id)
	return c.putBlobWithHashedContent(itemID, bytes.NewReader(itemJson))
}

func (c *Client) putMetaDirDoc(parentID, id, fileName string) (*blobMetadata, error) {
	item := metadataDoc{
		Parent:       parentID,
		Synced:       true,
		VisibleName:  fileName,
		LastModified: strconv.Itoa(int(time.Now().UnixNano())),
		Type:         collectionType,
	}

	itemJson, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	itemID := fmt.Sprintf("%s.metadata", id)
	return c.putBlobWithHashedContent(itemID, bytes.NewReader(itemJson))
}

func (c *Client) putEmptyPageData(id string) (*blobMetadata, error) {
	cdocID := fmt.Sprintf("%s.pagedata", id)
	data := make([]byte, 0)
	return c.putBlobWithHashedContent(cdocID, bytes.NewReader(data))
}

func (c *Client) putListing(id string, blobs []blobMetadata) (*blobMetadata, error) {
	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].id < blobs[j].id
	})

	var buf bytes.Buffer

	h := sha256.New()

	fmt.Fprintf(&buf, "%s\n", schemaVersion)
	for _, blob := range blobs {
		fmt.Fprintf(&buf, "%s\n", blob.String())

		hashBytes, err := hex.DecodeString(blob.hash)
		if err != nil {
			return nil, fmt.Errorf("invalid hash for %s:%s", blob.hash, blob.id)
		}
		h.Write(hashBytes)
	}

	key := hex.EncodeToString(h.Sum(nil))

	r := bytes.NewReader(buf.Bytes())

	err := c.PutBlob(key, r)
	if err != nil {
		return nil, err
	}

	ret := blobMetadata{
		hash:      key,
		id:        id,
		blobType:  "80000000",
		fileCount: len(blobs),
		size:      0,
	}

	return &ret, nil
}

type PutResult struct {
	OldRootHash string
	NewRootHash string
	DocID       string
}

func (c *Client) Put(p string, ext string, r io.ReadSeeker) (*PutResult, error) {
	raw, err := c.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := c.fsSnapshotFromList(items)
	if err != nil {
		return nil, err
	}

	parent, fileName := filepath.Split(p)

	var parentItem Item
	if parent == "" {
		parentItem.ID = ""
	} else {
		parent = strings.TrimSuffix(parent, "/")
		parentStat, err := fs.Stat(tree, parent)
		if err != nil {
			return nil, fmt.Errorf("parent dir error: %w", err)
		}
		if !parentStat.IsDir() {
			return nil, fmt.Errorf("parent is not directory")
		}
		rawMeta, err := fs.ReadFile(tree, parent+"/.meta")
		if err != nil {
			return nil, fmt.Errorf("read parent dir metadata err: %w", err)
		}
		err = json.Unmarshal(rawMeta, &parentItem)
		if err != nil {
			return nil, fmt.Errorf("parse parent dir metadata err: %w", err)
		}
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	id := uuid.String()

	if ext != "epub" && ext != "pdf" {
		return nil, errors.New("unsupported ext")
	}

	// put:
	// .content  contentDoc
	// .{pdf,epub}
	// .metadata metadataDocument
	// .pagedata (empty)

	files := make([]blobMetadata, 0)
	orig, err := c.putBlobWithHashedContent(id+"."+ext, r)
	if err != nil {
		return nil, fmt.Errorf("put raw doc err: %w", err)
	}
	files = append(files, *orig)

	cmeta, err := c.putContentDoc(id, ext)
	if err != nil {
		return nil, fmt.Errorf("put contentDoc err: %w", err)
	}
	files = append(files, *cmeta)

	meta, err := c.putMetaDoc(parentItem.ID, id, fileName)
	if err != nil {
		return nil, fmt.Errorf("put .metadata err: %w", err)
	}
	files = append(files, *meta)

	pd, err := c.putEmptyPageData(id)
	if err != nil {
		return nil, fmt.Errorf("put .pagedata err: %w", err)
	}
	files = append(files, *pd)

	listingMeta, err := c.putListing(id, files)
	if err != nil {
		return nil, fmt.Errorf("put listing err: %w", err)
	}

	rawEntries := raw.blobMetadata

	rawEntries = append(rawEntries, *listingMeta)

	rootMata, err := c.putListing("", rawEntries)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	err = c.PutBlob("root", bytes.NewReader([]byte(rootMata.hash)), WithGeneration(raw.generation))

	if err != nil {
		return nil, fmt.Errorf("put root ptr err: %w", err)
	}

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMata.hash,
		DocID:       id,
	}

	return &result, nil
}

// Mkdir creates a directory synced to the device.
// On success, Mkdir returns the directory ID.
func (c *Client) Mkdir(p string) (*PutResult, error) {
	raw, err := c.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := c.fsSnapshotFromList(items)
	if err != nil {
		return nil, err
	}

	parent, dirName := filepath.Split(p)
	if parent != "" {
		parentStat, err := fs.Stat(tree, parent)
		if err != nil {
			return nil, fmt.Errorf("parent dir error: %w", err)
		}
		if !parentStat.IsDir() {
			return nil, fmt.Errorf("parent is not directory")
		}
	}

	var parentItem Item
	if parent == "" {
		parentItem.ID = ""
	} else {
		rawMeta, err := fs.ReadFile(tree, parent+"/.meta")
		if err != nil {
			return nil, fmt.Errorf("read parent dir metadata err: %w", err)
		}
		err = json.Unmarshal(rawMeta, &parentItem)
		if err != nil {
			return nil, fmt.Errorf("parse parent dir metadata err: %w", err)
		}
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	id := uuid.String()

	files := make([]blobMetadata, 0)

	cmeta, err := c.putEmptyContentDoc(id)
	if err != nil {
		return nil, fmt.Errorf("put contentEmptyDoc err: %w", err)
	}
	files = append(files, *cmeta)

	meta, err := c.putMetaDirDoc(parentItem.ID, id, dirName)
	if err != nil {
		return nil, fmt.Errorf("put .metadata err: %w", err)
	}
	files = append(files, *meta)

	listingMeta, err := c.putListing(id, files)
	if err != nil {
		return nil, fmt.Errorf("put listing err: %w", err)
	}

	rawEntries := raw.blobMetadata

	rawEntries = append(rawEntries, *listingMeta)

	rootMata, err := c.putListing("", rawEntries)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	err = c.PutBlob("root", bytes.NewReader([]byte(rootMata.hash)), WithGeneration(raw.generation))

	if err != nil {
		return nil, fmt.Errorf("put root ptr err: %w", err)
	}

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMata.hash,
		DocID:       id,
	}

	return &result, nil
}

func (c *Client) Remove(name string) (*PutResult, error) {
	raw, err := c.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := c.fsSnapshotFromList(items)
	if err != nil {
		return nil, err
	}

	rawMeta, err := fs.ReadFile(tree, name)
	if err != nil {
		return nil, err
	}

	var item Item
	err = json.Unmarshal(rawMeta, &item)
	if err != nil {
		return nil, err
	}

	fmt.Printf("item: %+v\n", item)

	index := raw.blobMetadata

	matchingIdx := -1
	for i, meta := range index {
		if meta.hash == item.Hash {
			matchingIdx = i
			break
		}
	}

	if matchingIdx < 0 {
		return nil, fmt.Errorf("failed to find hash in root index: %s", item.Hash)
	}

	index = append(index[:matchingIdx], index[matchingIdx+1:]...)

	rootMata, err := c.putListing("", index)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	err = c.PutBlob("root", bytes.NewReader([]byte(rootMata.hash)), WithGeneration(raw.generation))

	if err != nil {
		return nil, fmt.Errorf("put root ptr err: %w", err)
	}

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMata.hash,
		DocID:       item.ID,
	}

	return &result, nil
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
	Expires      string `json:"expires"`
	Method       string `json:"method"`
	RelativePath string `json:"relative_path"`
	URL          string `json:"url"`
}

type storageRequest struct {
	RelativePath string `json:"relative_path"`
	Method       string `json:"http_method"`
	Generation   string `json:"generation,omitempty"`
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
