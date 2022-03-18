package remarkablecloud

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
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

func (c *Client) List() ([]Item, error) {
	resp, err := c.GetBlob("root")
	if err != nil {
		return nil, err
	}

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

	return items, nil
}

func readBlobIndex(r io.Reader) ([]blobMetadata, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return nil, fmt.Errorf("index metadata not found")
	}

	version := scanner.Text()
	if version != "3" {
		return nil, fmt.Errorf("unsupported index metadata version: %s, expected 3", version)
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

func (c *Client) Put(p string, ext string, r io.Reader) (string, error) {
	tree, err := c.FSSnapshot()
	if err != nil {
		return "", err
	}

	parent, fileName := filepath.Split(p)

	var parentItem Item
	if parent == "" {
		parentItem.ID = ""
	} else {
		parent = strings.TrimSuffix(parent, "/")
		parentStat, err := fs.Stat(tree, parent)
		if err != nil {
			return "", fmt.Errorf("parent dir error: %w", err)
		}
		if !parentStat.IsDir() {
			return "", fmt.Errorf("parent is not directory")
		}
		rawMeta, err := fs.ReadFile(tree, parent+"/.meta")
		if err != nil {
			return "", fmt.Errorf("read parent dir metadata err: %w", err)
		}
		err = json.Unmarshal(rawMeta, &parentItem)
		if err != nil {
			return "", fmt.Errorf("parse parent dir metadata err: %w", err)
		}
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	if ext != "epub" && ext != "pdf" {
		return "", errors.New("unsupported ext")
	}

	doc := uploadDocumentRequest{
		ID:      id.String(),
		Type:    "DocumentType",
		Version: 1,
	}

	docJSON, err := json.Marshal([]uploadDocumentRequest{doc})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("PUT", APIHost+UploadReqAPI, bytes.NewBuffer(docJSON))
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var uploadResp []uploadDocumentResponse
	err = json.Unmarshal(body, &uploadResp)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	zipW := zip.NewWriter(&b)

	fData, err := zipW.Create(fmt.Sprintf("%s.%s", id.String(), ext))
	if err != nil {
		return "", err
	}
	_, err = io.Copy(fData, r)
	if err != nil {
		return "", fmt.Errorf("create zip err: %w", err)
	}

	fMeta, err := zipW.Create(fmt.Sprintf("%s.content", id.String()))
	if err != nil {
		return "", err
	}

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
	json.NewEncoder(fMeta).Encode(cdoc)
	err = zipW.Close()
	if err != nil {
		return "", err
	}

	req, err = http.NewRequest("PUT", uploadResp[0].BlobURLPut, &b)
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	item := metadataDocument{
		Parent:         parentItem.ID,
		VissibleName:   fileName,
		Version:        1,
		ID:             uploadResp[0].ID,
		ModifiedClient: time.Now().Format(time.RFC3339Nano),

		Type: "DocumentType",
	}

	itemJSON, err := json.Marshal([]metadataDocument{item})
	if err != nil {
		return "", err
	}

	req, err = http.NewRequest("PUT", APIHost+UpdateAPI, bytes.NewBuffer(itemJSON))
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)

	return id.String(), nil
}

// Mkdir creates a directory synced to the device.
// On success, Mkdir returns the directory ID.
func (c *Client) Mkdir(p string) (string, error) {
	tree, err := c.FSSnapshot()
	if err != nil {
		return "", err
	}

	parent, dirName := filepath.Split(p)
	if parent != "" {
		parentStat, err := fs.Stat(tree, parent)
		if err != nil {
			return "", fmt.Errorf("parent dir error: %w", err)
		}
		if !parentStat.IsDir() {
			return "", fmt.Errorf("parent is not directory")
		}
	}

	var parentItem Item
	if parent == "" {
		parentItem.ID = ""
	} else {
		rawMeta, err := fs.ReadFile(tree, parent+"/.meta")
		if err != nil {
			return "", fmt.Errorf("read parent dir metadata err: %w", err)
		}
		err = json.Unmarshal(rawMeta, &parentItem)
		if err != nil {
			return "", fmt.Errorf("parse parent dir metadata err: %w", err)
		}
	}

	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	doc := uploadDocumentRequest{
		ID:      id.String(),
		Type:    "CollectionType",
		Version: 1,
	}

	docJSON, err := json.Marshal([]uploadDocumentRequest{doc})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("PUT", APIHost+UploadReqAPI, bytes.NewBuffer(docJSON))
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var uploadResp []uploadDocumentResponse
	err = json.Unmarshal(body, &uploadResp)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	zipW := zip.NewWriter(&b)
	f, err := zipW.Create(fmt.Sprintf("%s.content", id.String()))
	if err != nil {
		return "", err
	}
	f.Write([]byte("{}"))
	err = zipW.Close()
	if err != nil {
		return "", err
	}

	req, err = http.NewRequest("PUT", uploadResp[0].BlobURLPut, &b)
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	body, _ = ioutil.ReadAll(resp.Body)
	fmt.Printf("body upload: %s\n", body)
	resp.Body.Close()

	item := metadataDocument{
		Parent:         parentItem.ID,
		VissibleName:   dirName,
		Version:        1,
		ID:             uploadResp[0].ID,
		ModifiedClient: time.Now().Format(time.RFC3339Nano),

		Type: "CollectionType",
	}

	itemJSON, err := json.Marshal([]metadataDocument{item})
	if err != nil {
		return "", err
	}

	req, err = http.NewRequest("PUT", APIHost+UpdateAPI, bytes.NewBuffer(itemJSON))
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	fmt.Printf("body:%s\n", body)

	return id.String(), nil
}

func (c *Client) Remove(name string) error {
	tree, err := c.FSSnapshot()
	if err != nil {
		return err
	}

	rawMeta, err := fs.ReadFile(tree, name)
	if err != nil {
		return err
	}

	var item Item
	err = json.Unmarshal(rawMeta, &item)
	if err != nil {
		return err
	}

	deleteReq := []deleteDocumentRequest{
		{
			ID:      item.ID,
			Version: item.Version,
		},
	}

	reqTxt, err := json.Marshal(deleteReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", APIHost+DeleteAPI, bytes.NewBuffer(reqTxt))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+c.creds.Token())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) FSSnapshot() (fs.FS, error) {
	items, err := c.List()
	if err != nil {
		return nil, err
	}

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
					err = rootFS.WriteFile(pp, data, 0777)
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

const (
	collectionType = "CollectionType"
	documentType   = "DocumentType"
)

type metadataDocument struct {
	ID             string
	Parent         string
	VissibleName   string
	Type           string
	Version        int
	ModifiedClient string
}

type Item struct {
	ID               string
	Hash             string
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

type deleteDocumentRequest struct {
	ID      string
	Version int
}

type uploadDocumentRequest struct {
	ID      string
	Type    string
	Version int
}

type uploadDocumentResponse struct {
	ID                string
	Version           int
	Message           string
	Success           bool
	BlobURLPut        string
	BlobURLPutExpires string
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
}

type blobMetadata struct {
	hash      string
	id        string
	blobType  string
	fileCount int
	size      int
}
