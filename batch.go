package remarkablecloud

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Batch struct {
	generation   int
	origRootHash string
	curRootHash  string

	mu    sync.Mutex
	blobs map[string]batchBlob

	pendingPuts []pendingPut

	c *Client
}

type pendingPut struct {
	hashKey string
	id      string
	content []byte
}

type batchBlob struct {
	data    []byte
	headers http.Header
	status  int
}

func (b *Batch) GetAndCacheBlob(id string) (*http.Response, error) {
	b.mu.Lock()
	existing, found := b.blobs[id]
	if found && id == "root" {
		rootDoc := RootMetadata{
			Hash:       b.curRootHash,
			Generation: int64(b.generation),
		}
		data, err := json.Marshal(rootDoc)
		if err != nil {
			return nil, err
		}
		existing.data = data
	}
	b.mu.Unlock()

	if !found {
		r, err := b.c.GetBlob(id)
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		existing = batchBlob{
			data:    body,
			status:  r.StatusCode,
			headers: r.Header.Clone(),
		}

		b.mu.Lock()
		b.blobs[id] = existing
		if id == "root" {
			var rootDoc RootMetadata
			err = json.Unmarshal(body, &rootDoc)
			if err != nil {
				panic(err)
			}
			b.curRootHash = rootDoc.Hash
			b.generation = int(rootDoc.Generation)
		}

		b.mu.Unlock()
	}

	resp := http.Response{
		StatusCode: existing.status,
		Body:       io.NopCloser(bytes.NewReader(existing.data)),
		Header:     existing.headers.Clone(),
	}
	return &resp, nil
}

func (b *Batch) loadTree() (*rawListResult, error) {
	result, err := b.rawList()
	if err != nil {
		return nil, err
	}

	b.generation = result.generation
	b.origRootHash = result.rootHash
	b.curRootHash = result.rootHash

	return result, nil
}

func (b *Batch) rawList() (*rawListResult, error) {
	resp, err := b.GetAndCacheBlob("root")
	if err != nil {
		return nil, err
	}

	var rootMeta RootMetadata
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&rootMeta)
	if err != nil {
		return nil, err
	}

	resp, err = b.GetAndCacheBlob(string(rootMeta.Hash))
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
				resp, err := b.GetAndCacheBlob(meta.hash)
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
						resp, err = b.GetAndCacheBlob(entry.hash)
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
		generation:   int(rootMeta.Generation),
		rootHash:     rootMeta.Hash,
	}, nil
}

func (b *Batch) UpdateRootHash(hashKey string) {
	b.curRootHash = hashKey
}

func (b *Batch) PutBlob(hashKey, id string, r io.Reader, opts ...PutBlobOption) error {
	var options putBlobOptions
	for _, opt := range opts {
		opt(&options)
	}

	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.blobs[hashKey] = batchBlob{
		data:   content,
		status: 200,
	}

	b.pendingPuts = append(b.pendingPuts, pendingPut{
		hashKey: hashKey,
		id:      id,
		content: content,
	})

	return nil
}

func (b *Batch) putBlobWithHashedContent(id string, r io.ReadSeeker) (*blobMetadata, error) {
	key, err := hashKey(r)
	if err != nil {
		return nil, err
	}

	err = b.PutBlob(key, id, r)
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

func (b *Batch) putContentDoc(id, ext string) (*blobMetadata, error) {
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
	return b.putBlobWithHashedContent(cdocID, bytes.NewReader(cdocJson))
}

func (b *Batch) putEmptyContentDoc(id string) (*blobMetadata, error) {
	cdocJson := []byte("{}")

	cdocID := fmt.Sprintf("%s.content", id)
	return b.putBlobWithHashedContent(cdocID, bytes.NewReader(cdocJson))
}

func (b *Batch) putMetaDoc(parentID, id, fileName string) (*blobMetadata, error) {
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
	return b.putBlobWithHashedContent(itemID, bytes.NewReader(itemJson))
}

func (b *Batch) putMetaDirDoc(parentID, id, fileName string) (*blobMetadata, error) {
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
	return b.putBlobWithHashedContent(itemID, bytes.NewReader(itemJson))
}

func (b *Batch) putEmptyPageData(id string) (*blobMetadata, error) {
	cdocID := fmt.Sprintf("%s.pagedata", id)
	data := make([]byte, 0)
	return b.putBlobWithHashedContent(cdocID, bytes.NewReader(data))
}

func (b *Batch) putListing(id string, blobs []blobMetadata) (*blobMetadata, error) {
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

	if id == "" {
		return nil, fmt.Errorf("putListing id cannot be empty string")
	}

	err := b.PutBlob(key, id, r)
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

// Add a file to the batch for uploading.
// p is the name of the file on the device. It can be a path.
// ext is the file extension. The only values are "epub" and "pdf".
// r is a ReadSeeker of the actual content.
func (b *Batch) Put(p string, ext string, r io.ReadSeeker) (*PutResult, error) {
	raw, err := b.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := b.c.fsSnapshotFromList(items, false)
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
	orig, err := b.putBlobWithHashedContent(id+"."+ext, r)
	if err != nil {
		return nil, fmt.Errorf("put raw doc err: %w", err)
	}
	files = append(files, *orig)

	cmeta, err := b.putContentDoc(id, ext)
	if err != nil {
		return nil, fmt.Errorf("put contentDoc err: %w", err)
	}
	files = append(files, *cmeta)

	meta, err := b.putMetaDoc(parentItem.ID, id, fileName)
	if err != nil {
		return nil, fmt.Errorf("put .metadata err: %w", err)
	}
	files = append(files, *meta)

	pd, err := b.putEmptyPageData(id)
	if err != nil {
		return nil, fmt.Errorf("put .pagedata err: %w", err)
	}
	files = append(files, *pd)

	listingMeta, err := b.putListing(id+".docSchema", files)
	if err != nil {
		return nil, fmt.Errorf("put listing err: %w", err)
	}

	rawEntries := raw.blobMetadata

	rawEntries = append(rawEntries, *listingMeta)

	rootMeta, err := b.putListing(RootListingID, rawEntries)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	b.UpdateRootHash(rootMeta.hash)

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMeta.hash,
		DocID:       id,
	}

	return &result, nil
}

// Mkdir creates a directory synced to the device.
// On success, Mkdir returns the directory ID.
func (b *Batch) Mkdir(p string) (*PutResult, error) {
	raw, err := b.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := b.c.fsSnapshotFromList(items, false)
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

	cmeta, err := b.putEmptyContentDoc(id)
	if err != nil {
		return nil, fmt.Errorf("put contentEmptyDoc err: %w", err)
	}
	files = append(files, *cmeta)

	meta, err := b.putMetaDirDoc(parentItem.ID, id, dirName)
	if err != nil {
		return nil, fmt.Errorf("put .metadata err: %w", err)
	}
	files = append(files, *meta)

	listingMeta, err := b.putListing(id, files)
	if err != nil {
		return nil, fmt.Errorf("put listing err: %w", err)
	}

	rawEntries := raw.blobMetadata

	rawEntries = append(rawEntries, *listingMeta)

	rootMeta, err := b.putListing(RootListingID, rawEntries)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	b.UpdateRootHash(rootMeta.hash)

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMeta.hash,
		DocID:       id,
	}

	return &result, nil
}

func (b *Batch) Remove(name string) (*PutResult, error) {
	raw, err := b.rawList()
	if err != nil {
		return nil, err
	}

	items := raw.items

	tree, err := b.c.fsSnapshotFromList(items, false)
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

	rootMeta, err := b.putListing(RootListingID, index)
	if err != nil {
		return nil, fmt.Errorf("put root listing err: %w", err)
	}

	b.UpdateRootHash(rootMeta.hash)

	result := PutResult{
		OldRootHash: raw.rootHash,
		NewRootHash: rootMeta.hash,
		DocID:       item.ID,
	}

	return &result, nil
}

var RootListingID = "root.docSchema"

func (b *Batch) Commit() (*PutResult, error) {
	if b.curRootHash == b.origRootHash {
		return nil, fmt.Errorf("No root tree changes")
	}
	lastRootListingIdx := -1
	for i, change := range b.pendingPuts {
		if change.id == RootListingID {
			lastRootListingIdx = i
		}
	}
	if lastRootListingIdx < 0 {
		return nil, errors.New("No pending commits")
	}

	var putNewRoot bool

	for i, change := range b.pendingPuts {
		if change.id == RootListingID && i < lastRootListingIdx {
			continue
		}

		putReq := RawPubBlobRequest{
			Key:      change.hashKey,
			Filename: change.id,
			Content:  bytes.NewReader(change.content),
		}

		err := b.c.RawPutBlob(putReq)
		if err != nil {
			return nil, fmt.Errorf("put blob %s err: %s", change.hashKey, err)
		}
		if change.hashKey == b.curRootHash {
			putNewRoot = true
		}
	}

	if !putNewRoot {
		return nil, fmt.Errorf("did not put a new root index listing")
	}

	resp, err := b.c.PutRoot(RootMetadata{
		Hash:       b.curRootHash,
		Generation: int64(b.generation),
		Broadcast:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("put root ptr err: %w", err)
	}

	b.generation = int(resp.Generation)

	return &PutResult{
		OldRootHash: b.origRootHash,
		NewRootHash: b.curRootHash,
	}, nil
}

func (b *Batch) List() ([]Item, error) {
	raw, err := b.rawList()
	return raw.items, err
}

func (b *Batch) FSSnapshot() (fs.FS, error) {
	items, err := b.List()
	if err != nil {
		return nil, err
	}
	return b.c.fsSnapshotFromList(items, true)
}
