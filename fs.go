package remarkablecloud

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/psanford/memfs"
)

func (c *Client) fsSnapshotFromList(items []Item, fullOpen bool) (fs.FS, error) {
	seen := make(map[string]Item)
	paths := make(map[string]string)

	docPaths := make(map[string]*Item)

	openHook := func(path string, origContent []byte, origErr error) ([]byte, error) {
		if item := docPaths[path]; item != nil {
			resp, err := c.GetBlob(item.Hash)
			if err != nil {
				return nil, err
			}

			entries, err := readBlobIndex(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			var (
				fileType string
				fileHash string
			)
			for _, entry := range entries {
				if strings.HasSuffix(entry.id, ".epub") {
					fileType = "epub"
					fileHash = entry.hash
					break
				} else if strings.HasSuffix(entry.id, ".pdf") && fileType == "" {
					// epubs will also contain pdf, so we only set pdf if we haven't already seen epub
					// we will also continue to iterate to allow epub to overwrite this
					fileType = "pdf"
					fileHash = entry.hash
				}
			}

			if fileHash == "" {
				return nil, fmt.Errorf("failed to find file content")
			}

			resp, err = c.GetBlob(fileHash)
			if err != nil {
				return nil, err
			}

			content, err := io.ReadAll(resp.Body)
			resp.Body.Close()

			return content, err
		}
		return origContent, origErr
	}

	rootFS := memfs.New()
	if fullOpen {
		rootFS = memfs.New(memfs.WithOpenHook(openHook))
	}

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
					item := item
					docPaths[pp] = &item
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
