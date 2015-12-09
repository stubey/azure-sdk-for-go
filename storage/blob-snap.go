package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type BlobClientJ struct {
	AccountName string
	AccountKey  []byte
	UseHTTPS    bool
	BaseURL     string
	ApiVersion  string
}

func BlobClientToJson(c BlobStorageClient) string {
	c2 := BlobClientJ{
		AccountName: c.client.accountName,
		AccountKey:  c.client.accountKey,
		UseHTTPS:    c.client.useHTTPS,
		BaseURL:     c.client.baseURL,
		ApiVersion:  c.client.apiVersion,
	}
	txt, _ := json.MarshalIndent(c2, "", "  ")
	return string(txt)
}

type SnapshotResponse struct {
	StatusCode int
	Headers    http.Header
}

func (b BlobStorageClient) SnapshotBlob(container, name string) (res SnapshotResponse, err error) {
	verb := "PUT"
	path := fmt.Sprintf("%s/%s", container, name)

	// Snapshot cmd
	urlValues := url.Values{"comp": {"snapshot"}}

	uri := b.client.getEndpoint(blobServiceName, path, urlValues)
	headers := b.client.getStandardHeaders()
	headers["Content-Length"] = fmt.Sprintf("%v", 0)

	log.Printf("\n====  SnapshotBlob()")
	log.Printf("container = %s", container)
	log.Printf("name      = %s", name)
	log.Printf("verb      = %s", verb)
	//log.Printf("urlValues = %+v", urlValues)
	log.Printf("uri       = %s", uri)
	log.Printf("headers   = %+v", headers)

	resp, err := b.client.exec(verb, uri, headers, nil)
	if err != nil {
		return
	}
	// Actually, no body content since a PUT - still need to close
	defer resp.body.Close()

	res = SnapshotResponse{
		StatusCode: resp.statusCode,
		Headers:    resp.headers,
	}

	return
}
