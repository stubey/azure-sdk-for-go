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

type Metadata map[string]string

type ParsedURLNameQuery struct {
	Name  string
	Query url.Values
}

func ParseURLNameQuery(name string) (parsedURL ParsedURLNameQuery, err error) {
	uri, err := url.Parse(name)
	if err != nil {
		return
	}
	parsedURL.Query = uri.Query()
	uri.RawQuery = ""
	parsedURL.Name = uri.String()
	return
}

type BlobStorageStruct struct {
	AccountName string
	AccountKey  []byte
	UseHTTPS    bool
	BaseURL     string
	ApiVersion  string
}

func BlobStorageClientToJson(c BlobStorageClient) string {
	c2 := BlobStorageStruct{
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

func showRequest(name string, container string, verb string, uri string, headers map[string]string) (string, error) {
	s := struct {
		Name      string
		Container string
		Verb      string
		URI       string
		Headers   map[string]string
	}{name, container, verb, uri, headers}
	jbytes, err := json.MarshalIndent(s, "", "  ")
	return string(jbytes), err
}

func (b BlobStorageClient) Snapshot(container, name string, metaSnap Metadata) (res SnapshotResponse, err error) {

	verb := "PUT"
	path := fmt.Sprintf("%s/%s", container, name)
	// blob cmd
	urlValues := url.Values{"comp": {"snapshot"}}

	uri := b.client.getEndpoint(blobServiceName, path, urlValues)
	headers := b.client.getStandardHeaders()
	headers["Content-Length"] = fmt.Sprintf("%v", 0)

	// Merge original blob metadata with snap metadata
	metaBlob, err := b.client.GetBlobService().GetBlobMetadata(container, name)
	if err != nil {
		return
	}
	for key, value := range metaBlob {
		hv := fmt.Sprintf("x-ms-meta-%s", key)
		headers[hv] = value
	}
	for key, value := range metaSnap {
		hv := fmt.Sprintf("x-ms-meta-%s", key)
		headers[hv] = value
	}

	req, _ := showRequest(name, container, verb, uri, headers)
	log.Printf("%s", req)
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
