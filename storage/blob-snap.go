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

func renderRequest(name string, container string, verb string, uri string, headers map[string]string) string {
	s := struct {
		Name      string
		Container string
		Verb      string
		URI       string
		Headers   map[string]string
	}{name, container, verb, uri, headers}
	jbytes, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "unable to render request"
	}
	return string(jbytes)
}

func (b BlobStorageClient) Snapshot(container, name string, metaSnap Metadata) (res SnapshotResponse, err error) {
	debug := false

	if debug == true {
		jbytes, _ := json.MarshalIndent(metaSnap, "", "  ")
		log.Printf("metaSnap headers to add = ...\n%s", string(jbytes))
	}

	verb := "PUT"
	path := fmt.Sprintf("%s/%s", container, name)
	urlValues := url.Values{"comp": {"snapshot"}}

	uri := b.client.getEndpoint(blobServiceName, path, urlValues)
	headers := b.client.getStandardHeaders()
	headers["Content-Length"] = fmt.Sprintf("%v", 0)

	if debug == true {
		log.Printf("base request = %s", renderRequest(name, container, verb, uri, headers))
		//
	}

	// Add snapshot tags
	for key, value := range metaSnap {
		hv := fmt.Sprintf("x-ms-meta-%s", key)
		//log.Printf("metasnap headers[%s] = %s", hv, value)
		headers[hv] = value
	}

	if debug == true {
		log.Printf("snapshot request = %s", renderRequest(name, container, verb, uri, headers))
		//
	}

	resp, err := b.client.exec(verb, uri, headers, nil, b.auth)
	if err != nil {
		return res, err
	}
	defer resp.body.Close()

	res = SnapshotResponse{
		StatusCode: resp.statusCode,
		Headers:    resp.headers,
	}

	if debug == true {
		metaBlob, err := b.client.GetBlobService().GetBlobMetadata(container, name)
		if err != nil {
			return res, err
		}
		jbytes, _ := json.MarshalIndent(metaBlob, "", "  ")
		log.Printf("after snapshot: basename metadata = ...\n%s", string(jbytes))

		snapname := name + "?snapshot=" + res.Headers.Get("X-Ms-Snapshot")
		log.Printf("snapname = %s", snapname)
		metaBlob, err = b.client.GetBlobService().GetBlobMetadata(container, snapname)
		if err != nil {
			return res, err
		}
		jbytes, _ = json.MarshalIndent(metaBlob, "", "  ")
		log.Printf("after snapshot: snapshot metadata = ...\n%s", string(jbytes))
	}

	return
}
