package storage

import (
	//"bytes"
	//"encoding/xml"
	//"errors"
	"fmt"
	//"io"
	"log"
	"net/http"
	"net/url"
	//"strconv"
	//"strings"
	//"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

// go test -v azure-sdk-for-go/storage/ --check.vv --check.f StorageBlobSuite.TestBlobSASURICorrectness1
// https://msdn.microsoft.com/en-us/library/azure/ee691971.aspx

func (b BlobStorageClient) CreateContainer(name string, access ContainerAccessType) error {
	resp, err := b.createContainer(name, access)
	if err != nil {
		return err
	}
	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

func (b BlobStorageClient) createContainer(name string, access ContainerAccessType) (*storageResponse, error) {
	verb := "PUT"
	uri := b.client.getEndpoint(blobServiceName, pathForContainer(name), url.Values{"restype": {"container"}})

	headers := b.client.getStandardHeaders()
	headers["Content-Length"] = "0"
	if access != "" {
		headers["x-ms-blob-public-access"] = string(access)
	}

	log.Printf("\n=====  createContainer()")
	log.Printf("name     = %s", name)
	log.Printf("access   = %s", access)
	log.Printf("verb     = %s", verb)
	log.Printf("uri      = %s", uri)
	log.Printf("headers  = %+v", headers)

	return b.client.exec(verb, uri, headers, nil)
}

func (b BlobStorageClient) PutPageBlob(container, name string, size int64) error {
	verb := "PUT"
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypePage)
	headers["x-ms-blob-content-length"] = fmt.Sprintf("%v", size)
	headers["Content-Length"] = fmt.Sprintf("%v", 0)

	log.Printf("\n====  PutPageBlob()")
	log.Printf("container = %s", container)
	log.Printf("name      = %s", name)
	log.Printf("size      = %s", size)
	log.Printf("verb      = %s", verb)
	log.Printf("uri       = %s", uri)
	log.Printf("headers   = %+v", headers)

	resp, err := b.client.exec(verb, uri, headers, nil)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// func (b BlobStorageClient) PutPageBlob(container, name string, size int64) error {
func (b BlobStorageClient) SnapshotBlob(container, name string) error {
	verb := "PUT"
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypePage)
	headers["Content-Length"] = fmt.Sprintf("%v", 0)

	qry := "comp=snapshot"

	log.Printf("\n====  PutPageBlob()")
	log.Printf("container = %s", container)
	log.Printf("name      = %s", name)
	log.Printf("qry       = %s", qry)
	log.Printf("verb      = %s", verb)
	log.Printf("uri       = %s", uri)
	log.Printf("headers   = %+v", headers)

	resp, err := b.client.exec(verb, uri, headers, nil)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}
