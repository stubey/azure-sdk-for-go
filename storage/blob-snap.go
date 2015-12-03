package storage

import (
	//"bytes"
	//"encoding/xml"
	//"errors"
	"fmt"
	"io"
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

	log.Printf("verb = %s", verb)
	log.Printf("uri  = %s", uri)
	log.Printf("headers = %+v", headers)

	return b.client.exec(verb, uri, headers, nil)
}

// See https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
func (b BlobStorageClient) CreateBlockBlobFromReader(container, name string, size uint64, blob io.Reader) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypeBlock)
	headers["Content-Length"] = fmt.Sprintf("%d", size)

	log.Printf("verb = %s", verb)
	log.Printf("uri  = %s", uri)
	log.Printf("headers = %+v", headers)

	resp, err := b.client.exec("PUT", uri, headers, blob)
	if err != nil {
		return err
	}
	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}
