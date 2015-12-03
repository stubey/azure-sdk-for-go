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

// azure login -u tom@msazurextremedatainc.onmicrosoft.com
// azure storage account list => tom-rg-test, tomsatest
// azure storage account keys list --resource-group tom-rg-test tomsatest => PrimaryKey
// export ACCOUNT_NAME=tomsatest
// export ACCOUNT_KEY=PrimaryKey
// export AZURE_STORAGE_ACCOUNT=tomsatest        // For CLI use
// export AZURE_STORAGE_ACCESS_KEY=PrimaryKey    // For CLI use
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
	log.Printf("size      = %v", size)
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

func (b BlobStorageClient) SnapshotBlob(container, name string) error {
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
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}
