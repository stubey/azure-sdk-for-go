package storage_test

import (
	"crypto/rand"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"azure-sdk-for-go/storage"

	. "azure-sdk-for-go/Godeps/_workspace/src/gopkg.in/check.v1"
)

// go test -v azure-sdk-for-go/storage -check.vv -check.f SnapshotBlobSuite.TestSnapshotBlob

// azure login -u tom@msazurextremedatainc.onmicrosoft.com
// azure storage account list => tom-rg-test, tomsatest
// azure storage account keys list --resource-group tom-rg-test tomsatest => PrimaryKey
// export ACCOUNT_NAME=tomsatest
// export ACCOUNT_KEY=PrimaryKey
// export AZURE_STORAGE_ACCOUNT=tomsatest        // For CLI use, else --account-name
// export AZURE_STORAGE_ACCESS_KEY=PrimaryKey    // For CLI use, else --account-key

const testContainerPrefix = "zzzztest-"

func randContainer() string {
	return testContainerPrefix + randString(32-len(testContainerPrefix))
	//
}

func randString(n int) string {
	if n <= 0 {
		panic("negative number")
	}
	const alphanum = "0123456789abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

type SnapshotBlobSuite struct{}

var _ = Suite(&SnapshotBlobSuite{})

var name string
var key string

func init() {
	key = os.Getenv("ACCOUNT_KEY")
	name = os.Getenv("ACCOUNT_NAME")
}

// Run this to create a container and blob and get s SASURI
// Comment out defer delete to keep the blob around for TestBlobSASURICorrectness2
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness1(c *C) {
	blobClient, err := storage.NewBasicClient(name, key)
	c.Assert(err, IsNil)
	blobStorageClient := blobClient.GetBlobService()

	containerName := randContainer()
	blobName := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	c.Assert(blobStorageClient.CreateContainer(containerName, storage.ContainerAccessTypePrivate), IsNil)
	defer blobStorageClient.DeleteContainer(containerName)

	// Initialize a PageBlob
	err = blobStorageClient.PutPageBlob(containerName, blobName, int64(size))
	c.Assert(err, IsNil)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobStorageClient.PutPage(containerName, blobName, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Get SASURI
	sasURI, err := blobStorageClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	// Verify SASURI
	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)

	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(len(blobResp), Equals, size)

	log.Printf("blobStorageClient = ...\n%s", storage.BlobStorageClientToJson(blobStorageClient))
	log.Printf("containerName     = %v", containerName)
	log.Printf("blobName          = %v", blobName)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	log.Printf("blobResp          = %v", string(blobResp))
}

// Get a SAS URI for a known container and blob
// Get container and blob names from TestBlobSASURICorrectness
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness2(c *C) {
	blobClient, err := storage.NewBasicClient(name, key)
	c.Assert(err, IsNil)
	blobStorageClient := blobClient.GetBlobService()

	containerName := `zzzztest-bqb0asd55c95g5doycvok5r`
	blobName := `0kpayzncfnfxb2fpq063`
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	sasURI, err := blobStorageClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	log.Printf("blobStorageClient = ...\n%s", storage.BlobStorageClientToJson(blobStorageClient))
	log.Printf("containerName     = %v", containerName)
	log.Printf("blobName          = %v", blobName)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	log.Printf("blobResp          = %v", string(blobResp))
}

func (s *SnapshotBlobSuite) TestSnapshotBlob(c *C) {
	blobClient, err := storage.NewBasicClient(name, key)
	c.Assert(err, IsNil)
	blobStorageClient := blobClient.GetBlobService()

	containerName := randContainer()
	c.Assert(blobStorageClient.CreateContainer(containerName, storage.ContainerAccessTypePrivate), IsNil)
	// Delete container when test completes
	defer blobStorageClient.DeleteContainer(containerName)

	blobName := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	// Initialize a PageBlob
	err = blobStorageClient.PutPageBlob(containerName, blobName, int64(size))
	c.Assert(err, IsNil)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobStorageClient.PutPage(containerName, blobName, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Verify
	props, err := blobStorageClient.GetBlobProperties(containerName, blobName)
	c.Assert(err, IsNil)
	c.Assert(props.ContentLength, Equals, int64(size))
	c.Assert(props.BlobType, Equals, storage.BlobTypePage)

	sasURI, err := blobStorageClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	// Snapshot
	// Snapshot is a PUT - has same blobName as original (no body content)
	// Snapshot Time Identofier is returned in the X-Ms-Snapshot Header
	res, err := blobStorageClient.SnapshotBlob(containerName, blobName)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	statusCode := res.StatusCode
	snapTime := res.Headers.Get("X-Ms-Snapshot")

	log.Printf("blobStorageClient = ...\n%s", storage.BlobStorageClientToJson(blobStorageClient))
	log.Printf("containerName     = %v", containerName)
	log.Printf("blobName          = %v", blobName)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	//log.Printf("res           = %+v", res)
	log.Printf("statusCode    = %+v", statusCode)
	log.Printf("snapTime      = %+v", snapTime)
}
