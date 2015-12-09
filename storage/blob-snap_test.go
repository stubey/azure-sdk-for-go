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

// getBasicClient returns a test client from storage credentials in the env
func getBasicClient(c *C) storage.Client {
	name := os.Getenv("ACCOUNT_NAME")
	if name == "" {
		c.Fatal("ACCOUNT_NAME not set, need an empty storage account to test")
	}
	key := os.Getenv("ACCOUNT_KEY")
	if key == "" {
		c.Fatal("ACCOUNT_KEY not set")
	}
	cli, err := storage.NewBasicClient(name, key)
	c.Assert(err, IsNil)
	return cli
}

func getBlobClient(c *C) storage.BlobStorageClient {
	return getBasicClient(c).GetBlobService()
}

type SnapshotBlobSuite struct{}

var _ = Suite(&SnapshotBlobSuite{})

// Run this to create a container and blob and get s SASURI
// Comment out defer delete to keep the blob around for TestBlobSASURICorrectness2
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness1(c *C) {
	blobClient := getBlobClient(c)
	containerName := randContainer()
	blobName := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	c.Assert(blobClient.CreateContainer(containerName, storage.ContainerAccessTypePrivate), IsNil)
	defer blobClient.DeleteContainer(containerName)

	// Initialize a PageBlob
	err := blobClient.PutPageBlob(containerName, blobName, int64(size))
	c.Assert(err, IsNil)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobClient.PutPage(containerName, blobName, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Get SASURI
	sasURI, err := blobClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	// Verify SASURI
	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)

	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(len(blobResp), Equals, size)

	log.Printf("blobClient    = ...\n%s", storage.BlobClientToJson(blobClient))
	log.Printf("containerName = %v", containerName)
	log.Printf("blobName      = %v", blobName)
	log.Printf("expiry        = %v", expiry)
	log.Printf("permissions   = %s", permissions)
	log.Printf("sasURI        = %s", sasURI)
	log.Printf("blobResp      = %v", string(blobResp))
}

// Get a SAS URI for a known container and blob
// Get container and blob names from TestBlobSASURICorrectness
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness2(c *C) {
	blobClient := getBlobClient(c)
	containerName := `zzzztest-bqb0asd55c95g5doycvok5r`
	blobName := `0kpayzncfnfxb2fpq063`
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	sasURI, err := blobClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	log.Printf("blobClient    = ...\n%s", storage.BlobClientToJson(blobClient))
	log.Printf("containerName = %v", containerName)
	log.Printf("blobName      = %v", blobName)
	log.Printf("expiry        = %v", expiry)
	log.Printf("permissions   = %s", permissions)
	log.Printf("sasURI        = %s", sasURI)
	log.Printf("blobResp      = %v", string(blobResp))
}

func (s *SnapshotBlobSuite) TestSnapshotBlob(c *C) {
	blobClient := getBlobClient(c)
	containerName := randContainer()
	c.Assert(blobClient.CreateContainer(containerName, storage.ContainerAccessTypePrivate), IsNil)
	defer blobClient.DeleteContainer(containerName)

	blobName := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	// Initialize a PageBlob
	err := blobClient.PutPageBlob(containerName, blobName, int64(size))
	c.Assert(err, IsNil)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobClient.PutPage(containerName, blobName, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Verify
	props, err := blobClient.GetBlobProperties(containerName, blobName)
	c.Assert(err, IsNil)
	c.Assert(props.ContentLength, Equals, int64(size))
	c.Assert(props.BlobType, Equals, storage.BlobTypePage)

	sasURI, err := blobClient.GetBlobSASURI(containerName, blobName, expiry, permissions)
	c.Assert(err, IsNil)

	// Snapshot
	// Snapshot is a PUT - has same blobName as original (no body content)
	// Snapshot Time Identofier is returned in the X-Ms-Snapshot Header
	res, err := blobClient.SnapshotBlob(containerName, blobName)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	statusCode := res.StatusCode
	snapTime := res.Headers.Get("X-Ms-Snapshot")

	log.Printf("blobClient    = ...\n%s", storage.BlobClientToJson(blobClient))
	log.Printf("containerName = %v", containerName)
	log.Printf("blobName      = %v", blobName)
	log.Printf("expiry        = %v", expiry)
	log.Printf("permissions   = %s", permissions)
	log.Printf("sasURI        = %s", sasURI)
	//log.Printf("res           = %+v", res)
	log.Printf("statusCode    = %+v", statusCode)
	log.Printf("snapTime      = %+v", snapTime)
}
