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
// azure group create --location eastus tomrgsnaptest
// azure storage account create --resource-group tomrgsnaptest --type LRS --location eastus tomsasnaptest
// azure storage account list => tomsasnaptest / tomrgsnaptest
// azure storage account keys list --resource-group tomrgsnaptest tomsasnaptest => PrimaryKey
// export ACCOUNT_NAME=tomsasnaptest
// export ACCOUNT_KEY=PrimaryKey
// export AZURE_STORAGE_ACCOUNT=tomsatest        // For CLI use, else --account-name
// export AZURE_STORAGE_ACCESS_KEY=PrimaryKey    // For CLI use, else --account-key

const testContainerPrefix = "zzzzsnaptest-"

func getBlobClient(c *C) storage.BlobStorageClient {
	bc := getBasicClient(c)
	return bc.GetBlobService()
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

func init() {
	log.SetFlags(log.Lshortfile)
}

// Run this to create a container and blob and get s SASURI
// Comment out defer delete to keep the blob around for TestBlobSASURICorrectness2
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness1(c *C) {
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	containerName := randContainer()
	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	c.Assert(blobService.CreateContainer(containerName, storage.ContainerAccessTypePrivate), IsNil)
	defer blobService.DeleteContainer(containerName)

	// Initialize a PageBlob
	err := blobService.PutPageBlob(containerName, blob, int64(size))
	c.Assert(err, IsNil)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(containerName, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Get SASURI
	sasURI, err := blobService.GetBlobSASURI(containerName, blob, expiry, permissions)
	c.Assert(err, IsNil)

	// Verify SASURI
	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)

	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(len(blobResp), Equals, size)

	log.Printf("blobService = ...\n%s", storage.BlobStorageClientToJson(blobService))
	log.Printf("containerName     = %v", containerName)
	log.Printf("blob          = %v", blob)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	log.Printf("blobResp          = %v", string(blobResp))
}

// Get a SAS URI for a known container and blob
// Get container and blob names from TestBlobSASURICorrectness
// Remember to manually delete the container
func (s *SnapshotBlobSuite) TestBlobSASURICorrectness2(c *C) {
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	containerName := `zzzztest-bqb0asd55c95g5doycvok5r`
	blob := `0kpayzncfnfxb2fpq063`
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	sasURI, err := blobService.GetBlobSASURI(containerName, blob, expiry, permissions)
	c.Assert(err, IsNil)

	resp, err := http.Get(sasURI)
	c.Assert(err, IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	log.Printf("blobService = ...\n%s", storage.BlobStorageClientToJson(blobService))
	log.Printf("containerName     = %v", containerName)
	log.Printf("blob          = %v", blob)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	log.Printf("blobResp          = %v", string(blobResp))
}

func (s *SnapshotBlobSuite) TestSnapshotBlob(c *C) {
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	// Delete container when test completes
	defer blobService.DeleteContainer(container)

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	// Initialize a PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer blobService.DeleteBlob(container, blob)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Verify
	props, err := blobService.GetBlobProperties(container, blob)
	c.Assert(err, IsNil)
	c.Assert(props.ContentLength, Equals, int64(size))
	c.Assert(props.BlobType, Equals, storage.BlobTypePage)

	sasURI, err := blobService.GetBlobSASURI(container, blob, expiry, permissions)
	c.Assert(err, IsNil)

	// Snapshot
	// Snapshot is a PUT - has same blob as original (no body content)
	// Snapshot Time Identofier is returned in the X-Ms-Snapshot Header
	res, err := blobService.SnapshotBlob(container, blob, storage.Metadata{})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	statusCode := res.StatusCode
	snapTime := res.Headers.Get("X-Ms-Snapshot")

	log.Printf("blobService = ...\n%s", storage.BlobStorageClientToJson(blobService))
	log.Printf("container     = %v", container)
	log.Printf("blob          = %v", blob)
	log.Printf("expiry            = %v", expiry)
	log.Printf("permissions       = %s", permissions)
	log.Printf("sasURI            = %s", sasURI)
	//log.Printf("res           = %+v", res)
	log.Printf("statusCode    = %+v", statusCode)
	log.Printf("snapTime      = %+v", snapTime)
}

func (s *SnapshotBlobSuite) TestSnapExists(c *C) {
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	// Delete container when test completes
	defer blobService.DeleteContainer(container)

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))

	// Initialize an empty PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer blobService.DeleteBlob(container, blob)

	// Update an existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	res, err := blobService.SnapshotBlob(container, blob, storage.Metadata{})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)
	snaptime := res.Headers.Get("X-Ms-Snapshot")
	snap := blob + "?snapshot=" + snaptime
	log.Printf("snap = %v", snap)
	defer blobService.DeleteBlob(container, snap)

	ok, err := blobService.BlobExists(container, snap)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	snapfmt := "2006-01-02T15:04:05.9999999Z"
	// Go way back to avoid time zone issues (local datetime is in future of remote)
	badSnaptime := time.Now().Add(time.Duration(-24 * time.Hour)).Format(snapfmt)
	c.Assert(snaptime, Not(Equals), badSnaptime)
	badsnap := blob + "?snapshot=" + badSnaptime
	log.Printf("badsnap = %v", badsnap)
	ok, err = blobService.BlobExists(container, badsnap)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, false)
}
