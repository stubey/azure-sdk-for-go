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

// go test -v azure-sdk-for-go/storage -check.vv -check.f SnapshotSuite.TestSnapshot

// azure login -u tom@msazurextremedatainc.onmicrosoft.com
// azure group create --location eastus tom0rgsnaptest
// azure storage account create --resource-group tom0rgsnaptest --type LRS --location eastus tom0sasnaptest
// azure storage account list => tom0sasnaptest / tom0rgsnaptest
// azure storage account keys list --resource-group tom0rgsnaptest tom0sasnaptest => PrimaryKey
// export ACCOUNT_NAME=tom0sasnaptest
// export ACCOUNT_KEY=PrimaryKey
// export AZURE_STORAGE_ACCOUNT=tomsatest        // For CLI use, else --account-name
// export AZURE_STORAGE_ACCESS_KEY=PrimaryKey    // For CLI use, else --account-key

const testContainerPrefix = "zzzzsnaptest-"

// Query ENV for a STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY to test with
func getBlobClient(c *C) storage.BlobStorageClient {
	bc := getBasicClient(c)
	return bc.GetBlobService()
}

// getBasicClient returns a test client from storage credentials in the env
func getBasicClient(c *C) storage.Client {
	name := os.Getenv("STORAGE_ACCOUNT_NAME")
	if name == "" {
		c.Fatal("STORAGE_ACCOUNT_NAME not set, need an empty storage account to test")
	}
	key := os.Getenv("STORAGE_ACCOUNT_KEY")
	if key == "" {
		c.Fatal("STORAGE_ACCOUNT_KEY not set")
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

type SnapshotSuite struct{}

var _ = Suite(&SnapshotSuite{})

func init() {
	log.SetFlags(log.Lshortfile)
}

// Run this to create a container and blob and get s SASURI
// Comment out "defer blobService.DeleteContainer(containerName)" to keep the blob around for TestBlobSASURICorrectness2
// Remember to manually delete the container
func (s *SnapshotSuite) TestBlobSASURICorrectness1(c *C) {
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
func (s *SnapshotSuite) TestBlobSASURICorrectness2(c *C) {
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

func createStorageAccount() {
	// azure login -u tom@msazurextremedatainc.onmicrosoft.com
	// azure group create --location eastus --name tom0rgsnaptest
	// - azure group list | grep tom0rgsnaptest
	// - azure group delete --quiet tom0rgsnaptest
	// azure storage account create --resource-group tom0rgsnaptest --type LRS --location eastus tom0sasnaptest
	// -- azure storage account list --resource-group tom0rgsnaptest
	// azure storage account keys  list --resource-group tom0rgsnaptest tom0sasnaptest
	// export STORAGE_ACCOUNT_NAME=tom0sasnaptest
	// export STORAGE_ACCOUNT_KEY=
	// go test -v azure-sdk-for-go/storage -check.vv -check.f SnapshotSuite.TestSnapExists
}

func (s *SnapshotSuite) TestSnapCreate(c *C) {
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
	res, err := blobService.Snapshot(container, blob, storage.Metadata{})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	statusCode := res.StatusCode
	snapTime := res.Headers.Get("X-Ms-Snapshot")

	log.Printf("blobService = ...\n%s", storage.BlobStorageClientToJson(blobService))
	log.Printf("container     = %v", container)
	log.Printf("blob          = %v", blob)
	log.Printf("expiry        = %v", expiry)
	log.Printf("permissions   = %s", permissions)
	log.Printf("sasURI        = %s", sasURI)
	//log.Printf("res           = %+v", res)
	log.Printf("statusCode    = %+v", statusCode)
	log.Printf("snapTime      = %+v", snapTime)
}

func (s *SnapshotSuite) TestSnapExists(c *C) {
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

	res, err := blobService.Snapshot(container, blob, storage.Metadata{})
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
	// Go back 24*Hours to avoid time zone issues (local datetime is in future of remote)
	badSnaptime := time.Now().Add(time.Duration(-24 * time.Hour)).Format(snapfmt)
	c.Assert(snaptime, Not(Equals), badSnaptime)
	badsnap := blob + "?snapshot=" + badSnaptime
	log.Printf("badsnap = %v", badsnap)
	ok, err = blobService.BlobExists(container, badsnap)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, false)
}

func (s *SnapshotSuite) TestSnapGetURL(c *C) {
	api, err := storage.NewBasicClient("foo", "YmFy")
	c.Assert(err, IsNil)
	cli := api.GetBlobService()

	// TODO: Is this right? ? => %3F
	c.Assert(cli.GetBlobURL("c", "nested/blob?snapshot=2015-12-23T17:51:38.8999249Z"), Equals, "https://foo.blob.core.windows.net/c/nested/blob%3Fsnapshot=2015-12-23T17:51:38.8999249Z")
}
