package storage_test

import (
	"crypto/rand"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
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
// export AZURE_STORAGE_ACCOUNT=tom0sasnaptest        // For CLI use, else --account-name
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
func (s *SnapshotSuite) xTestBlobSASURICorrectness2(c *C) {
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

// CLI commands to create a Resource Group and Storage Account and add to ENV strings
func createStorageAccount() {
	// azure login -u tom@msazurextremedatainc.onmicrosoft.com
	// azure group create --location eastus --name tom0rgsnaptest
	// - azure group list | grep tom0rgsnaptest
	// - azure group delete --quiet tom0rgsnaptest
	// azure storage account create --resource-group tom0rgsnaptest --type LRS --location eastus tom0sasnaptest
	// -- azure storage account list --resource-group tom0rgsnaptest
	// azure storage account keys list --resource-group tom0rgsnaptest tom0sasnaptest
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
	// Snapshot Time Identifier is returned in the X-Ms-Snapshot Header
	meta := storage.Metadata{"someKey": "someValue"}
	res, err := blobService.Snapshot(container, blob, meta)
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
	log.Printf("res.Headers   = %+v", res.Headers)
}

func (s *SnapshotSuite) TestSnapBlobExists(c *C) {
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
	// Else Azure detects a bad request due to future timestamp without doing an actual query
	badSnaptime := time.Now().Add(time.Duration(-24 * time.Hour)).Format(snapfmt)
	c.Assert(snaptime, Not(Equals), badSnaptime)
	badsnap := blob + "?snapshot=" + badSnaptime
	log.Printf("badsnap = %v", badsnap)
	ok, err = blobService.BlobExists(container, badsnap)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, false)
}

// Just tests that snapshot name can be formed by
func (s *SnapshotSuite) TestSnapGetBlobURL(c *C) {
	api, err := storage.NewBasicClient("foo", "YmFy")
	c.Assert(err, IsNil)
	blobService := api.GetBlobService()

	c.Assert(blobService.GetBlobURL("c", "nested/blob?snapshot=2015-12-23T17:51:38.8999249Z"), Equals, "https://foo.blob.core.windows.net/c/nested/blob?snapshot=2015-12-23T17%3A51%3A38.8999249Z")
}

func (s *SnapshotSuite) TestSnapGetBlobProperties(c *C) {
	// === Create a snapshot :: Start
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	defer func() {
		err := blobService.DeleteContainer(container)
		c.Check(err, IsNil)
	}()

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))

	// Initialize an empty PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer func() {
		err := blobService.DeleteBlob(container, blob)
		c.Check(err, IsNil)
	}()

	// Add some blob metadata
	blobMeta := map[string]string{
		"blobMetaKey1": "blobMetaValue1",
	}
	err = blobService.SetBlobMetadata(container, blob, blobMeta)

	// Write body to existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Prepare some metadata - N.B. Keys get camelCased
	snapid := time.Now().Format(time.RFC3339Nano)
	snapMeta := storage.Metadata{
		"snapid":       snapid,
		"snapMetaKey1": "snapMetaValue1",
	}

	// Do snapshot
	res, err := blobService.Snapshot(container, blob, snapMeta)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	// Retrieve snapshot id from response header
	snaptime := res.Headers.Get("X-Ms-Snapshot")
	snap := blob + "?snapshot=" + snaptime
	defer func() {
		err := blobService.DeleteBlob(container, snap)
		c.Check(err, IsNil)
	}()
	// === Create a snapshot :: End

	// Get blob properties
	props, err := blobService.GetBlobProperties(container, snap)
	c.Assert(err, IsNil)

	c.Assert(props.ContentLength, Equals, int64(len(body)))
	c.Assert(props.BlobType, Equals, storage.BlobTypePage)
}

func (s *SnapshotSuite) TestSnapGetBlobMetadata(c *C) {
	// === Create a snapshot :: Start
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	defer func() {
		err := blobService.DeleteContainer(container)
		c.Check(err, IsNil)
	}()

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))

	// Initialize an empty PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer func() {
		err := blobService.DeleteBlob(container, blob)
		c.Check(err, IsNil)
	}()

	// Add some blob metadata
	blobMeta := map[string]string{
		"blobMetaKey1": "blobMetaValue1",
	}
	err = blobService.SetBlobMetadata(container, blob, blobMeta)

	// Write body to existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Prepare some metadata - N.B. Keys get camelCased
	snapid := time.Now().Format(time.RFC3339Nano)
	snapMeta := storage.Metadata{
		"snapid":       snapid,
		"snapMetaKey1": "snapMetaValue1",
	}

	// Do snapshot
	res, err := blobService.Snapshot(container, blob, snapMeta)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	// Retrieve snapshot id from response header
	snaptime := res.Headers.Get("X-Ms-Snapshot")
	snap := blob + "?snapshot=" + snaptime
	defer func() {
		err := blobService.DeleteBlob(container, snap)
		c.Check(err, IsNil)
	}()
	// === Create a snapshot :: End

	m, err := blobService.GetBlobMetadata(container, snap)
	c.Assert(err, IsNil)
	c.Assert(m, Not(Equals), nil)
	c.Assert(len(m), Equals, 3) // 1 blob + 2 snap

	// N.B. - GetBlobMetadata() returns lowercases keys
	c.Assert(m["blobmetakey1"], Equals, "blobMetaValue1")
	c.Assert(m["snapid"], Equals, snapid)
	c.Assert(m["snapmetakey1"], Equals, "snapMetaValue1")

	// snapSetMeta := map[string]string{
	// 	"snapAddKey1": "snapAddValue1",
	// 	"snapAddKey2": "snapAddValue2",
	// }

	// Can't set snapshot metadata!  Maybe set it on the container
	// err = blobService.SetBlobMetadata(container, snap, snapSetMeta)
	// c.Assert(err, IsNil)

	// m, err = blobService.GetBlobMetadata(container, snap)
	// c.Assert(err, IsNil)
	// c.Check(m, DeepEquals, mPut)

	// // Case munging

	// mPutUpper := map[string]string{
	// 	"Foo":     "different bar",
	// 	"bar_BAZ": "different waz qux",
	// }
	// mExpectLower := map[string]string{
	// 	"foo":     "different bar",
	// 	"bar_baz": "different waz qux",
	// }

	// err = blobService.SetBlobMetadata(container, snap, mPutUpper)
	// c.Assert(err, IsNil)

	// m, err = blobService.GetBlobMetadata(container, snap)
	// c.Assert(err, IsNil)
	// c.Check(m, DeepEquals, mExpectLower)
}

func (s *SnapshotSuite) TestSnapDeleteBlob(c *C) {
	// === Create a snapshot :: Start
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	defer func() {
		err := blobService.DeleteContainer(container)
		c.Check(err, IsNil)
	}()

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))

	// Initialize an empty PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer func() {
		err := blobService.DeleteBlob(container, blob)
		c.Check(err, IsNil)
	}()

	// Add some blob metadata
	blobMeta := map[string]string{
		"blobMetaKey1": "blobMetaValue1",
	}
	err = blobService.SetBlobMetadata(container, blob, blobMeta)

	// Write body to existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Prepare some metadata - N.B. Keys get camelCased
	snapid := time.Now().Format(time.RFC3339Nano)
	snapMeta := storage.Metadata{
		"snapid":       snapid,
		"snapMetaKey1": "snapMetaValue1",
	}

	// Do snapshot
	res, err := blobService.Snapshot(container, blob, snapMeta)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	// Retrieve snapshot id from response header
	snaptime := res.Headers.Get("X-Ms-Snapshot")
	snap := blob + "?snapshot=" + snaptime
	defer func() {
		err := blobService.DeleteBlob(container, snap)
		c.Check(err, IsNil)
	}()
	// === Create a snapshot :: End
}

func (s *SnapshotSuite) TestSnapBlobCopy(c *C) {
	if testing.Short() {
		c.Skip("skipping blob copy in short mode, no SLA on async operation")
	}

	// === Create a snapshot :: Start
	blobClient := getBasicClient(c)
	blobService := blobClient.GetBlobService()

	container := randContainer()
	c.Assert(blobService.CreateContainer(container, storage.ContainerAccessTypePrivate), IsNil)
	defer func() {
		err := blobService.DeleteContainer(container)
		c.Check(err, IsNil)
	}()

	blob := randString(20)
	size := (1 * 1024)
	body := []byte(randString(size))

	// Initialize an empty PageBlob
	err := blobService.PutPageBlob(container, blob, int64(size))
	c.Assert(err, IsNil)
	defer func() {
		err := blobService.DeleteBlob(container, blob)
		c.Check(err, IsNil)
	}()

	// Add some blob metadata
	blobMeta := map[string]string{
		"blobMetaKey1": "blobMetaValue1",
	}
	err = blobService.SetBlobMetadata(container, blob, blobMeta)

	// Write body to existing Page Blob
	// N.B. size - 1 : start and stop are indices
	err = blobService.PutPage(container, blob, 0, int64(size-1), storage.PageWriteTypeUpdate, body)
	c.Assert(err, IsNil)

	// Prepare some metadata - N.B. Keys get camelCased
	snapid := time.Now().Format(time.RFC3339Nano)
	snapMeta := storage.Metadata{
		"snapid":       snapid,
		"snapMetaKey1": "snapMetaValue1",
	}

	// Do snapshot
	res, err := blobService.Snapshot(container, blob, snapMeta)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusCreated)

	// Retrieve snapshot id from response header
	snaptime := res.Headers.Get("X-Ms-Snapshot")
	snap := blob + "?snapshot=" + snaptime
	defer func() {
		err := blobService.DeleteBlob(container, snap)
		c.Check(err, IsNil)
	}()
	// === Create a snapshot :: End

	// List blobs
	resList, err := blobService.ListBlobs(container, storage.ListBlobsParameters{Include: "snapshots,metadata"})
	c.Assert(err, IsNil)
	jbytes, err := json.MarshalIndent(resList, "", "  ")
	c.Assert(err, IsNil)
	log.Printf("blobs (%d) = ...\n%v", len(resList.Blobs), string(jbytes))

	// Get Blob metadata
	metadata, err := blobService.GetBlobMetadata(container, snap)
	c.Assert(err, IsNil)
	c.Assert(metadata["snapid"], Equals, snapid)

	// Define a destination blob
	dstBlob := randString(20)

	// Copy the blob
	snapURL := blobService.GetBlobURL(container, snap)
	srcBlobURL := blobService.GetBlobURL(container, blob)
	log.Printf("container = %v", container)
	log.Printf("dstBlob   = %v", dstBlob)
	log.Printf("snapURL   = %v", snapURL)
	log.Printf("srcBlobURL   = %v", srcBlobURL)
	err = blobService.CopyBlob(container, dstBlob, snapURL)
	if err != nil {
		log.Printf("Copy Failed xxxxxxxxx")
	}
	c.Assert(err, IsNil)
	defer blobService.DeleteBlob(container, dstBlob)

	// blobBody, err := blobService.GetBlob(container, dstBlob)
	// c.Assert(err, IsNil)

	// b, err := ioutil.ReadAll(blobBody)
	// defer blobBody.Close()
	// c.Assert(err, IsNil)
	// c.Assert(b, DeepEquals, body)

	// // Check new blob metadata same as snapshot
	// m, err := blobService.GetBlobMetadata(container, dstBlob)
	// c.Assert(err, IsNil)
	// c.Assert(m, Not(Equals), nil)
	// c.Assert(len(m), Equals, 3) // 1 blob + 2 snap

	// // N.B. - GetBlobMetadata() returns lowercases keys
	// c.Assert(m["blobmetakey1"], Equals, "blobMetaValue1")
	// c.Assert(m["snapmetakey1"], Equals, "snapMetaValue1")
}
