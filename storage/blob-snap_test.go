package storage

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	chk "azure-sdk-for-go/Godeps/_workspace/src/gopkg.in/check.v1"
)

type ClientJ struct {
	AccountName string
	AccountKey  []byte
	UseHTTPS    bool
	BaseURL     string
	ApiVersion  string
}

func cliToJson(c BlobStorageClient) string {
	c2 := ClientJ{
		AccountName: c.client.accountName,
		AccountKey:  c.client.accountKey,
		UseHTTPS:    c.client.useHTTPS,
		BaseURL:     c.client.baseURL,
		ApiVersion:  c.client.apiVersion,
	}
	txt, _ := json.MarshalIndent(c2, "", "  ")
	return string(txt)
}

func (s *StorageBlobSuite) TestBlobSASURICorrectness1(c *chk.C) {
	cli := getBlobClient(c)
	cnt := randContainer()
	blob := randString(20)
	body := []byte(randString(100))
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	c.Assert(cli.CreateContainer(cnt, ContainerAccessTypePrivate), chk.IsNil)
	//defer cli.DeleteContainer(cnt)

	c.Assert(cli.putSingleBlockBlob(cnt, blob, body), chk.IsNil)

	sasURI, err := cli.GetBlobSASURI(cnt, blob, expiry, permissions)
	c.Assert(err, chk.IsNil)

	resp, err := http.Get(sasURI)
	c.Assert(err, chk.IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, chk.IsNil)

	c.Assert(resp.StatusCode, chk.Equals, http.StatusOK)
	c.Assert(len(blobResp), chk.Equals, len(body))

	log.Printf("cli    = %T - ...\n%s", cli, cliToJson(cli))
	log.Printf("cnt    = %T - %v", cnt, cnt)
	log.Printf("blob   = %T - %v", blob, blob)
	log.Printf("body   = %T - %s", body, string(body))
	log.Printf("expiry = %T - %v", expiry, expiry)
	log.Printf("permissions   = %T - %s", permissions, permissions)
	log.Printf("sasURI   = %T - %s", sasURI, sasURI)
	log.Printf("blobResp = %T - %v", blobResp, string(blobResp))
}

func (s *StorageBlobSuite) TestBlobSASURICorrectness2(c *chk.C) {
	cli := getBlobClient(c)

	cnt := `zzzztest-oyf8qoya3sws9pj9eimopei`
	blob := `k73nbr4x70ytaapo0lvn`
	expiry := time.Now().UTC().Add(time.Hour)
	permissions := "r"

	sasURI, err := cli.GetBlobSASURI(cnt, blob, expiry, permissions)
	c.Assert(err, chk.IsNil)

	resp, err := http.Get(sasURI)
	c.Assert(err, chk.IsNil)

	blobResp, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode, chk.Equals, http.StatusOK)

	log.Printf("cli    = %T - ...\n%s", cli, cliToJson(cli))
	log.Printf("cnt    = %T - %v", cnt, cnt)
	log.Printf("blob   = %T - %v", blob, blob)
	log.Printf("expiry = %T - %v", expiry, expiry)
	log.Printf("permissions   = %T - %s", permissions, permissions)
	log.Printf("sasURI   = %T - %s", sasURI, sasURI)
	log.Printf("blobResp = %T - %v", blobResp, string(blobResp))
}

//func (s *StorageBlobSuite) TestPutPageBlob(c *chk.C) {
func (s *StorageBlobSuite) TestSnapshotBlob(c *chk.C) {
	cli := getBlobClient(c)
	cnt := randContainer()
	c.Assert(cli.CreateContainer(cnt, ContainerAccessTypePrivate), chk.IsNil)
	defer cli.deleteContainer(cnt)

	blob := randString(20)
	size := int64(10 * 1024 * 1024)
	c.Assert(cli.PutPageBlob(cnt, blob, size), chk.IsNil)

	// Verify
	props, err := cli.GetBlobProperties(cnt, blob)
	c.Assert(err, chk.IsNil)
	c.Assert(props.ContentLength, chk.Equals, size)
	c.Assert(props.BlobType, chk.Equals, BlobTypePage)

	// Snapshot
	err = cli.SnapshotBlob(cnt, blob)
	c.Assert(err, chk.IsNil)
}
