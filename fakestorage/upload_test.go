// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
)

func TestServerClientObjectWriter(t *testing.T) {
	const baseContent = "some nice content"
	content := strings.Repeat(baseContent+"\n", googleapi.MinUploadChunkSize)
	u32Checksum := uint32Checksum([]byte(content))
	hash := checksum.MD5Hash([]byte(content))

	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
			chunkSize  int
		}{
			{
				"default chunk size",
				"some-bucket",
				"some/interesting/object.txt",
				googleapi.DefaultUploadChunkSize,
			},
			{
				"small chunk size",
				"other-bucket",
				"other/interesting/object.txt",
				googleapi.MinUploadChunkSize,
			},
			{
				"file with backslash at beginning",
				"other-bucket",
				"/some/other/object.txt",
				googleapi.DefaultUploadChunkSize,
			},
			{
				"file with backslashes at name",
				"other-bucket",
				"//some//other//file.txt",
				googleapi.MinUploadChunkSize,
			},
		}

		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				const contentType = "text/plain; charset=utf-8"
				cacheControl := "public, max-age=3600"
				server.CreateBucketWithOpts(CreateBucketOpts{Name: test.bucketName})
				client := server.Client()

				customTime := time.Now().Truncate(time.Second).Add(-time.Hour)
				objHandle := client.Bucket(test.bucketName).Object(test.objectName)
				w := objHandle.NewWriter(context.Background())
				w.ChunkSize = test.chunkSize
				w.ContentType = contentType
				w.CustomTime = customTime
				w.Metadata = map[string]string{
					"foo": "bar",
				}
				w.CacheControl = cacheControl
				w.Write([]byte(content))
				err := w.Close()
				if err != nil {
					t.Fatal(err)
				}

				obj, err := server.GetObject(test.bucketName, test.objectName)
				if err != nil {
					t.Fatal(err)
				}
				if string(obj.Content) != content {
					n := strings.Count(string(obj.Content), baseContent)
					t.Errorf("wrong content returned\nwant %dx%q\ngot  %dx%q",
						googleapi.MinUploadChunkSize, baseContent,
						n, baseContent)
				}

				if returnedSize := w.Attrs().Size; returnedSize != int64(len(content)) {
					t.Errorf("wrong writer.Attrs() size returned\nwant %d\ngot  %d", len(content), returnedSize)
				}
				if returnedChecksum := w.Attrs().CRC32C; returnedChecksum != u32Checksum {
					t.Errorf("wrong writer.Attrs() checksum returned\nwant %d\ngot  %d", u32Checksum, returnedChecksum)
				}
				if base64Checksum := checksum.EncodedChecksum(uint32ToBytes(u32Checksum)); obj.Crc32c != base64Checksum {
					t.Errorf("wrong obj.Crc32c returned\nwant %s\ngot %s", base64Checksum, obj.Crc32c)
				}
				if returnedHash := w.Attrs().MD5; !bytes.Equal(returnedHash, hash) {
					t.Errorf("wrong writer.Attrs() hash returned\nwant %d\ngot  %d", hash, returnedHash)
				}
				if stringHash := checksum.EncodedHash(hash); obj.Md5Hash != stringHash {
					t.Errorf("wrong obj.Md5Hash returned\nwant %s\ngot %s", stringHash, obj.Md5Hash)
				}
				if obj.ContentType != contentType {
					t.Errorf("wrong content-type\nwant %q\ngot  %q", contentType, obj.ContentType)
				}
				if !reflect.DeepEqual(obj.Metadata, w.Metadata) {
					t.Errorf("wrong meta data\nwant %+v\ngot  %+v", w.Metadata, obj.Metadata)
				}
				if !customTime.Equal(obj.CustomTime) {
					t.Errorf("wrong custom time\nwant %q\ngot  %q", customTime.String(), obj.CustomTime.String())
				}
				if obj.CacheControl != cacheControl {
					t.Errorf("wrong cache control\nwant %q\ngot  %q", cacheControl, obj.CacheControl)
				}

				reader, err := client.Bucket(test.bucketName).Object(test.objectName).NewReader(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != content {
					n := strings.Count(string(obj.Content), baseContent)
					t.Errorf("wrong content returned via object reader\nwant %dx%q\ngot  %dx%q",
						googleapi.MinUploadChunkSize, baseContent,
						n, baseContent)
				}
			})
		}
	})
}

func checkChecksum(t *testing.T, content []byte, obj Object) {
	t.Helper()
	if expect := checksum.EncodedCrc32cChecksum(content); expect != obj.Crc32c {
		t.Errorf("wrong checksum in the object\nwant %s\ngot  %s", expect, obj.Crc32c)
	}
}

func TestServerClientObjectWriterOverwrite(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		const content = "other content"
		const contentType = "text/plain"
		const cacheControl = "no-cache"
		server.CreateObject(Object{
			ObjectAttrs: ObjectAttrs{
				BucketName:  "some-bucket",
				Name:        "some-object.txt",
				ContentType: "some-stff",
			},
			Content: []byte("some content"),
		})
		objHandle := server.Client().Bucket("some-bucket").Object("some-object.txt")
		w := objHandle.NewWriter(context.Background())
		w.ContentType = contentType
		w.CacheControl = cacheControl
		w.Write([]byte(content))
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		obj, err := server.GetObject("some-bucket", "some-object.txt")
		if err != nil {
			t.Fatal(err)
		}
		if string(obj.Content) != content {
			t.Errorf("wrong content in the object\nwant %q\ngot  %q", content, string(obj.Content))
		}
		checkChecksum(t, []byte(content), obj)
		if obj.ContentType != contentType {
			t.Errorf("wrong content-type\nwsant %q\ngot  %q", contentType, obj.ContentType)
		}
		if obj.CacheControl != cacheControl {
			t.Errorf("wrong cache control\nwant %q\ngot  %q", cacheControl, obj.CacheControl)
		}
	})
}

func TestServerClientObjectWriterWithDoesNotExistPrecondition(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		const originalContent = "original content"
		const originalContentType = "text/plain"
		const bucketName = "some-bucket"
		const objectName = "some-object-2.txt"

		bucket := server.Client().Bucket(bucketName)
		if err := bucket.Create(context.Background(), "my-project", nil); err != nil {
			t.Fatal(err)
		}

		objHandle := bucket.Object(objectName)

		firstWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		firstWriter.ContentType = originalContentType
		firstWriter.Write([]byte(originalContent))
		if err := firstWriter.Close(); err != nil {
			t.Fatal(err)
		}

		firstReader, err := objHandle.NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContent, err := io.ReadAll(firstReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContent) != originalContent {
			t.Errorf("wrong content in the object after initial write with precondition\nwant %q\ngot  %q", originalContent, string(objectContent))
		}

		secondWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		secondWriter.ContentType = "application/json"
		secondWriter.Write([]byte("new content"))
		err = secondWriter.Close()
		if err == nil {
			t.Fatal("expected overwriting existing object to fail, but received no error")
		}
		if err.Error() != "googleapi: Error 412: Precondition failed, Precondition Failed" {
			t.Errorf("expected HTTP 412 precondition failed error, but got %v", err)
		}

		secondReader, err := objHandle.NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContentAfterFailedPrecondition, err := io.ReadAll(secondReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContentAfterFailedPrecondition) != originalContent {
			t.Errorf("wrong content in the object after failed precondition\nwant %q\ngot  %q", originalContent, string(objectContentAfterFailedPrecondition))
		}
	})
}

func TestServerClientObjectOperationsWithIfGenerationMatchPrecondition(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		const (
			originalContent     = "original content"
			newContent          = "new content"
			originalContentType = "text/plain"
			bucketName          = "some-bucket"
			objectName          = "some-object-2.txt"
		)

		bucket := server.Client().Bucket(bucketName)
		if err := bucket.Create(context.Background(), "my-project", nil); err != nil {
			t.Fatal(err)
		}

		objHandle := bucket.Object(objectName)

		firstWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		firstWriter.ContentType = originalContentType
		firstWriter.Write([]byte(originalContent))
		if err := firstWriter.Close(); err != nil {
			t.Fatal(err)
		}
		gen := firstWriter.Attrs().Generation

		firstReader, err := objHandle.If(storage.Conditions{GenerationMatch: gen}).NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContent, err := io.ReadAll(firstReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContent) != originalContent {
			t.Errorf("wrong content in the object after initial write with precondition\nwant %q\ngot  %q", originalContent, string(objectContent))
		}

		secondWriter := objHandle.If(storage.Conditions{GenerationMatch: gen}).NewWriter(context.Background())
		secondWriter.ContentType = "application/json"
		secondWriter.Write([]byte(newContent))
		err = secondWriter.Close()
		if err != nil {
			t.Fatal(err)
		}
		gen = secondWriter.Attrs().Generation

		secondReader, err := objHandle.If(storage.Conditions{GenerationMatch: gen}).NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContentAfterMatchedPrecondition, err := io.ReadAll(secondReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContentAfterMatchedPrecondition) != newContent {
			t.Errorf("wrong content in the object after matched precondition\nwant %q\ngot  %q", newContent, string(objectContentAfterMatchedPrecondition))
		}
	})
}

func TestServerClientObjectOperationsWithIfGenerationNotMatchPrecondition(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		const (
			originalContent     = "original content"
			newContent          = "new content"
			originalContentType = "text/plain"
			bucketName          = "some-bucket"
			objectName          = "some-object-2.txt"
		)

		bucket := server.Client().Bucket(bucketName)
		if err := bucket.Create(context.Background(), "my-project", nil); err != nil {
			t.Fatal(err)
		}

		objHandle := bucket.Object(objectName)

		firstWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		firstWriter.ContentType = originalContentType
		firstWriter.Write([]byte(originalContent))
		if err := firstWriter.Close(); err != nil {
			t.Fatal(err)
		}
		gen := firstWriter.Attrs().Generation

		firstReader, err := objHandle.If(storage.Conditions{GenerationMatch: gen}).NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContent, err := io.ReadAll(firstReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContent) != originalContent {
			t.Errorf("wrong content in the object after initial write with precondition\nwant %q\ngot  %q", originalContent, string(objectContent))
		}

		secondWriter := objHandle.If(storage.Conditions{GenerationNotMatch: gen}).NewWriter(context.Background())
		secondWriter.ContentType = "application/json"
		secondWriter.Write([]byte(newContent))
		err = secondWriter.Close()
		if err == nil {
			t.Fatal("expected overwriting existing object to fail, but received no error")
		}
		if err.Error() != "googleapi: Error 412: Precondition failed, Precondition Failed" {
			t.Errorf("expected HTTP 412 precondition failed error, but got %v", err)
		}

		secondReader, err := objHandle.NewReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		objectContentAfterFailedPrecondition, err := io.ReadAll(secondReader)
		if err != nil {
			t.Fatal(err)
		}
		if string(objectContentAfterFailedPrecondition) != originalContent {
			t.Errorf("wrong content in the object after failed precondition\nwant %q\ngot  %q", originalContent, string(objectContentAfterFailedPrecondition))
		}
	})
}

func TestServerClientObjectOperationsFailureToWriteExistingObject(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const (
			originalContent     = "original content"
			newContent          = "new content"
			originalContentType = "text/plain"
			bucketName          = "some-bucket"
			objectName          = "some-object.txt"
		)

		bucket := server.Client().Bucket(bucketName)
		if err := bucket.Create(context.Background(), "my-project", nil); err != nil {
			t.Fatal(err)
		}

		objHandle := bucket.Object(objectName)

		firstWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		firstWriter.ContentType = originalContentType
		firstWriter.Write([]byte(originalContent))
		if err := firstWriter.Close(); err != nil {
			t.Fatal(err)
		}

		secondWriter := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		secondWriter.ContentType = "application/json"
		secondWriter.Write([]byte(newContent))
		err := secondWriter.Close()
		if err == nil {
			t.Fatal("expected overwriting existing object to fail, but received no error")
		}
	})
}

func TestServerClientUploadRacesAreOnlyWonByOne(t *testing.T) {
	const (
		bucketName  = "some-bucket"
		repetitions = 10
		parallelism = 5
	)

	type workerResult struct {
		success bool
		worker  uint16
	}

	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		bucket := server.Client().Bucket(bucketName)
		if err := bucket.Create(context.Background(), "my-project", nil); err != nil {
			t.Fatal(err)
		}

		// Repeat test to increase chance of detecting race
		for i := 0; i < repetitions; i++ {
			objHandle := bucket.Object(fmt.Sprintf("object-%d.bin", i))

			results := make(chan workerResult)
			for j := uint16(0); j < parallelism; j++ {
				go func(workerIndex uint16) {
					writer := objHandle.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
					buf := make([]byte, 2)
					binary.BigEndian.PutUint16(buf, workerIndex)
					writer.Write(buf)
					results <- workerResult{success: writer.Close() == nil, worker: workerIndex}
				}(j)
			}

			var successes int
			var failures int
			var winner uint16
			for j := 0; j < parallelism; j++ {
				result := <-results
				if result.success {
					successes++
					winner = result.worker
				} else {
					failures++
				}
			}

			if successes != 1 {
				t.Errorf("in attempt %d, expected 1 success but got %d", i, successes)
			}
			if failures != parallelism-1 {
				t.Errorf("in attempt %d, expected %d failures but got %d", i, parallelism-1, failures)
			}
			reader, err := objHandle.NewReader(context.Background())
			if err != nil {
				t.Errorf("in attempt %d, readback failed with %#v", i, err)
			}
			buf := make([]byte, 2)
			l, err := reader.Read(buf)
			if err != nil {
				t.Errorf("in attempt %d, readback.read failed with %#v", i, err)
			}
			if l != 2 {
				t.Errorf("in attempt %d, insufficient bytes read", i)
			}
			if winner != binary.BigEndian.Uint16(buf) {
				t.Errorf("in attempt %d, %d were told as winner, but %d actually stored", i, winner, binary.BigEndian.Uint16(buf))
			}
		}
	})
}

func TestServerClientObjectWriterBucketNotFound(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		client := server.Client()
		objHandle := client.Bucket("some-bucket").Object("some/interesting/object.txt")
		w := objHandle.NewWriter(context.Background())
		w.Write([]byte("whatever"))
		err := w.Close()
		if err == nil {
			t.Fatal("unexpected <nil> error")
		}
	})
}

func TestServerClientSimpleUpload(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})

	const data = "some nice content"
	const contentType = "text/plain"
	req, err := http.NewRequest("POST", server.URL()+"/upload/storage/v1/b/other-bucket/o?uploadType=media&name=some/nice/object.txt", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expectedStatus := http.StatusOK
	if resp.StatusCode != expectedStatus {
		t.Errorf("wrong status code\nwant %d\ngot  %d", expectedStatus, resp.StatusCode)
	}

	obj, err := server.GetObject("other-bucket", "some/nice/object.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Content) != data {
		t.Errorf("wrong content\nwant %q\ngot  %q", string(obj.Content), data)
	}
	if obj.ContentType != contentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
	}
	checkChecksum(t, []byte(data), obj)
}

func TestServerClientSignedUpload(t *testing.T) {
	server, err := NewServerWithOptions(Options{PublicHost: "127.0.0.1"})
	if err != nil {
		t.Fatalf("could not start server: %v", err)
	}
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})
	const data = "some nice content"
	const contentType = "text/plain"
	req, err := http.NewRequest("PUT", server.URL()+"/other-bucket/some/nice/object.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Goog-Meta-Key", "Value")
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expectedStatus := http.StatusOK
	if resp.StatusCode != expectedStatus {
		t.Errorf("wrong status code\nwant %d\ngot  %d", expectedStatus, resp.StatusCode)
	}

	obj, err := server.GetObject("other-bucket", "some/nice/object.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Content) != data {
		t.Errorf("wrong content\nwant %q\ngot  %q", string(obj.Content), data)
	}
	if obj.ContentType != contentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
	}
	if want := map[string]string{"key": "Value"}; !reflect.DeepEqual(obj.Metadata, want) {
		t.Errorf("wrong metadata\nwant %q\ngot  %q", want, obj.Metadata)
	}
	checkChecksum(t, []byte(data), obj)
}

func TestServerClientSignedUploadBucketCNAME(t *testing.T) {
	url := "https://mybucket.mydomain.com:4443/files/txt/text-02.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=fake-gcs&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=fake-gc"
	expectedName := "files/txt/text-02.txt"
	expectedContentType := "text/plain"
	expectedHash := "bHupxaFBQh4cA8uYB8l8dA=="
	opts := Options{
		InitialObjects: []Object{
			{ObjectAttrs: ObjectAttrs{BucketName: "mybucket.mydomain.com", Name: "files/txt/text-01.txt"}, Content: []byte("something")},
		},
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	client := server.HTTPClient()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader("something else"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var obj Object
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if obj.Name != expectedName {
		t.Errorf("wrong filename\nwant %q\ngot  %q", expectedName, obj.Name)
	}
	if obj.ContentType != expectedContentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", expectedContentType, obj.ContentType)
	}
	if obj.Md5Hash != expectedHash {
		t.Errorf("wrong md5 hash\nwant %q\ngot  %q", expectedHash, obj.Md5Hash)
	}
}

func TestServerClientUploadWithPredefinedAclPublicRead(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})

	const contentType = "text/plain"
	const contentEncoding = "gzip"

	const data = "some nice content"
	// store the data compressed with gzip
	buf := &bytes.Buffer{}
	gzw := gzip.NewWriter(buf)
	if _, err := gzw.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	compressed := buf.Bytes()

	req, err := http.NewRequest("POST", server.URL()+"/upload/storage/v1/b/other-bucket/o?predefinedAcl=publicRead&uploadType=media&name=some/nice/object.txt&contentEncoding="+contentEncoding, bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expectedStatus := http.StatusOK
	if resp.StatusCode != expectedStatus {
		t.Errorf("wrong status code\nwant %d\ngot  %d", expectedStatus, resp.StatusCode)
	}

	obj, err := server.GetObject("other-bucket", "some/nice/object.txt")
	if err != nil {
		t.Fatal(err)
	}

	attrs, err := server.Client().Bucket("other-bucket").Object("some/nice/object.txt").Attrs(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if attrs.ContentEncoding != contentEncoding {
		t.Errorf("wrong contentEncoding\nwant %q\ngot %q", contentEncoding, attrs.ContentEncoding)
	}

	acl, err := server.Client().Bucket("other-bucket").Object("some/nice/object.txt").ACL().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !isACLPublic(acl) {
		t.Errorf("wrong acl\ngot %+v", acl)
	}
	if string(obj.Content) != string(compressed) {
		t.Errorf("wrong content\nwant %q\ngot  %q", string(obj.Content), string(compressed))
	}
	if obj.ContentType != contentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
	}
	checkChecksum(t, compressed, obj)
}

func TestServerClientSimpleUploadNoName(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})

	const data = "some nice content"
	req, err := http.NewRequest("POST", server.URL()+"/upload/storage/v1/b/other-bucket/o?uploadType=media", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expectedStatus := http.StatusBadRequest
	if resp.StatusCode != expectedStatus {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", expectedStatus, resp.StatusCode)
	}
}

func TestServerInvalidUploadType(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})
	const data = "some nice content"
	req, err := http.NewRequest("POST", server.URL()+"/upload/storage/v1/b/other-bucket/o?uploadType=bananas&name=some-object.txt", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expectedStatus := http.StatusBadRequest
	if resp.StatusCode != expectedStatus {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", expectedStatus, resp.StatusCode)
	}
}

func TestParseContentRange(t *testing.T) {
	t.Parallel()
	goodHeaderTests := []struct {
		header string
		output contentRange
	}{
		{
			"bytes */1024", // End of a streaming request, total is now known
			contentRange{KnownTotal: true, Start: -1, End: -1, Total: 1024},
		},
		{
			"bytes 0-*/*", // Start and end of a streaming request as done by nodeJS client lib
			contentRange{KnownTotal: false, Start: 0, End: -1, Total: -1},
		},
		{
			"bytes 1024-2047/4096", // Range with a known total
			contentRange{KnownRange: true, KnownTotal: true, Start: 1024, End: 2047, Total: 4096},
		},
		{
			"bytes 0-1024/*", // A streaming request, unknown total
			contentRange{KnownRange: true, Start: 0, End: 1024, Total: -1},
		},
		{
			"bytes */*", // Start and end of a streaming request as sent by the C++ SDK
			contentRange{KnownRange: false, KnownTotal: false, Start: -1, End: -1, Total: -1},
		},
	}

	for _, test := range goodHeaderTests {
		test := test
		t.Run(test.header, func(t *testing.T) {
			t.Parallel()
			output, err := parseContentRange(test.header)
			if err != nil {
				t.Fatal(err)
			}
			if output != test.output {
				t.Fatalf("output is different.\nexpected: %+v\n  actual: %+v\n", test.output, output)
			}
		})
	}

	badHeaderTests := []string{
		"none",                // Unsupported unit "none"
		"bytes 20",            // No slash to split range from size
		"bytes 1/4",           // Single-field range
		"bytes start-20/100",  // Non-integer range start
		"bytes 20-end/100",    // Non-integer range end
		"bytes 100-200/total", // Non-integer size
	}
	for _, test := range badHeaderTests {
		test := test
		t.Run(test, func(t *testing.T) {
			t.Parallel()
			_, err := parseContentRange(test)
			if err == nil {
				t.Fatalf("Expected err!=<nil>, but was %v", err)
			}
		})
	}
}

func resumableUploadTest(t *testing.T, server *Server, bucketName string, uploadRequestBody *strings.Reader) {
	server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})

	client := server.HTTPClient()

	url := server.URL()
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/upload/storage/v1/b/%s/o?name=testobj", url, bucketName), uploadRequestBody)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Goog-Upload-Protocol", "resumable")
	req.Header.Set("X-Goog-Upload-Command", "start")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		t.Errorf("expected a 200 response, got: %d", resp.StatusCode)
	}

	if hdr := resp.Header.Get("X-Goog-Upload-Status"); hdr != "active" {
		t.Errorf("X-Goog-Upload-Status response header expected 'active' got: %s", hdr)
	}

	uploadURL := resp.Header.Get("X-Goog-Upload-URL")
	if uploadURL == "" {
		t.Error("X-Goog-Upload-URL did not return upload url")
	}

	body := strings.NewReader("{\"test\": \"foo\"}")
	req, err = http.NewRequest(http.MethodPost, uploadURL, body)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Goog-Upload-Command", "upload, finalize")

	resp2, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp2.Body)
		_ = resp2.Body.Close()
	}()

	if resp2.StatusCode != 200 {
		t.Errorf("expected a 200 response, got: %d", resp2.StatusCode)
	}

	if hdr := resp2.Header.Get("X-Goog-Upload-Status"); hdr != "final" {
		t.Errorf("X-Goog-Upload-Status response header expected 'final' got: %s", hdr)
	}
}

// this is to support the Ruby SDK.
func TestServerUndocumentedResumableUploadAPI(t *testing.T) {
	bucketName := "testbucket"

	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		t.Run("test headers", func(t *testing.T) {
			resumableUploadTest(t, server, bucketName, strings.NewReader("{\"contentType\": \"application/json\"}"))
		})
		t.Run("test headers without initial body", func(t *testing.T) {
			resumableUploadTest(t, server, bucketName, strings.NewReader(""))
		})
	})
}

// this is to support the Java SDK.
func TestServerGzippedUpload(t *testing.T) {
	const bucketName = "testbucket"

	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		t.Run("test headers", func(t *testing.T) {
			server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})

			client := server.HTTPClient()

			var buf bytes.Buffer
			const content = "some interesting content perhaps?"
			writer := gzip.NewWriter(&buf)
			_, err := writer.Write([]byte(content))
			if err != nil {
				t.Fatal(err)
			}
			err = writer.Close()
			if err != nil {
				t.Fatal(err)
			}

			serverUrl := server.URL()
			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/upload/storage/v1/b/%s/o?name=testobj&uploadType=media", serverUrl, bucketName), &buf)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Encoding", "gzip")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			if resp.StatusCode != 200 {
				t.Errorf("expected a 200 response, got: %d", resp.StatusCode)
			}

			obj, err := server.GetObject(bucketName, "testobj")
			if err != nil {
				t.Fatal(err)
			}

			if string(obj.Content) != content {
				t.Errorf("wrong content\nwant %q\ngot  %q", content, obj.Content)
			}
		})
	})
}

func TestFormDataUpload(t *testing.T) {
	server, err := NewServerWithOptions(Options{PublicHost: "127.0.0.1"})
	if err != nil {
		t.Fatalf("could not start server: %v", err)
	}
	defer server.Stop()
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "other-bucket"})

	var buf bytes.Buffer
	const content = "some weird content"
	const contentType = "text/plain"
	successActionStatus := http.StatusNoContent
	writer := multipart.NewWriter(&buf)

	var fieldWriter io.Writer
	if fieldWriter, err = writer.CreateFormField("key"); err != nil {
		t.Fatal(err)
	}
	if _, err := fieldWriter.Write([]byte("object.txt")); err != nil {
		t.Fatal(err)
	}

	if fieldWriter, err = writer.CreateFormField("Content-Type"); err != nil {
		t.Fatal(err)
	}
	if _, err := fieldWriter.Write([]byte(contentType)); err != nil {
		t.Fatal(err)
	}

	if fieldWriter, err = writer.CreateFormField("success_action_status"); err != nil {
		t.Fatal(err)
	}
	if _, err := fieldWriter.Write([]byte(strconv.Itoa(successActionStatus))); err != nil {
		t.Fatal(err)
	}

	if fieldWriter, err = writer.CreateFormField("x-goog-meta-key"); err != nil {
		t.Fatal(err)
	}
	if _, err := fieldWriter.Write([]byte("Value")); err != nil {
		t.Fatal(err)
	}

	if fieldWriter, err = writer.CreateFormFile("file", "object.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := fieldWriter.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("POST", server.URL()+"/other-bucket", &buf)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != successActionStatus {
		t.Errorf("wrong status code\nwant %d\ngot  %d", successActionStatus, resp.StatusCode)
	}

	obj, err := server.GetObject("other-bucket", "object.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Content) != content {
		t.Errorf("wrong content\nwant %q\ngot  %q", string(obj.Content), content)
	}
	if obj.ContentType != contentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
	}
	if want := map[string]string{"key": "Value"}; !reflect.DeepEqual(obj.Metadata, want) {
		t.Errorf("wrong metadata\nwant %q\ngot  %q", want, obj.Metadata)
	}
	checkChecksum(t, []byte(content), obj)
}

func isACLPublic(acl []storage.ACLRule) bool {
	for _, entry := range acl {
		if entry.Entity == storage.AllUsers && entry.Role == storage.RoleReader {
			return true
		}
	}
	return false
}

func TestParseContentTypeParams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		input          string
		expectedParams map[string]string
	}{
		{
			name:           "no boundary",
			input:          "multipart/related",
			expectedParams: map[string]string{},
		},
		{
			name:           "with boundary",
			input:          "multipart/related; boundary=something",
			expectedParams: map[string]string{"boundary": "something"},
		},
		{
			name:           "with quoted boundary",
			input:          `multipart/related; boundary="something"`,
			expectedParams: map[string]string{"boundary": "something"},
		},
		{
			name:           "boundaries that have a single quote, but don't use special chars",
			input:          `multipart/related; boundary='something'`,
			expectedParams: map[string]string{"boundary": "'something'"},
		},
		{
			name:           "special characters within single quotes",
			input:          `text/plain; boundary='===============1523364337061494617=='`,
			expectedParams: map[string]string{"boundary": "===============1523364337061494617=="},
		},
		{
			name:           "special characters within single quotes + other parameters",
			input:          `text/plain; boundary='===============1523364337061494617=='; some=thing`,
			expectedParams: map[string]string{"boundary": "===============1523364337061494617==", "some": "thing"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			params, err := parseContentTypeParams(test.input)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(params, test.expectedParams); diff != "" {
				t.Errorf("unexpected params: %v", diff)
			}
		})
	}
}

func TestParseContentTypeParamsGsutilEdgeCases(t *testing.T) {
	t.Parallel()

	// "hardening" of the regex, to make sure we don't miss anything. As we
	// run into bugs, this slice will grow.
	testCases := []string{
		"===============5900997287163282353==",
		"===============5900997287163282353",
		"590099728(7163282353",
		"something with spaces",
		"                ",
		"===============5900997287163282353==590099728===",
		"(",
		")",
		"<",
		">",
		"@",
		",",
		";",
		":",
		"/",
		"[",
		"]",
		"?",
		"=",
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase, func(t *testing.T) {
			t.Parallel()
			input := fmt.Sprintf("text/plain; a=b; boundary='%s'; c=d", testCase)
			expectedParams := map[string]string{"boundary": testCase, "a": "b", "c": "d"}

			params, err := parseContentTypeParams(input)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(params, expectedParams); diff != "" {
				t.Errorf("unexpected params: %v", diff)
			}
		})
	}
}
