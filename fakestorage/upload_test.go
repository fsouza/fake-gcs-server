// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

func TestServerClientObjectWriter(t *testing.T) {
	const baseContent = "some nice content"
	content := strings.Repeat(baseContent+"\n", googleapi.MinUploadChunkSize)
	checksum := uint32Checksum([]byte(content))
	hash := md5Hash([]byte(content))

	runServersTest(t, nil, func(t *testing.T, server *Server) {
		var tests = []struct {
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
		}

		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				server.CreateBucket(test.bucketName)
				client := server.Client()

				objHandle := client.Bucket(test.bucketName).Object(test.objectName)
				w := objHandle.NewWriter(context.Background())
				w.ChunkSize = test.chunkSize
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

				if returnedChecksum := w.Attrs().CRC32C; returnedChecksum != checksum {
					t.Errorf("wrong writer.Attrs() checksum returned\nwant %d\ngot  %d", checksum, returnedChecksum)
				}
				if base64Checksum := encodedChecksum(uint32ToBytes(checksum)); obj.Crc32c != base64Checksum {
					t.Errorf("wrong obj.Crc32c returned\nwant %s\ngot %s", base64Checksum, obj.Crc32c)
				}
				if returnedHash := w.Attrs().MD5; !bytes.Equal(returnedHash, hash) {
					t.Errorf("wrong writer.Attrs() hash returned\nwant %d\ngot  %d", hash, returnedHash)
				}
				if stringHash := encodedHash(hash); obj.Md5Hash != stringHash {
					t.Errorf("wrong obj.Md5Hash returned\nwant %s\ngot %s", stringHash, obj.Md5Hash)
				}
			})
		}
	})
}

func checkChecksum(t *testing.T, content []byte, obj Object) {
	t.Helper()
	if expect := encodedCrc32cChecksum(content); expect != obj.Crc32c {
		t.Errorf("wrong checksum in the object\nwant %s\ngot  %s", expect, obj.Crc32c)
	}
}

func TestServerClientObjectWriterOverwrite(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
		const content = "other content"
		server.CreateObject(Object{
			BucketName: "some-bucket",
			Name:       "some-object.txt",
			Content:    []byte("some content"),
		})
		objHandle := server.Client().Bucket("some-bucket").Object("some-object.txt")
		w := objHandle.NewWriter(context.Background())
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
	})
}

func TestServerClientObjectWriterBucketNotFound(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
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
	server.CreateBucket("other-bucket")

	const data = "some nice content"
	req, err := http.NewRequest("POST", server.URL()+"/storage/v1/b/other-bucket/o?uploadType=media&name=some/nice/object.txt", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			// #nosec
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
	checkChecksum(t, []byte(data), obj)
}

func TestServerClientSimpleUploadNoName(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	server.CreateBucket("other-bucket")

	const data = "some nice content"
	req, err := http.NewRequest("POST", server.URL()+"/storage/v1/b/other-bucket/o?uploadType=media", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			// #nosec
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
	server.CreateBucket("other-bucket")
	const data = "some nice content"
	req, err := http.NewRequest("POST", server.URL()+"/storage/v1/b/other-bucket/o?uploadType=bananas&name=some-object.txt", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			// #nosec
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
