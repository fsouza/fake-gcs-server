// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
	"google.golang.org/api/iterator"
)

func uint32ToBytes(ui uint32) []byte {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, ui)
	return res
}

func uint32Checksum(b []byte) uint32 {
	checksummer := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	checksummer.Write(b)
	return checksummer.Sum32()
}

type objectTestCases []struct {
	testCase string
	obj      Object
}

func getObjectTestCases() objectTestCases {
	const (
		bucketName      = "some-bucket"
		content         = "some nice content"
		contentType     = "text/plain; charset=utf-8"
		contentEncoding = "gzip"
		metaValue       = "MetaValue"
	)
	testInitExecTime := time.Now().Truncate(time.Microsecond)
	u32Checksum := uint32Checksum([]byte(content))
	hash := checksum.MD5Hash([]byte(content))

	tests := objectTestCases{
		{
			"object but no creation nor modification date",
			Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: checksum.EncodedChecksum(uint32ToBytes(u32Checksum)), Md5Hash: checksum.EncodedHash(hash)},
		},
		{
			"object with creation and modification dates",
			Object{BucketName: bucketName, Name: "img/low-res/party-02.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: checksum.EncodedChecksum(uint32ToBytes(u32Checksum)), Md5Hash: checksum.EncodedHash(hash), Created: testInitExecTime, Updated: testInitExecTime},
		},
		{
			"object with creation, modification dates and generation",
			Object{BucketName: bucketName, Name: "img/low-res/party-02.jpg", Content: []byte(content), ContentType: contentType, Crc32c: checksum.EncodedChecksum(uint32ToBytes(u32Checksum)), Md5Hash: checksum.EncodedHash(hash), Created: testInitExecTime, Updated: testInitExecTime, Generation: testInitExecTime.UnixNano()},
		},
		{
			"object with everything",
			Object{BucketName: bucketName, Name: "img/location/meta.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: checksum.EncodedChecksum(uint32ToBytes(u32Checksum)), Md5Hash: checksum.EncodedHash(hash), Metadata: map[string]string{"MetaHeader": metaValue}},
		},
		{
			"object with no contents neither dates",
			Object{BucketName: bucketName, Name: "video/hi-res/best_video_1080p.mp4", ContentType: "text/html; charset=utf-8"},
		},
	}
	return tests
}

func checkObjectAttrs(testObj Object, attrs *storage.ObjectAttrs, t *testing.T) {
	if attrs == nil {
		t.Fatalf("unexpected nil attrs")

		// This exists to deal with a false-positive reported by
		// staticcheck where it can't understand that t.Fatalf will end
		// execution and dereferencing attrs is safe.
		return
	}
	if attrs.Bucket != testObj.BucketName {
		t.Errorf("wrong bucket name\nwant %q\ngot  %q", testObj.BucketName, attrs.Bucket)
	}
	if attrs.Name != testObj.Name {
		t.Errorf("wrong object name\nwant %q\ngot  %q", testObj.Name, attrs.Name)
	}
	if !(testObj.Created.IsZero()) && !testObj.Created.Equal(attrs.Created) {
		t.Errorf("wrong created date\nwant %v\ngot   %v\nname %v", testObj.Created, attrs.Created, attrs.Name)
	}
	if !(testObj.Updated.IsZero()) && !testObj.Updated.Equal(attrs.Updated) {
		t.Errorf("wrong updated date\nwant %v\ngot   %v\nname %v", testObj.Updated, attrs.Updated, attrs.Name)
	}
	if testObj.Created.IsZero() && attrs.Created.IsZero() {
		t.Errorf("wrong created date\nwant non zero, got   %v\nname %v", attrs.Created, attrs.Name)
	}
	if testObj.Updated.IsZero() && attrs.Updated.IsZero() {
		t.Errorf("wrong updated date\nwant non zero, got   %v\nname %v", attrs.Updated, attrs.Name)
	}
	if testObj.Generation != 0 && attrs.Generation != testObj.Generation {
		t.Errorf("wrong generation\nwant %d\ngot   %d\nname %v", testObj.Generation, attrs.Generation, attrs.Name)
	}
	if testObj.Generation == 0 && attrs.Generation == 0 {
		t.Errorf("generation value is zero")
	}
	if attrs.ContentType != testObj.ContentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", testObj.ContentType, attrs.ContentType)
	}
	if attrs.ContentEncoding != testObj.ContentEncoding {
		t.Errorf("wrong content encoding\nwant %q\ngot  %q", testObj.ContentEncoding, attrs.ContentEncoding)
	}
	if testObj.Content != nil && attrs.Size != int64(len(testObj.Content)) {
		t.Errorf("wrong size returned\nwant %d\ngot  %d", len(testObj.Content), attrs.Size)
	}
	if testObj.Content != nil && attrs.CRC32C != uint32Checksum(testObj.Content) {
		t.Errorf("wrong checksum returned\nwant %d\ngot   %d", uint32Checksum(testObj.Content), attrs.CRC32C)
	}
	if testObj.Content != nil && !bytes.Equal(attrs.MD5, checksum.MD5Hash(testObj.Content)) {
		t.Errorf("wrong hash returned\nwant %d\ngot   %d", checksum.MD5Hash(testObj.Content), attrs.MD5)
	}
	if testObj.Metadata != nil {
		if val, err := getMetadataHeaderFromAttrs(attrs, "MetaHeader"); err != nil || val != testObj.Metadata["MetaHeader"] {
			t.Errorf("wrong MetaHeader returned\nwant %s\ngot %v", testObj.Metadata["MetaHeader"], val)
		}
	}
}

func TestServerClientObjectAttrs(t *testing.T) {
	tests := getObjectTestCases()
	for _, test := range tests {
		test := test
		runServersTest(t, []Object{test.obj}, func(t *testing.T, server *Server) {
			t.Run(test.testCase, func(t *testing.T) {
				client := server.Client()
				objHandle := client.Bucket(test.obj.BucketName).Object(test.obj.Name)
				attrs, err := objHandle.Attrs(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
				checkObjectAttrs(test.obj, attrs, t)
			})
		})
	}
}

func TestServerClientObjectAttrsAfterCreateObject(t *testing.T) {
	tests := getObjectTestCases()
	for _, test := range tests {
		test := test
		runServersTest(t, nil, func(t *testing.T, server *Server) {
			server.CreateObject(test.obj)
			client := server.Client()
			objHandle := client.Bucket(test.obj.BucketName).Object(test.obj.Name)
			attrs, err := objHandle.Attrs(context.TODO())
			if err != nil {
				t.Fatal(err)
			}
			checkObjectAttrs(test.obj, attrs, t)
		})
	}
}

func TestServerClientObjectAttrsAfterOverwriteWithVersioning(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
		const (
			bucketName  = "some-bucket-with-ver"
			content     = "some nice content"
			content2    = "some nice content x2"
			contentType = "text/plain; charset=utf-8"
			metaValue   = "MetaValue"
		)
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, VersioningEnabled: true})
		initialObj := Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content), ContentType: contentType, Crc32c: checksum.EncodedChecksum(uint32ToBytes(uint32Checksum([]byte(content)))), Md5Hash: checksum.EncodedHash(checksum.MD5Hash([]byte(content))), Metadata: map[string]string{"MetaHeader": metaValue}}
		server.CreateObject(initialObj)
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(initialObj.Name)
		originalObjAttrs, err := objHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("checking initial object attributes")
		checkObjectAttrs(initialObj, originalObjAttrs, t)

		// sleep for at least 100ns or more, so the creation time will differ on all platforms.
		time.Sleep(time.Microsecond)

		latestObjVersion := Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content2), ContentType: contentType, Crc32c: checksum.EncodedChecksum(uint32ToBytes(uint32Checksum([]byte(content2)))), Md5Hash: checksum.EncodedHash(checksum.MD5Hash([]byte(content2)))}
		server.CreateObject(latestObjVersion)
		objHandle = client.Bucket(bucketName).Object(latestObjVersion.Name)
		latestAttrs, err := objHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("checking object attributes after overwrite")
		checkObjectAttrs(latestObjVersion, latestAttrs, t)

		objHandle = client.Bucket(bucketName).Object(initialObj.Name).Generation(originalObjAttrs.Generation)
		originalObjAttrsAfterOverwrite, err := objHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("checking initial object attributes after overwrite")
		initialObj.Generation = originalObjAttrs.Generation
		checkObjectAttrs(initialObj, originalObjAttrsAfterOverwrite, t)
		if originalObjAttrsAfterOverwrite.Deleted.IsZero() || originalObjAttrsAfterOverwrite.Deleted.Before(originalObjAttrsAfterOverwrite.Created) {
			t.Errorf("unexpected delete time, %v", originalObjAttrsAfterOverwrite.Deleted)
		}

		if val, err := getMetadataHeaderFromAttrs(originalObjAttrsAfterOverwrite, "MetaHeader"); err != nil || val != metaValue {
			t.Errorf("wrong MetaHeader returned\nwant %s\ngot %v", metaValue, val)
		}
	})
}

func getMetadataHeaderFromAttrs(attrs *storage.ObjectAttrs, headerName string) (string, error) {
	if attrs.Metadata != nil {
		if val, ok := attrs.Metadata[headerName]; ok {
			return val, nil
		}
	}

	return "", errors.New("header does not exists")
}

func TestServerClientObjectAttrsErrors(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
		}{
			{
				"bucket not found",
				"other-bucket",
				"whatever-object",
			},
			{
				"object not found",
				"some-bucket",
				"img/low-res/party-01.jpg",
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				objHandle := server.Client().Bucket(test.bucketName).Object(test.objectName)
				attrs, err := objHandle.Attrs(context.TODO())
				if err == nil {
					t.Error("unexpected <nil> error")
				}
				if attrs != nil {
					t.Errorf("unexpected non-nil attrs: %#v", attrs)
				}
			})
		}
	})
}

func TestServerClientObjectReader(t *testing.T) {
	const (
		bucketName  = "some-bucket"
		objectName  = "items/data.txt"
		content     = "some nice content"
		contentType = "text/plain; charset=utf-8"
	)
	objs := []Object{
		{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)
		reader, err := objHandle.NewReader(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != content {
			t.Errorf("wrong data returned\nwant %q\ngot  %q", content, string(data))
		}
		if ct := reader.Attrs.ContentType; ct != contentType {
			t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, ct)
		}
	})
}

func TestServerClientObjectRangeReader(t *testing.T) {
	const (
		bucketName  = "some-bucket"
		objectName  = "items/data.txt"
		content     = "some really nice but long content stored in my object"
		contentType = "text/plain; charset=iso-8859"
	)
	objs := []Object{
		{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		tests := []struct {
			testCase string
			offset   int64
			length   int64
		}{
			{
				"no length, just offset",
				2,
				-1,
			},
			{
				"zero offset, length",
				0,
				10,
			},
			{
				"offset and length",
				4,
				10,
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				length := test.length
				if length == -1 {
					length = int64(len(content)) - test.offset
				}
				expectedData := content[test.offset : test.offset+length]
				client := server.Client()
				objHandle := client.Bucket(bucketName).Object(objectName)
				reader, err := objHandle.NewRangeReader(context.TODO(), test.offset, test.length)
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				data, err := ioutil.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != expectedData {
					t.Errorf("wrong data returned\nwant %q\ngot  %q", expectedData, string(data))
				}
				if ct := reader.Attrs.ContentType; ct != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, ct)
				}
			})
		}
	})
}

func TestServerClientObjectReaderAfterCreateObject(t *testing.T) {
	const (
		bucketName  = "staging-bucket"
		objectName  = "items/data-overwritten.txt"
		content     = "data inside the object"
		contentType = "text/plain; charset=iso-8859"
	)

	runServersTest(t, nil, func(t *testing.T, server *Server) {
		server.CreateObject(Object{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
		})
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)
		reader, err := objHandle.NewReader(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != content {
			t.Errorf("wrong data returned\nwant %q\ngot  %q", content, string(data))
		}
		if ct := reader.Attrs.ContentType; ct != contentType {
			t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, ct)
		}
	})
}

func TestServerClientObjectReaderAgainstSpecificGenerations(t *testing.T) {
	const (
		bucketName  = "staging-bucket"
		objectName  = "items/data-overwritten.txt"
		content     = "data inside the object"
		contentType = "text/plain; charset=iso-8859"
	)

	runServersTest(t, nil, func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, VersioningEnabled: true})
		object1 := Object{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
			Generation:  1111,
		}
		server.CreateObject(object1)
		object2 := Object{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content + "2"),
			ContentType: contentType,
		}
		server.CreateObject(object2)
		client := server.Client()
		latestHandle := client.Bucket(bucketName).Object(objectName)
		latestAttrs, err := latestHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		object2.Generation = latestAttrs.Generation
		for _, object := range []Object{object1, object2} {
			t.Log("about to get contents by generation for object", object)
			objHandle := client.Bucket(bucketName).Object(objectName).Generation(object.Generation)
			reader, err := objHandle.NewReader(context.TODO())
			if err != nil {
				t.Fatal(err)
			}
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(object.Content) {
				t.Errorf("wrong data returned\nwant %q\ngot  %q", string(object.Content), string(data))
			}
			if ct := reader.Attrs.ContentType; ct != object.ContentType {
				t.Errorf("wrong content type\nwant %q\ngot  %q", object.ContentType, ct)
			}
			reader.Close()
		}
	})
}

func TestServerClientObjectReaderError(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
		}{
			{
				"bucket not found",
				"other-bucket",
				"whatever-object",
			},
			{
				"object not found",
				"some-bucket",
				"img/low-res/party-01.jpg",
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				objHandle := server.Client().Bucket(test.bucketName).Object(test.objectName)
				reader, err := objHandle.NewReader(context.TODO())
				if err == nil {
					t.Error("unexpected <nil> error")
				}
				if reader != nil {
					t.Errorf("unexpected non-nil attrs: %#v", reader)
				}
			})
		}
	})
}

func TestServerClientObjectReadBucketCNAME(t *testing.T) {
	url := "https://mybucket.mydomain.com:4443/files/txt/text-01.txt"
	expectedHeaders := map[string]string{"accept-ranges": "bytes", "content-length": "9"}
	expectedBody := "something"
	opts := Options{
		InitialObjects: []Object{
			{BucketName: "mybucket.mydomain.com", Name: "files/txt/text-01.txt", Content: []byte("something")},
		},
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	client := server.HTTPClient()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
	}
	for k, expectedV := range expectedHeaders {
		if v := resp.Header.Get(k); v != expectedV {
			t.Errorf("wrong value for header %q:\nwant %q\ngot  %q", k, expectedV, v)
		}
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if body := string(data); body != expectedBody {
		t.Errorf("wrong body\nwant %q\ngot  %q", expectedBody, body)
	}
}

func getObjectsForListTests() []Object {
	return []Object{
		{BucketName: "some-bucket", Name: "img/low-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/low-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/low-res/party-03.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
		{BucketName: "some-bucket", Name: "img/brand.jpg"},
		{BucketName: "some-bucket", Name: "video/hi-res/some_video_1080p.mp4"},
		{BucketName: "other-bucket", Name: "static/css/style.css"},
	}
}

type listTest struct {
	testCase         string
	bucketName       string
	query            *storage.Query
	expectedNames    []string
	expectedPrefixes []string
}

func getTestCasesForListTests(versioningEnabled, withOverwrites bool) []listTest {
	return []listTest{
		{
			fmt.Sprintf("no prefix, no delimiter, multiple objects, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			nil,
			[]string{
				"img/brand.jpg",
				"img/hi-res/party-01.jpg",
				"img/hi-res/party-02.jpg",
				"img/hi-res/party-03.jpg",
				"img/low-res/party-01.jpg",
				"img/low-res/party-02.jpg",
				"img/low-res/party-03.jpg",
				"video/hi-res/some_video_1080p.mp4",
			},
			nil,
		},
		{
			fmt.Sprintf("no prefix, no delimiter, single object, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"other-bucket",
			nil,
			[]string{"static/css/style.css"},
			nil,
		},
		{
			fmt.Sprintf("no prefix, no delimiter, no objects, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"empty-bucket",
			nil,
			[]string{},
			nil,
		},
		{
			fmt.Sprintf("filtering prefix only, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "img/"},
			[]string{
				"img/brand.jpg",
				"img/hi-res/party-01.jpg",
				"img/hi-res/party-02.jpg",
				"img/hi-res/party-03.jpg",
				"img/low-res/party-01.jpg",
				"img/low-res/party-02.jpg",
				"img/low-res/party-03.jpg",
			},
			nil,
		},
		{
			fmt.Sprintf("full prefix, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "img/brand.jpg"},
			[]string{"img/brand.jpg"},
			nil,
		},
		{
			fmt.Sprintf("filtering prefix and delimiter, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "img/", Delimiter: "/"},
			[]string{"img/brand.jpg"},
			[]string{"img/hi-res/", "img/low-res/"},
		},
		{
			fmt.Sprintf("filtering prefix, no objects, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "static/"},
			[]string{},
			nil,
		},
		{
			fmt.Sprintf("filtering endOffset, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{EndOffset: "img/low-res"},
			[]string{
				"img/brand.jpg",
				"img/hi-res/party-01.jpg",
				"img/hi-res/party-02.jpg",
				"img/hi-res/party-03.jpg",
			},
			nil,
		},
		{
			fmt.Sprintf("filtering startOffset and endOffset, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{StartOffset: "img/hi-res", EndOffset: "img/low-res"},
			[]string{
				"img/hi-res/party-01.jpg",
				"img/hi-res/party-02.jpg",
				"img/hi-res/party-03.jpg",
			},
			nil,
		},
		{
			fmt.Sprintf("filtering prefix, delimiter, startOffset and endOffset, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "img/", Delimiter: "/", StartOffset: "img/hi-res", EndOffset: "img/low-res"},
			[]string{},
			[]string{"img/hi-res/"},
		},
		{
			fmt.Sprintf("filtering prefix, delimiter and endOffset, versioning %t and overwrites %t", versioningEnabled, withOverwrites),
			"some-bucket",
			&storage.Query{Prefix: "img/", Delimiter: "/", StartOffset: "", EndOffset: "img/low-res"},
			[]string{"img/brand.jpg"},
			[]string{"img/hi-res/"},
		},
	}
}

func TestServiceClientListObjects(t *testing.T) {
	runServersTest(t, getObjectsForListTests(), func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: "empty-bucket"})
		tests := getTestCasesForListTests(false, false)
		client := server.Client()
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				iter := client.Bucket(test.bucketName).Objects(context.TODO(), test.query)
				var prefixes []string
				names := []string{}
				obj, err := iter.Next()
				for ; err == nil; obj, err = iter.Next() {
					if obj.Name != "" {
						names = append(names, obj.Name)
					}
					if obj.Prefix != "" {
						prefixes = append(prefixes, obj.Prefix)
					}
				}
				if err != iterator.Done {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(names, test.expectedNames) {
					t.Errorf("wrong names returned\nwant %#v\ngot  %#v", test.expectedNames, names)
				}
				if !reflect.DeepEqual(prefixes, test.expectedPrefixes) {
					t.Errorf("wrong prefixes returned\nwant %#v\ngot  %#v", test.expectedPrefixes, prefixes)
				}
			})
		}
	})
}

func TestServerClientListAfterCreate(t *testing.T) {
	for _, versioningEnabled := range []bool{true, false} {
		for _, withOverwrites := range []bool{true, false} {
			versioningEnabled := versioningEnabled
			withOverwrites := withOverwrites
			runServersTest(t, nil, func(t *testing.T, server *Server) {
				for _, bucketName := range []string{"some-bucket", "other-bucket", "empty-bucket"} {
					server.CreateBucketWithOpts(CreateBucketOpts{
						Name:              bucketName,
						VersioningEnabled: versioningEnabled,
					})
				}
				for _, obj := range getObjectsForListTests() {
					server.CreateObject(obj)
					if withOverwrites {
						obj.Content = []byte("final content")
						server.CreateObject(obj)
					}
				}
				tests := getTestCasesForListTests(versioningEnabled, withOverwrites)
				client := server.Client()
				for _, test := range tests {
					test := test
					t.Run(test.testCase, func(t *testing.T) {
						iter := client.Bucket(test.bucketName).Objects(context.TODO(), test.query)
						var prefixes []string
						names := []string{}
						obj, err := iter.Next()
						for ; err == nil; obj, err = iter.Next() {
							if obj.Name != "" {
								names = append(names, obj.Name)
							}
							if obj.Prefix != "" {
								prefixes = append(prefixes, obj.Prefix)
							}
						}
						if err != iterator.Done {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(names, test.expectedNames) {
							t.Errorf("wrong names returned\nwant %#v\ngot  %#v", test.expectedNames, names)
						}
						if !reflect.DeepEqual(prefixes, test.expectedPrefixes) {
							t.Errorf("wrong prefixes returned\nwant %#v\ngot  %#v", test.expectedPrefixes, prefixes)
						}
					})
				}
			})
		}
	}
}

func contains(s []int64, e int64) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func unique(in []string) []string {
	out := []string{}
	found := make(map[string]bool)
	for _, entry := range in {
		if _, value := found[entry]; !value {
			found[entry] = true
			out = append(out, entry)
		}
	}
	return out
}

func TestServerClientListAfterCreateQueryingAllVersions(t *testing.T) {
	const initialGeneration = 1234
	const finalGeneration = initialGeneration + 1
	type listTestForQueryWithVersions struct {
		listTest
		expectedGenerations []int64
		expectedNumObjects  int
		versioningEnabled   bool
		withOverwrites      bool
	}
	tests := []listTestForQueryWithVersions{
		{
			listTest{
				"no prefix, no delimiter, multiple objects, with versioning and overwrites",
				"some-bucket",
				&storage.Query{Versions: true},
				[]string{
					"img/brand.jpg",
					"img/hi-res/party-01.jpg",
					"img/hi-res/party-02.jpg",
					"img/hi-res/party-03.jpg",
					"img/low-res/party-01.jpg",
					"img/low-res/party-02.jpg",
					"img/low-res/party-03.jpg",
					"video/hi-res/some_video_1080p.mp4",
				},
				nil,
			},
			[]int64{initialGeneration, finalGeneration},
			8 * 2,
			true,
			true,
		},
		{
			listTest{
				"no prefix, no delimiter, single object, with versioning and overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil,
			},
			[]int64{initialGeneration, finalGeneration},
			2,
			true,
			true,
		},
		{
			listTest{
				"no prefix, no delimiter, single object, without versioning and overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil,
			},
			[]int64{finalGeneration},
			1,
			false,
			true,
		},
		{
			listTest{
				"no prefix, no delimiter, single object, without versioning neither overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil,
			},
			[]int64{initialGeneration},
			1,
			false,
			false,
		},
		{
			listTest{
				"no prefix, no delimiter, single object, with versioning but no overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil,
			},
			[]int64{initialGeneration},
			1,
			true,
			false,
		},
		{
			listTest{
				"no prefix, no delimiter, no objects, versioning and overwrites",
				"empty-bucket",
				&storage.Query{Versions: true},
				[]string{},
				nil,
			},
			[]int64{},
			0,
			true,
			true,
		},
	}
	for _, test := range tests {
		test := test
		runServersTest(t, nil, func(t *testing.T, server *Server) {
			for _, bucketName := range []string{"some-bucket", "other-bucket", "empty-bucket"} {
				server.CreateBucketWithOpts(CreateBucketOpts{
					Name:              bucketName,
					VersioningEnabled: test.versioningEnabled,
				})
			}
			for _, obj := range getObjectsForListTests() {
				obj.Generation = initialGeneration
				server.CreateObject(obj)
				if test.withOverwrites {
					obj.Generation = finalGeneration
					obj.Content = []byte("final content")
					server.CreateObject(obj)
				}
			}
			client := server.Client()
			t.Run(test.testCase, func(t *testing.T) {
				iter := client.Bucket(test.bucketName).Objects(context.TODO(), test.query)
				names := []string{}
				obj, err := iter.Next()
				for ; err == nil; obj, err = iter.Next() {
					names = append(names, obj.Name)
					if !contains(test.expectedGenerations, obj.Generation) {
						t.Errorf("unexpected generation\nwant in %v\ngot    %d", test.expectedGenerations, obj.Generation)
					}
				}
				if err != iterator.Done {
					t.Fatal(err)
				}
				if len(names) != test.expectedNumObjects {
					t.Errorf("wrong number objects\nwant %d\ngot  %d", test.expectedNumObjects, len(names))
				}

				if !reflect.DeepEqual(unique(names), test.expectedNames) {
					t.Errorf("wrong names returned\nwant %#v\ngot  %#v", test.expectedNames, names)
				}
			})
		})
	}
}

func TestServiceClientListObjectsBucketNotFound(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
		iter := server.Client().Bucket("some-bucket").Objects(context.TODO(), nil)
		obj, err := iter.Next()
		if err == nil {
			t.Error("got unexpected <nil> error")
		}
		if obj != nil {
			t.Errorf("got unexpected non-nil obj: %#v", obj)
		}
	})
}

func TestServiceClientRewriteObject(t *testing.T) {
	const (
		content     = "some content"
		contentType = "text/plain; charset=utf-8"
	)
	u32Checksum := uint32Checksum([]byte(content))
	hash := checksum.MD5Hash([]byte(content))
	objs := []Object{
		{
			BucketName:  "first-bucket",
			Name:        "files/some-file.txt",
			Content:     []byte(content),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(u32Checksum)),
			Md5Hash:     checksum.EncodedHash(hash),
			Metadata:    map[string]string{"foo": "bar"},
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: "empty-bucket"})
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
			crc32c     uint32
			md5Hash    string
		}{
			{
				"same bucket same file",
				"first-bucket",
				"files/some-file.txt",
				u32Checksum,
				checksum.EncodedHash(hash),
			},
			{
				"same bucket",
				"first-bucket",
				"files/other-file.txt",
				u32Checksum,
				checksum.EncodedHash(hash),
			},
			{
				"different bucket",
				"empty-bucket",
				"some/interesting/file.txt",
				u32Checksum,
				checksum.EncodedHash(hash),
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				client := server.Client()
				sourceObject := client.Bucket("first-bucket").Object("files/some-file.txt")
				dstObject := client.Bucket(test.bucketName).Object(test.objectName)
				copier := dstObject.CopierFrom(sourceObject)
				copier.ContentType = contentType
				copier.Metadata = map[string]string{"baz": "qux"}
				attrs, err := copier.Run(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
				if attrs.Bucket != test.bucketName {
					t.Errorf("wrong bucket in copied object attrs\nwant %q\ngot  %q", test.bucketName, attrs.Bucket)
				}
				if attrs.Name != test.objectName {
					t.Errorf("wrong name in copied object attrs\nwant %q\ngot  %q", test.objectName, attrs.Name)
				}
				if attrs.Size != int64(len(content)) {
					t.Errorf("wrong size in copied object attrs\nwant %d\ngot  %d", len(content), attrs.Size)
				}
				if attrs.CRC32C != u32Checksum {
					t.Errorf("wrong checksum in copied object attrs\nwant %d\ngot  %d", u32Checksum, attrs.CRC32C)
				}
				if attrs.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, attrs.ContentType)
				}
				if !bytes.Equal(attrs.MD5, hash) {
					t.Errorf("wrong hash returned\nwant %d\ngot   %d", hash, attrs.MD5)
				}
				obj, err := server.GetObject(test.bucketName, test.objectName)
				if err != nil {
					t.Fatal(err)
				}
				if string(obj.Content) != content {
					t.Errorf("wrong content on object\nwant %q\ngot  %q", content, string(obj.Content))
				}
				if expect := checksum.EncodedChecksum(uint32ToBytes(u32Checksum)); expect != obj.Crc32c {
					t.Errorf("wrong checksum on object\nwant %s\ngot  %s", expect, obj.Crc32c)
				}
				if expect := checksum.EncodedHash(hash); expect != obj.Md5Hash {
					t.Errorf("wrong hash on object\nwant %s\ngot  %s", expect, obj.Md5Hash)
				}
				if obj.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
				}
				if !reflect.DeepEqual(obj.Metadata, copier.Metadata) {
					t.Errorf("wrong meta data\nwant %+v\ngot  %+v", copier.Metadata, obj.Metadata)
				}
			})
		}
	})
}

func TestServiceClientRewriteObjectWithGenerations(t *testing.T) {
	const (
		overwrittenContent    = "i was there"
		overwrittenGeneration = 123
		latestContent         = "some content"
		contentType           = "text/plain; charset=utf-8"
	)
	objs := []Object{
		{
			BucketName:  "first-bucket",
			Name:        "files/some-file.txt",
			Content:     []byte(overwrittenContent),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(uint32Checksum([]byte(overwrittenContent)))),
			Md5Hash:     checksum.EncodedHash(checksum.MD5Hash([]byte(overwrittenContent))),
			Generation:  overwrittenGeneration,
		},
		{
			BucketName:  "first-bucket",
			Name:        "files/some-file.txt",
			Content:     []byte(latestContent),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(uint32Checksum([]byte(latestContent)))),
			Md5Hash:     checksum.EncodedHash(checksum.MD5Hash([]byte(latestContent))),
		},
	}
	tests := []struct {
		testCase   string
		bucketName string
		objectName string
		versioning bool
		expectErr  bool
	}{
		{
			"same bucket from old gen",
			"first-bucket",
			"files/other-file.txt",
			true,
			false,
		},
		{
			"same bucket same file from old gen",
			"first-bucket",
			"files/some-file.txt",
			true,
			false,
		},
		{
			"different bucket",
			"empty-bucket",
			"some/interesting/file.txt",
			true,
			false,
		},
		{
			"no versioning, no old gen",
			"empty-bucket",
			"some/interesting/file.txt",
			false,
			true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.testCase, func(t *testing.T) {
			runServersTest(t, []Object{}, func(t *testing.T, server *Server) {
				server.CreateBucketWithOpts(CreateBucketOpts{Name: "empty-bucket", VersioningEnabled: false})
				server.CreateBucketWithOpts(CreateBucketOpts{Name: "first-bucket", VersioningEnabled: test.versioning})
				for _, obj := range objs {
					server.CreateObject(obj)
				}
				client := server.Client()
				sourceObject := client.Bucket("first-bucket").Object("files/some-file.txt").Generation(overwrittenGeneration)
				expectedContent := overwrittenContent
				expectedChecksum := uint32Checksum([]byte(overwrittenContent))
				expectedHash := checksum.MD5Hash([]byte(overwrittenContent))
				dstObject := client.Bucket(test.bucketName).Object(test.objectName)
				copier := dstObject.CopierFrom(sourceObject)
				copier.ContentType = contentType
				attrs, err := copier.Run(context.TODO())
				if err != nil {
					if test.expectErr {
						t.Skip("we were expecting an error for this test")
					}
					t.Fatal(err)
				}
				if attrs.Bucket != test.bucketName {
					t.Errorf("wrong bucket in copied object attrs\nwant %q\ngot  %q", test.bucketName, attrs.Bucket)
				}
				if attrs.Name != test.objectName {
					t.Errorf("wrong name in copied object attrs\nwant %q\ngot  %q", test.objectName, attrs.Name)
				}
				if attrs.Size != int64(len(expectedContent)) {
					t.Errorf("wrong size in copied object attrs\nwant %d\ngot  %d", len(expectedContent), attrs.Size)
				}
				if attrs.CRC32C != expectedChecksum {
					t.Errorf("wrong checksum in copied object attrs\nwant %d\ngot  %d", expectedChecksum, attrs.CRC32C)
				}
				if attrs.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, attrs.ContentType)
				}
				if !bytes.Equal(attrs.MD5, expectedHash) {
					t.Errorf("wrong hash returned\nwant %d\ngot   %d", expectedHash, attrs.MD5)
				}
				obj, err := server.GetObject(test.bucketName, test.objectName)
				if err != nil {
					t.Fatal(err)
				}
				if string(obj.Content) != expectedContent {
					t.Errorf("wrong content on object\nwant %q\ngot  %q", expectedContent, string(obj.Content))
				}
				if expect := checksum.EncodedChecksum(uint32ToBytes(expectedChecksum)); expect != obj.Crc32c {
					t.Errorf("wrong checksum on object\nwant %s\ngot  %s", expect, obj.Crc32c)
				}
				if expect := checksum.EncodedHash(expectedHash); expect != obj.Md5Hash {
					t.Errorf("wrong hash on object\nwant %s\ngot  %s", expect, obj.Md5Hash)
				}
				if obj.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
				}
			})
		})
	}
}

func TestServerClientObjectDelete(t *testing.T) {
	const (
		bucketName = "some-bucket"
		objectName = "img/hi-res/party-01.jpg"
		content    = "some nice content"
	)
	objs := []Object{
		{BucketName: bucketName, Name: objectName, Content: []byte(content)},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)
		err := objHandle.Delete(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		obj, err := server.GetObject(bucketName, objectName)
		if err == nil {
			t.Fatalf("unexpected success. obj: %#v", obj)
		}
	})
}

func TestServerClientObjectDeleteWithVersioning(t *testing.T) {
	obj := Object{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg", Content: []byte("some nice content"), Generation: 123}

	runServersTest(t, nil, func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: obj.BucketName, VersioningEnabled: true})
		server.CreateObject(obj)

		client := server.Client()
		objHandle := client.Bucket(obj.BucketName).Object(obj.Name)
		err := objHandle.Delete(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		objAfterDelete, err := server.GetObject(obj.BucketName, obj.Name)
		if err == nil {
			t.Fatalf("unexpected success. obj: %#v", objAfterDelete)
		}
		objWithGen, err := server.GetObjectWithGeneration(obj.BucketName, obj.Name, obj.Generation)
		if err != nil {
			t.Fatalf("unable to retrieve archived object. err: %v", err)
		}
		if objWithGen.Deleted.IsZero() || objWithGen.Deleted.Before(objWithGen.Created) {
			t.Errorf("unexpected delete time.\ndeleted: %v\ncreated: %v", objWithGen.Deleted, objWithGen.Created)
		}
	})
}

func TestServerClientObjectDeleteErrors(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
		}{
			{
				"bucket not found",
				"other-bucket",
				"whatever-object",
			},
			{
				"object not found",
				"some-bucket",
				"img/low-res/party-01.jpg",
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				objHandle := server.Client().Bucket(test.bucketName).Object(test.objectName)
				err := objHandle.Delete(context.TODO())
				if err == nil {
					t.Error("unexpected <nil> error")
				}
			})
		}
	})
}

func TestServerClientObjectSetAclPrivate(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/public-to-private.jpg"},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		t.Run("public to private", func(t *testing.T) {
			ctx := context.Background()
			objHandle := server.Client().Bucket("some-bucket").Object("img/public-to-private.jpg")

			err := objHandle.ACL().Set(ctx, storage.AllAuthenticatedUsers, storage.RoleReader)
			if err != nil {
				t.Fatalf("unexpected error while setting acl %+v", err)
				return
			}

			rules, err := objHandle.ACL().List(ctx)
			if err != nil {
				t.Fatalf("unexpected error while getting acl %+v", err)
				return
			}

			if len(rules) != 1 {
				t.Fatal("acl has no rules")
				return
			}
			if rules[0].Entity != storage.AllAuthenticatedUsers {
				t.Fatal("acl entity not set to AllAuthenticatedUsers")
				return
			}

			if rules[0].Role != storage.RoleReader {
				t.Fatal("acl role not set to RoleReader")
				return
			}
		})
	})
}

func TestServerClientObjectPatchMetadata(t *testing.T) {
	const (
		bucketName  = "some-bucket"
		objectName  = "items/data.txt"
		content     = "some nice content"
		contentType = "text/plain; charset=utf-8"
	)
	objs := []Object{
		{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
		},
	}
	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)

		metadata := map[string]string{
			"1-key": "1-value",
			"2-key": "2-value",
		}
		testPatch(metadata, metadata, objHandle, t)

		metadata = map[string]string{
			"1-key": "1.1-value",
			"3-key": "3-value",
		}
		testPatch(metadata, map[string]string{
			"1-key": "1.1-value",
			"2-key": "2-value",
			"3-key": "3-value",
		}, objHandle, t)
	})
}

func testPatch(newMetadata, finalMetadata map[string]string, objHandle *storage.ObjectHandle, t *testing.T) {
	ctx := context.TODO()
	_, err := objHandle.Update(ctx, storage.ObjectAttrsToUpdate{Metadata: newMetadata})
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := objHandle.Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkObjectMetadata(attrs.Metadata, finalMetadata, t)
}

func checkObjectMetadata(actual, expected map[string]string, t *testing.T) {
	if actual == nil {
		t.Fatalf("unexpected nil metadata")
	} else if len(actual) != len(expected) {
		t.Fatalf("Number of metadata mismatched: #actual = %d, #expected = %d", len(actual), len(expected))
	}
	for k, v := range expected {
		if actual[k] != v {
			t.Errorf("Metadata for key %s: actual = %s, expected = %s", k, actual[k], v)
		}
	}
}

func TestParseRangeRequest(t *testing.T) {
	ctx := context.TODO()

	in := []byte("this is a test object")

	srv, _ := NewServerWithOptions(Options{
		InitialObjects: []Object{
			{
				BucketName:  "test-bucket",
				Name:        "test-object",
				ContentType: "text/plain",
				Content:     in,
			},
		},
		NoListener: true,
	})
	obj := srv.Client().Bucket("test-bucket").Object("test-object")

	tests := []struct {
		Start  int64
		Length int64
	}{
		{4, 8},
		{4, -1},
		{0, 0},
		{0, -1},
		{0, 21},
	}

	for _, test := range tests {
		test := test
		t.Run("", func(t *testing.T) {
			t.Parallel()
			start, length := test.Start, test.Length

			rng, err := obj.NewRangeReader(ctx, start, length)
			if err != nil {
				t.Fatal(err)
			}
			out, _ := ioutil.ReadAll(rng)
			rng.Close()

			if length < 0 {
				length = int64(len(in)) - start
			}
			if n := int64(len(out)); n != length {
				t.Fatalf("expected %d bytes, RangeReader returned %d bytes", length, n)
			}
			if expected := in[start : start+length]; !bytes.Equal(expected, out) {
				t.Fatalf("expected %q, RangeReader returned %q", expected, out)
			}
		})
	}
}

func TestServiceClientComposeObject(t *testing.T) {
	const (
		source1Content = "some content"
		source2Content = "other content"
		source3Content = "third test"
		contentType    = "text/plain; charset=utf-8"
	)
	u32Checksum := uint32Checksum([]byte(source1Content))
	hash := checksum.MD5Hash([]byte(source1Content))

	objs := []Object{
		{
			BucketName:  "first-bucket",
			Name:        "files/source1.txt",
			Content:     []byte(source1Content),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(u32Checksum)),
			Md5Hash:     checksum.EncodedHash(hash),
			Metadata:    map[string]string{"foo": "bar"},
		},
		{
			BucketName:  "first-bucket",
			Name:        "files/source2.txt",
			Content:     []byte(source2Content),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(u32Checksum)),
			Md5Hash:     checksum.EncodedHash(hash),
			Metadata:    map[string]string{"foo": "bar"},
		},
		{
			BucketName:  "first-bucket",
			Name:        "files/source3.txt",
			Content:     []byte(source3Content),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(u32Checksum)),
			Md5Hash:     checksum.EncodedHash(hash),
			Metadata:    map[string]string{"foo": "bar"},
		},
		{
			BucketName:  "first-bucket",
			Name:        "files/destination.txt",
			Content:     []byte("test"),
			ContentType: contentType,
			Crc32c:      checksum.EncodedChecksum(uint32ToBytes(u32Checksum)),
			Md5Hash:     checksum.EncodedHash(hash),
			Metadata:    map[string]string{"foo": "bar"},
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: "empty-bucket"})
		tests := []struct {
			testCase          string
			bucketName        string
			destObjectName    string
			sourceObjectNames []string
			expectedContent   string
		}{
			{
				"destination file doesn't exist",
				"first-bucket",
				"files/some-file.txt",
				[]string{"files/source1.txt", "files/source2.txt"},
				source1Content + source2Content,
			},
			{
				"destination file already exists",
				"first-bucket",
				"files/destination.txt",
				[]string{"files/source1.txt", "files/source2.txt"},
				source1Content + source2Content,
			},
			{
				"destination is a source",
				"first-bucket",
				"files/source3.txt",
				[]string{"files/source2.txt", "files/source3.txt"},
				source2Content + source3Content,
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.testCase, func(t *testing.T) {
				client := server.Client()

				var sourceObjects []*storage.ObjectHandle
				for _, n := range test.sourceObjectNames {
					obj := client.Bucket(test.bucketName).Object(n)
					sourceObjects = append(sourceObjects, obj)
				}

				dstObject := client.Bucket(test.bucketName).Object(test.destObjectName)
				composer := dstObject.ComposerFrom(sourceObjects...)

				composer.ContentType = contentType
				composer.Metadata = map[string]string{"baz": "qux"}
				attrs, err := composer.Run(context.TODO())
				if err != nil {
					t.Fatal(err)
				}

				expectedChecksum := uint32Checksum([]byte(test.expectedContent))
				expectedHash := checksum.MD5Hash([]byte(test.expectedContent))

				if attrs.Bucket != test.bucketName {
					t.Errorf("wrong bucket in composed object attrs\nwant %q\ngot  %q", test.bucketName, attrs.Bucket)
				}
				if attrs.Name != test.destObjectName {
					t.Errorf("wrong name in composed object attrs\nwant %q\ngot  %q", test.destObjectName, attrs.Name)
				}
				if attrs.Size != int64(len(test.expectedContent)) {
					t.Errorf("wrong size in composed object attrs\nwant %d\ngot  %d", int64(len(test.expectedContent)), attrs.Size)
				}
				if attrs.CRC32C != expectedChecksum {
					t.Errorf("wrong checksum in copied object attrs\nwant %d\ngot  %d", u32Checksum, attrs.CRC32C)
				}
				if attrs.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, attrs.ContentType)
				}
				if !bytes.Equal(attrs.MD5, expectedHash) {
					t.Errorf("wrong hash returned\nwant %d\ngot   %d", hash, attrs.MD5)
				}
				obj, err := server.GetObject(test.bucketName, test.destObjectName)
				if err != nil {
					t.Fatal(err)
				}
				if string(obj.Content) != test.expectedContent {
					t.Errorf("wrong content on object\nwant %q\ngot  %q", test.expectedContent, string(obj.Content))
				}
				if expect := checksum.EncodedChecksum(uint32ToBytes(expectedChecksum)); expect != obj.Crc32c {
					t.Errorf("wrong checksum on object\nwant %s\ngot  %s", expect, obj.Crc32c)
				}
				if expect := checksum.EncodedHash(expectedHash); expect != obj.Md5Hash {
					t.Errorf("wrong hash on object\nwant %s\ngot  %s", expect, obj.Md5Hash)
				}
				if obj.ContentType != contentType {
					t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, obj.ContentType)
				}
				if !reflect.DeepEqual(obj.Metadata, composer.Metadata) {
					t.Errorf("wrong meta data\nwant %+v\ngot  %+v", composer.Metadata, obj.Metadata)
				}
			})
		}
	})
}
