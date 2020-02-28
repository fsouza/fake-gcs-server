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
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func uint32ToBytes(ui uint32) []byte {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, ui)
	return res
}

func uint32Checksum(b []byte) uint32 {
	checksummer := crc32.New(crc32cTable)
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
	testInitExecTime := time.Now()
	checksum := uint32Checksum([]byte(content))
	hash := md5Hash([]byte(content))

	tests := objectTestCases{
		{
			"object but no creation nor modification date",
			Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: encodedChecksum(uint32ToBytes(checksum)), Md5Hash: encodedHash(hash)},
		},
		{
			"object with creation and modification dates",
			Object{BucketName: bucketName, Name: "img/low-res/party-02.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: encodedChecksum(uint32ToBytes(checksum)), Md5Hash: encodedHash(hash), Created: testInitExecTime, Updated: testInitExecTime},
		},
		{
			"object with creation, modification dates, and generation",
			Object{BucketName: bucketName, Name: "img/low-res/party-02.jpg", Content: []byte(content), ContentType: contentType, Crc32c: encodedChecksum(uint32ToBytes(checksum)), Md5Hash: encodedHash(hash), Created: testInitExecTime, Updated: testInitExecTime, Generation: testInitExecTime.UnixNano()},
		},
		{
			"object with everything",
			Object{BucketName: bucketName, Name: "img/location/meta.jpg", Content: []byte(content), ContentType: contentType, ContentEncoding: contentEncoding, Crc32c: encodedChecksum(uint32ToBytes(checksum)), Md5Hash: encodedHash(hash), Metadata: map[string]string{"MetaHeader": metaValue}},
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
	}
	if attrs.Bucket != testObj.BucketName {
		t.Errorf("wrong bucket name\nwant %q\ngot  %q", testObj.BucketName, attrs.Bucket)
	}
	if attrs.Name != testObj.Name {
		t.Errorf("wrong object name\nwant %q\ngot  %q", testObj.Name, attrs.Name)
	}
	if !(testObj.Created.IsZero()) && testObj.Created.Equal(attrs.Created) {
		t.Errorf("wrong created date\nwant %v\ngot   %v\nname %v", testObj.Created, attrs.Created, attrs.Name)
	}
	if !(testObj.Updated.IsZero()) && testObj.Updated.Equal(attrs.Updated) {
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
	if testObj.Content != nil && !bytes.Equal(attrs.MD5, md5Hash(testObj.Content)) {
		t.Errorf("wrong hash returned\nwant %d\ngot   %d", md5Hash(testObj.Content), attrs.MD5)
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
		initialObj := Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content), ContentType: contentType, Crc32c: encodedChecksum(uint32ToBytes(uint32Checksum([]byte(content)))), Md5Hash: encodedHash(md5Hash([]byte(content))), Metadata: map[string]string{"MetaHeader": metaValue}}
		server.CreateObject(initialObj)
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(initialObj.Name)
		originalObjAttrs, err := objHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("checking initial object attributes")
		checkObjectAttrs(initialObj, originalObjAttrs, t)

		latestObjVersion := Object{BucketName: bucketName, Name: "img/low-res/party-01.jpg", Content: []byte(content2), ContentType: contentType, Crc32c: encodedChecksum(uint32ToBytes(uint32Checksum([]byte(content2)))), Md5Hash: encodedHash(md5Hash([]byte(content2)))}
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
					length = int64(len(content)) - test.offset + 1
				}
				expectedData := content[test.offset : test.offset+length-1]
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
			listTest{"no prefix, no delimiter, multiple objects, with versioning and overwrites",
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
				nil},
			[]int64{initialGeneration, finalGeneration},
			8 * 2,
			true,
			true,
		},
		{
			listTest{"no prefix, no delimiter, single object, with versioning and overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil},
			[]int64{initialGeneration, finalGeneration},
			2,
			true,
			true,
		},
		{
			listTest{"no prefix, no delimiter, single object, without versioning and overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil},
			[]int64{finalGeneration},
			1,
			false,
			true,
		},
		{
			listTest{"no prefix, no delimiter, single object, without versioning neither overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil},
			[]int64{initialGeneration},
			1,
			false,
			false,
		},
		{
			listTest{"no prefix, no delimiter, single object, with versioning but no overwrites",
				"other-bucket",
				&storage.Query{Versions: true},
				[]string{
					"static/css/style.css",
				},
				nil},
			[]int64{initialGeneration},
			1,
			true,
			false,
		},
		{
			listTest{"no prefix, no delimiter, no objects, versioning and overwrites",
				"empty-bucket",
				&storage.Query{Versions: true},
				[]string{},
				nil},
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
	checksum := uint32Checksum([]byte(content))
	hash := md5Hash([]byte(content))
	objs := []Object{
		{
			BucketName:  "first-bucket",
			Name:        "files/some-file.txt",
			Content:     []byte(content),
			ContentType: contentType,
			Crc32c:      encodedChecksum(uint32ToBytes(checksum)),
			Md5Hash:     encodedHash(hash),
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
				checksum,
				encodedHash(hash),
			},
			{
				"same bucket",
				"first-bucket",
				"files/other-file.txt",
				checksum,
				encodedHash(hash),
			},
			{
				"different bucket",
				"empty-bucket",
				"some/interesting/file.txt",
				checksum,
				encodedHash(hash),
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
				if attrs.CRC32C != checksum {
					t.Errorf("wrong checksum in copied object attrs\nwant %d\ngot  %d", checksum, attrs.CRC32C)
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
				if expect := encodedChecksum(uint32ToBytes(checksum)); expect != obj.Crc32c {
					t.Errorf("wrong checksum on object\nwant %s\ngot  %s", expect, obj.Crc32c)
				}
				if expect := encodedHash(hash); expect != obj.Md5Hash {
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
			Crc32c:      encodedChecksum(uint32ToBytes(uint32Checksum([]byte(overwrittenContent)))),
			Md5Hash:     encodedHash(md5Hash([]byte(overwrittenContent))),
			Generation:  overwrittenGeneration,
		},
		{
			BucketName:  "first-bucket",
			Name:        "files/some-file.txt",
			Content:     []byte(latestContent),
			ContentType: contentType,
			Crc32c:      encodedChecksum(uint32ToBytes(uint32Checksum([]byte(latestContent)))),
			Md5Hash:     encodedHash(md5Hash([]byte(latestContent))),
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
				expectedHash := md5Hash([]byte(overwrittenContent))
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
				if expect := encodedChecksum(uint32ToBytes(expectedChecksum)); expect != obj.Crc32c {
					t.Errorf("wrong checksum on object\nwant %s\ngot  %s", expect, obj.Crc32c)
				}
				if expect := encodedHash(expectedHash); expect != obj.Md5Hash {
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
			t.Errorf("unexpected delete time, %v", objWithGen.Deleted)
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
