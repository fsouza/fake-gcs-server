// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"io/ioutil"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func TestServerClientObjectAttrs(t *testing.T) {
	const bucketName = "some-bucket"
	const objectName = "img/hi-res/party-01.jpg"
	const content = "some nice content"
	server := NewServer([]Object{
		{BucketName: bucketName, Name: objectName, Content: []byte(content)},
	})
	defer server.Stop()
	client := server.Client()
	objHandle := client.Bucket(bucketName).Object(objectName)
	attrs, err := objHandle.Attrs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Bucket != bucketName {
		t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, attrs.Bucket)
	}
	if attrs.Name != objectName {
		t.Errorf("wrong object name\nwant %q\ngot  %q", objectName, attrs.Name)
	}
	if attrs.Size != int64(len(content)) {
		t.Errorf("wrong size returned\nwant %d\ngot  %d", len(content), attrs.Size)
	}
}

func TestServerClientObjectAttrsAfterCreateObject(t *testing.T) {
	const bucketName = "prod-bucket"
	const objectName = "video/hi-res/best_video_1080p.mp4"
	server := NewServer(nil)
	defer server.Stop()
	server.CreateObject(Object{BucketName: bucketName, Name: objectName})
	client := server.Client()
	objHandle := client.Bucket(bucketName).Object(objectName)
	attrs, err := objHandle.Attrs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Bucket != bucketName {
		t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, attrs.Bucket)
	}
	if attrs.Name != objectName {
		t.Errorf("wrong object name\n want %q\ngot  %q", objectName, attrs.Name)
	}
}

func TestServerClientObjectAttrsErrors(t *testing.T) {
	server := NewServer([]Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
	})
	defer server.Stop()
	var tests = []struct {
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
		t.Run(test.testCase, func(t *testing.T) {
			objHandle := server.Client().Bucket(test.bucketName).Object(test.objectName)
			attrs, err := objHandle.Attrs(context.Background())
			if err == nil {
				t.Error("unexpected <nil> error")
			}
			if attrs != nil {
				t.Errorf("unexpected non-nil attrs: %#v", attrs)
			}
		})
	}
}

func TestServerClientObjectReader(t *testing.T) {
	const bucketName = "some-bucket"
	const objectName = "items/data.txt"
	const content = "some nice content"
	server := NewServer([]Object{
		{
			BucketName: bucketName,
			Name:       objectName,
			Content:    []byte(content),
		},
	})
	defer server.Stop()
	client := server.Client()
	objHandle := client.Bucket(bucketName).Object(objectName)
	reader, err := objHandle.NewReader(context.Background())
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
}

func TestServerClientObjectRangeReader(t *testing.T) {
	const bucketName = "some-bucket"
	const objectName = "items/data.txt"
	const content = "some really nice but long content stored in my object"
	server := NewServer([]Object{
		{
			BucketName: bucketName,
			Name:       objectName,
			Content:    []byte(content),
		},
	})
	defer server.Stop()
	var tests = []struct {
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
		t.Run(test.testCase, func(t *testing.T) {
			if test.length == -1 {
				test.length = int64(len(content)) - test.offset
			}
			expectedData := content[test.offset : test.offset+test.length-1]
			client := server.Client()
			objHandle := client.Bucket(bucketName).Object(objectName)
			reader, err := objHandle.NewRangeReader(context.Background(), test.offset, test.length)
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
		})
	}
}

func TestServerClientObjectReaderAfterCreateObject(t *testing.T) {
	const bucketName = "staging-bucket"
	const objectName = "items/data-overwritten.txt"
	const content = "data inside the object"
	server := NewServer([]Object{
		{BucketName: bucketName, Name: objectName},
	})
	defer server.Stop()
	server.CreateObject(Object{BucketName: bucketName, Name: objectName, Content: []byte(content)})
	client := server.Client()
	objHandle := client.Bucket(bucketName).Object(objectName)
	reader, err := objHandle.NewReader(context.Background())
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
}

func TestServerClientObjectReaderError(t *testing.T) {
	server := NewServer([]Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
	})
	defer server.Stop()
	var tests = []struct {
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
		t.Run(test.testCase, func(t *testing.T) {
			objHandle := server.Client().Bucket(test.bucketName).Object(test.objectName)
			reader, err := objHandle.NewReader(context.Background())
			if err == nil {
				t.Error("unexpected <nil> error")
			}
			if reader != nil {
				t.Errorf("unexpected non-nil attrs: %#v", reader)
			}
		})
	}
}

func TestServiceClientListObjects(t *testing.T) {
	server := NewServer([]Object{
		{BucketName: "some-bucket", Name: "img/low-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/low-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/low-res/party-03.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
		{BucketName: "some-bucket", Name: "img/brand.jpg"},
		{BucketName: "some-bucket", Name: "video/hi-res/some_video_1080p.mp4"},
		{BucketName: "other-bucket", Name: "static/css/style.css"},
	})
	defer server.Stop()
	server.CreateBucket("empty-bucket")
	var tests = []struct {
		testCase      string
		bucketName    string
		query         *storage.Query
		expectedNames []string
	}{
		{
			"no prefix, no delimiter, multiple objects",
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
		},
		{
			"no prefix, no delimiter, single object",
			"other-bucket",
			nil,
			[]string{"static/css/style.css"},
		},
		{
			"no prefix, no delimiter, no objects",
			"empty-bucket",
			nil,
			[]string{},
		},
		{
			"filtering prefix only",
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
		},
		{
			"filtering prefix and delimiter",
			"some-bucket",
			&storage.Query{Prefix: "img/", Delimiter: "/"},
			[]string{"img/brand.jpg"},
		},
		{
			"filtering prefix, no objects",
			"some-bucket",
			&storage.Query{Prefix: "static/"},
			[]string{},
		},
	}
	client := server.Client()
	for _, test := range tests {
		t.Run(test.testCase, func(t *testing.T) {
			iter := client.Bucket(test.bucketName).Objects(context.Background(), test.query)
			names := []string{}
			obj, err := iter.Next()
			for ; err == nil; obj, err = iter.Next() {
				names = append(names, obj.Name)
			}
			if err != iterator.Done {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(names, test.expectedNames) {
				t.Errorf("wrong names returned\nwant %#v\ngot  %#v", test.expectedNames, names)
			}
		})
	}
}

func TestServiceClientListObjectsBucketNotFound(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	iter := server.Client().Bucket("some-bucket").Objects(context.Background(), nil)
	obj, err := iter.Next()
	if err == nil {
		t.Error("got unexpected <nil> error")
	}
	if obj != nil {
		t.Errorf("got unexpected non-nil obj: %#v", obj)
	}
}
