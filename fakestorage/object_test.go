// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"reflect"
	"testing"

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

func TestServerClientObjectAttrs(t *testing.T) {
	const (
		bucketName  = "some-bucket"
		objectName  = "img/hi-res/party-01.jpg"
		content     = "some nice content"
		contentType = "text/plain; charset=utf-8"
	)
	checksum := uint32Checksum([]byte(content))
	hash := md5Hash([]byte(content))
	objs := []Object{
		{
			BucketName:  bucketName,
			Name:        objectName,
			Content:     []byte(content),
			ContentType: contentType,
			Crc32c:      encodedChecksum(uint32ToBytes(checksum)),
			Md5Hash:     encodedHash(hash),
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)
		attrs, err := objHandle.Attrs(context.TODO())
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
		if attrs.ContentType != contentType {
			t.Errorf("wrong content type\nwant %q\ngot  %q", contentType, attrs.ContentType)
		}
		if attrs.CRC32C != checksum {
			t.Errorf("wrong checksum returned\nwant %d\ngot   %d", checksum, attrs.CRC32C)
		}
		if !bytes.Equal(attrs.MD5, hash) {
			t.Errorf("wrong hash returned\nwant %d\ngot   %d", hash, attrs.MD5)
		}
	})
}

func TestServerClientObjectAttrsAfterCreateObject(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
		const (
			bucketName  = "prod-bucket"
			objectName  = "video/hi-res/best_video_1080p.mp4"
			contentType = "text/html; charset=utf-8"
		)
		server.CreateObject(Object{
			BucketName:  bucketName,
			Name:        objectName,
			ContentType: contentType,
		})
		client := server.Client()
		objHandle := client.Bucket(bucketName).Object(objectName)
		attrs, err := objHandle.Attrs(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		if attrs.Bucket != bucketName {
			t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, attrs.Bucket)
		}
		if attrs.Name != objectName {
			t.Errorf("wrong object name\n want %q\ngot  %q", objectName, attrs.Name)
		}
		if attrs.ContentType != contentType {
			t.Errorf("wrong content type\n want %q\ngot  %q", contentType, attrs.ContentType)
		}
	})
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

func TestServiceClientListObjects(t *testing.T) {
	objs := []Object{
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

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		server.CreateBucket("empty-bucket")
		tests := []struct {
			testCase         string
			bucketName       string
			query            *storage.Query
			expectedNames    []string
			expectedPrefixes []string
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
				nil,
			},
			{
				"no prefix, no delimiter, single object",
				"other-bucket",
				nil,
				[]string{"static/css/style.css"},
				nil,
			},
			{
				"no prefix, no delimiter, no objects",
				"empty-bucket",
				nil,
				[]string{},
				nil,
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
				nil,
			},
			{
				"full prefix",
				"some-bucket",
				&storage.Query{Prefix: "img/brand.jpg"},
				[]string{"img/brand.jpg"},
				nil,
			},
			{
				"filtering prefix and delimiter",
				"some-bucket",
				&storage.Query{Prefix: "img/", Delimiter: "/"},
				[]string{"img/brand.jpg"},
				[]string{"img/hi-res/", "img/low-res/"},
			},
			{
				"filtering prefix, no objects",
				"some-bucket",
				&storage.Query{Prefix: "static/"},
				[]string{},
				nil,
			},
		}
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
		},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		server.CreateBucket("empty-bucket")
		tests := []struct {
			testCase   string
			bucketName string
			objectName string
			crc32c     uint32
			md5hash    string
		}{
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
				attrs, err := dstObject.CopierFrom(sourceObject).Run(context.TODO())
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
			})
		}
	})
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
			t.Fatalf("unexpected nil error. obj: %#v", obj)
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
				t.Errorf("unexpected error while setting acl %+v", err)
				return
			}

			rules, err := objHandle.ACL().List(ctx)
			if err != nil {
				t.Errorf("unexpected error while getting acl %+v", err)
				return
			}

			if len(rules) != 1 && rules[0].Entity != storage.AllAuthenticatedUsers && rules[0].Role != storage.RoleReader {
				t.Error("bad acl")
				return
			}
		})
	})
}
