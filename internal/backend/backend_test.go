// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func makeStorageBackends(t *testing.T) (map[string]Storage, func()) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "fakegcstest")
	if err != nil {
		t.Fatal(err)
	}
	storageFS, err := NewStorageFS(nil, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]Storage{
			"memory":     NewStorageMemory(nil),
			"filesystem": storageFS,
		}, func() {
			err := os.RemoveAll(tempDir)
			if err != nil {
				t.Fatal(err)
			}
		}
}

func testForStorageBackends(t *testing.T, test func(t *testing.T, storage Storage)) {
	backends, cleanup := makeStorageBackends(t)
	defer cleanup()
	for backendName, storage := range backends {
		storage := storage
		t.Run(fmt.Sprintf("storage backend %s", backendName), func(t *testing.T) {
			test(t, storage)
		})
	}
}

func noError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func shouldError(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatal(message)
	}
}

func TestObjectCRUD(t *testing.T) {
	const bucketName = "prod-bucket"
	const objectName = "video/hi-res/best_video_1080p.mp4"
	content1 := []byte("content1")
	const crc1 = "crc1"
	const md51 = "md51"
	content2 := []byte("content2")
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		// Get in non-existent case
		_, err := storage.GetObject(bucketName, objectName)
		shouldError(t, err, "object found before being created")
		// Delete in non-existent case
		err = storage.DeleteObject(bucketName, objectName)
		shouldError(t, err, "object successfully delete before being created")
		// Create in non-existent case
		noError(t, storage.CreateObject(Object{BucketName: bucketName, Name: objectName, Content: content1, Crc32c: crc1, Md5Hash: md51}))
		// Get in existent case
		t.Logf("%v", storage)
		obj, err := storage.GetObject(bucketName, objectName)
		noError(t, err)
		if obj.BucketName != bucketName {
			t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, obj.BucketName)
		}
		if obj.Name != objectName {
			t.Errorf("wrong object name\n want %q\ngot  %q", objectName, obj.Name)
		}
		if obj.Crc32c != crc1 {
			t.Errorf("wrong crc\n want %q\ngot  %q", crc1, obj.Crc32c)
		}
		if obj.Md5Hash != md51 {
			t.Errorf("wrong md5\n want %q\ngot  %q", md51, obj.Md5Hash)
		}
		if !bytes.Equal(obj.Content, content1) {
			t.Errorf("wrong object content\n want %q\ngot  %q", content1, obj.Content)
		}
		// Create (update) in existent case
		err = storage.CreateObject(Object{BucketName: bucketName, Name: objectName, Content: content2})
		noError(t, err)
		obj, err = storage.GetObject(bucketName, objectName)
		noError(t, err)
		if obj.BucketName != bucketName {
			t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, obj.BucketName)
		}
		if obj.Name != objectName {
			t.Errorf("wrong object name\n want %q\ngot  %q", objectName, obj.Name)
		}
		if !bytes.Equal(obj.Content, content2) {
			t.Errorf("wrong object content\n want %q\ngot  %q", content2, obj.Content)
		}

		// List objects
		objs, err := storage.ListObjects(bucketName)
		noError(t, err)
		if len(objs) != 1 {
			t.Errorf("wrong number of objects returned\nwant 1\ngot  %d", len(objs))
		}
		if objs[0].Name != objectName {
			t.Errorf("wrong object name\nwant %q\ngot  %q", objectName, objs[0].Name)
		}

		// Delete in existent case
		err = storage.DeleteObject(bucketName, objectName)
		noError(t, err)
	})
}

func TestBucketCreateGetList(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		buckets, err := storage.ListBuckets()
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 0 {
			t.Fatalf("more than zero buckets found: %d, and expecting zero when starting the test", len(buckets))
		}
		bucketsToTest := []Bucket{
			{"prod-bucket", false},
			{"prod-bucket-with-versioning", true},
		}
		for i, bucket := range bucketsToTest {
			_, err := storage.GetBucket(bucket.Name)
			if err == nil {
				t.Fatalf("bucket %s, exists before being created", bucket.Name)
			}
			err = storage.CreateBucket(bucket.Name, bucket.VersioningEnabled)
			if reflect.TypeOf(storage) == reflect.TypeOf(&StorageFS{}) && bucket.VersioningEnabled {
				if err == nil {
					t.Fatal("fs storage should not accept creating buckets with versioning, but it's not failing")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			bucketFromStorage, err := storage.GetBucket(bucket.Name)
			if err != nil {
				t.Fatal(err)
			}
			if bucketFromStorage != bucket {
				t.Errorf("bucket %v does not have the expected props after retrieving. Expected %v", bucketFromStorage, bucket)
			}
			buckets, err = storage.ListBuckets()
			if err != nil {
				t.Fatal(err)
			}
			if len(buckets) != i+1 {
				t.Errorf("number of buckets does not match the times we have lopped. Expected %d, found %d", i, len(buckets))
			}
			if buckets[i].Name != bucket.Name {
				t.Errorf("wrong bucket name; expected %s, got %s", bucket.Name, buckets[i].Name)
			}
		}
	})
}
func TestBucketDuplication(t *testing.T) {
	const bucketName = "prod-bucket"
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		err := storage.CreateBucket(bucketName, false)
		if err != nil {
			t.Fatal(err)
		}

		err = storage.CreateBucket(bucketName, true)
		if err == nil {
			t.Fatal("we were expecting a bucket duplication error")
		}
	})
}
