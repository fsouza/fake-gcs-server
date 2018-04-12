package backend

import (
	"bytes"
	"fmt"
	"testing"
)

func makeStorageBackends(t *testing.T) map[string]Storage {
	return map[string]Storage{
		"memory": NewStorageMemory(nil),
	}
}

func testForStorageBackends(t *testing.T, test func(t *testing.T, storage Storage)) {
	for backendName, storage := range makeStorageBackends(t) {
		t.Run(fmt.Sprintf("storage backend %s", backendName), func(t *testing.T) {
			test(t, storage)
		})
	}
}

func TestObjectCRUD(t *testing.T) {
	const bucketName = "prod-bucket"
	const objectName = "video/hi-res/best_video_1080p.mp4"
	var content1 = []byte("content1")
	var content2 = []byte("content2")
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		// Get in non-existent case
		_, err := storage.GetObject(bucketName, objectName)
		if err == nil {
			t.Fatal("object found before being created")
		}
		// Delete in non-existent case
		err = storage.DeleteObject(bucketName, objectName)
		if err == nil {
			t.Fatal("object successfully delete before being created")
		}
		// Create in non-existent case
		err = storage.CreateObject(Object{BucketName: bucketName, Name: objectName, Content: content1})
		if err != nil {
			t.Fatal(err)
		}
		// Get in existent case
		obj, err := storage.GetObject(bucketName, objectName)
		if err != nil {
			t.Fatal(err)
		}
		if obj.BucketName != bucketName {
			t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, obj.BucketName)
		}
		if obj.Name != objectName {
			t.Errorf("wrong object name\n want %q\ngot  %q", objectName, obj.Name)
		}
		if !bytes.Equal(obj.Content, content1) {
			t.Errorf("wrong object content\n want %q\ngot  %q", content1, obj.Content)
		}
		// Create (update) in existent case
		err = storage.CreateObject(Object{BucketName: bucketName, Name: objectName, Content: content2})
		if err != nil {
			t.Fatal(err)
		}
		obj, err = storage.GetObject(bucketName, objectName)
		if err != nil {
			t.Fatal(err)
		}
		if obj.BucketName != bucketName {
			t.Errorf("wrong bucket name\nwant %q\ngot  %q", bucketName, obj.BucketName)
		}
		if obj.Name != objectName {
			t.Errorf("wrong object name\n want %q\ngot  %q", objectName, obj.Name)
		}
		if !bytes.Equal(obj.Content, content2) {
			t.Errorf("wrong object content\n want %q\ngot  %q", content2, obj.Content)
		}
		// Delete in existent case
		err = storage.DeleteObject(bucketName, objectName)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBucketCreateGetList(t *testing.T) {
	const bucketName = "prod-bucket"
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		err := storage.GetBucket(bucketName)
		if err == nil {
			t.Fatal("bucket exists before being created")
		}
		buckets, err := storage.ListBuckets()
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 0 {
			t.Fatalf("more than zero buckets found: %d", len(buckets))
		}
		err = storage.CreateBucket(bucketName)
		if err != nil {
			t.Fatal(err)
		}
		err = storage.GetBucket(bucketName)
		if err != nil {
			t.Fatal(err)
		}
		buckets, err = storage.ListBuckets()
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 1 {
			t.Fatalf("one bucket not found after creating it, found: %d", len(buckets))
		}
		if buckets[0] != bucketName {
			t.Fatalf("wrong bucket name; expected %s, got %s", bucketName, buckets[0])
		}
	})
}
