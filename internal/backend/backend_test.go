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
	"time"
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
	t.Helper()
	if err != nil {
		t.Fatalf("should not error, but got: %v", err)
	}
}

func shouldError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("should error, but error is nil")
	}
}

func uploadAndCompare(t *testing.T, storage Storage, obj Object) int64 {
	isFSStorage := reflect.TypeOf(storage) == reflect.TypeOf(&StorageFS{})
	err := storage.CreateObject(obj)
	if isFSStorage && obj.Generation != 0 {
		t.Log("FS should not support objects generation")
		shouldError(t, err)
		obj.Generation = 0
		err = storage.CreateObject(obj)
	}
	noError(t, err)
	activeObj, err := storage.GetObject(obj.BucketName, obj.Name)
	noError(t, err)
	if isFSStorage && activeObj.Generation != 0 {
		t.Errorf("FS should leave generation empty, as it does not persist it. Value: %d", activeObj.Generation)
	}
	if !isFSStorage && activeObj.Generation == 0 {
		t.Errorf("generation is empty, but we expect a unique int")
	}
	if err := compareObjects(activeObj, obj); err != nil {
		t.Errorf("object retrieved differs from the created one. Descr: %v", err)
	}
	objFromGeneration, err := storage.GetObjectWithGeneration(obj.BucketName, obj.Name, activeObj.Generation)
	if isFSStorage {
		t.Log("FS should not implement fetch with generation")
		shouldError(t, err)
	} else {
		noError(t, err)
		if err := compareObjects(objFromGeneration, obj); err != nil {
			t.Errorf("object retrieved differs from the created one. Descr: %v", err)
		}
	}
	return activeObj.Generation
}

func TestObjectCRUD(t *testing.T) {
	const bucketName = "prod-bucket"
	const objectName = "video/hi-res/best_video_1080p.mp4"
	content1 := []byte("content1")
	const crc1 = "crc1"
	const md51 = "md51"
	content2 := []byte("content2")
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		testForStorageBackends(t, func(t *testing.T, storage Storage) {
			// Get in non-existent case
			_, err := storage.GetObject(bucketName, objectName)
			shouldError(t, err)
			// Delete in non-existent case
			err = storage.DeleteObject(bucketName, objectName)
			shouldError(t, err)
			err = storage.CreateBucket(bucketName, versioningEnabled)
			if reflect.TypeOf(storage) == reflect.TypeOf(&StorageFS{}) && versioningEnabled {
				t.Log("FS storage type should not implement versioning")
				shouldError(t, err)
				return
			}

			initialObject := Object{BucketName: bucketName, Name: objectName, Content: content1, Crc32c: crc1, Md5Hash: md51}
			t.Logf("create an initial object on an empty bucket with versioning %t", versioningEnabled)
			initialGeneration := uploadAndCompare(t, storage, initialObject)

			t.Logf("create (update) in existent case with explicit generation and versioning %t", versioningEnabled)
			secondVersionWithGeneration := Object{BucketName: bucketName, Name: objectName, Content: content2, Generation: 1234}
			uploadAndCompare(t, storage, secondVersionWithGeneration)

			initialObjectFromGeneration, err := storage.GetObjectWithGeneration(initialObject.BucketName, initialObject.Name, initialGeneration)
			if !versioningEnabled {
				shouldError(t, err)
			} else {
				noError(t, err)
				if err := compareObjects(initialObjectFromGeneration, initialObject); err != nil {
					t.Errorf("get initial generation - object retrieved differs from the created one. Descr: %v", err)
				}
			}

			t.Logf("checking active object is the expected one when versioning is %t", versioningEnabled)
			objs, err := storage.ListObjects(bucketName, false)
			noError(t, err)
			if len(objs) != 1 {
				t.Errorf("wrong number of objects returned\nwant 1\ngot  %d", len(objs))
			}
			if objs[0].Name != objectName {
				t.Errorf("wrong object name\nwant %q\ngot  %q", objectName, objs[0].Name)
			}

			t.Logf("checking all object listing is the expected one when versioning is %t", versioningEnabled)
			objs, err = storage.ListObjects(bucketName, true)
			noError(t, err)
			if versioningEnabled && len(objs) != 2 {
				t.Errorf("wrong number of objects returned\nwant 2\ngot  %d", len(objs))
			}
			if !versioningEnabled && len(objs) != 1 {
				t.Errorf("wrong number of objects returned\nwant 1\ngot  %d", len(objs))
			}

			err = storage.DeleteObject(bucketName, objectName)
			noError(t, err)

			_, err = storage.GetObject(bucketName, objectName)
			shouldError(t, err)

			retrievedObject, err := storage.GetObjectWithGeneration(secondVersionWithGeneration.BucketName, secondVersionWithGeneration.Name, secondVersionWithGeneration.Generation)
			if !versioningEnabled {
				shouldError(t, err)
				return
			}
			noError(t, err)
			if err := compareObjects(retrievedObject, secondVersionWithGeneration); err != nil {
				t.Errorf("get object by generation after removal - object retrieved differs from the created one. Descr: %v", err)
			}
		})
	}
}

func TestObjectQueryErrors(t *testing.T) {
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		testForStorageBackends(t, func(t *testing.T, storage Storage) {
			const bucketName = "random-bucket"
			err := storage.CreateBucket(bucketName, versioningEnabled)
			if reflect.TypeOf(storage) == reflect.TypeOf(&StorageFS{}) && versioningEnabled {
				t.Log("FS storage type should not implement versioning")
				shouldError(t, err)
				return
			}
			validObject := Object{BucketName: bucketName, Name: "random-object", Content: []byte("random-content")}
			err = storage.CreateObject(validObject)
			noError(t, err)
			_, err = storage.GetObjectWithGeneration(validObject.BucketName, validObject.Name, 33333)
			shouldError(t, err)
		})
	}
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
			{"prod-bucket", false, time.Time{}},
			{"prod-bucket-with-versioning", true, time.Time{}},
		}
		for i, bucket := range bucketsToTest {
			_, err := storage.GetBucket(bucket.Name)
			if err == nil {
				t.Fatalf("bucket %s, exists before being created", bucket.Name)
			}
			timeBeforeCreation := time.Now().Truncate(time.Second) // we may lose precission
			err = storage.CreateBucket(bucket.Name, bucket.VersioningEnabled)
			timeAfterCreation := time.Now()
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
			if !isBucketEquivalentTo(bucketFromStorage, bucket, timeBeforeCreation, timeAfterCreation) {
				t.Errorf("bucket %v does not have the expected props after retrieving. Expected %v and time between %v and %v",
					bucketFromStorage, bucket, timeBeforeCreation, timeAfterCreation)
			}
			buckets, err = storage.ListBuckets()
			if err != nil {
				t.Fatal(err)
			}
			if len(buckets) != i+1 {
				t.Errorf("number of buckets does not match the times we have lopped. Expected %d, found %d", i, len(buckets))
			}
			found := false
			for _, listedBucket := range buckets {
				if listedBucket.Name == bucket.Name {
					found = true
				}
			}
			if !found {
				t.Errorf("Bucket we have just created is not part of the bucket listing. Expected %s, results: %v", bucket.Name, buckets)
			}
		}
	})
}

func isBucketEquivalentTo(a, b Bucket, earliest, latest time.Time) bool {
	return a.Name == b.Name &&
		a.VersioningEnabled == b.VersioningEnabled &&
		a.TimeCreated.After(earliest) && a.TimeCreated.Before(latest)
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

func compareObjects(o1, o2 Object) error {
	if o1.BucketName != o2.BucketName {
		return fmt.Errorf("bucket name differs:\nmain %q\narg  %q", o1.BucketName, o2.BucketName)
	}
	if o1.Name != o2.Name {
		return fmt.Errorf("wrong object name:\nmain %q\narg  %q", o1.Name, o2.Name)
	}
	if o1.ContentType != o2.ContentType {
		return fmt.Errorf("wrong object contenttype:\nmain %q\narg  %q", o1.ContentType, o2.ContentType)
	}
	if o1.Crc32c != o2.Crc32c {
		return fmt.Errorf("wrong crc:\nmain %q\narg  %q", o1.Crc32c, o2.Crc32c)
	}
	if o1.Md5Hash != o2.Md5Hash {
		return fmt.Errorf("wrong md5:\nmain %q\narg  %q", o1.Md5Hash, o2.Md5Hash)
	}
	if o1.Generation != 0 && o2.Generation != 0 && o1.Generation != o2.Generation {
		return fmt.Errorf("generations different from 0, but not equal:\nmain %q\narg  %q", o1.Generation, o2.Generation)
	}
	if !bytes.Equal(o1.Content, o2.Content) {
		return fmt.Errorf("wrong object content:\nmain %q\narg  %q", o1.Content, o2.Content)
	}
	return nil
}
