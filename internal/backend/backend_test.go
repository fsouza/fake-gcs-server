// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/internal/checksum"
)

func makeStorageBackends(t *testing.T) (map[string]Storage, func()) {
	tempDir, err := os.MkdirTemp("", "fakegcstest")
	if err != nil {
		t.Fatal(err)
	}
	storageFS, err := NewStorageFS(nil, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	storageMemory, err := NewStorageMemory(nil)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]Storage{
			"memory":     storageMemory,
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
		t.Fatal("should error, but error is nil")
	}
}

func uploadAndCompare(t *testing.T, storage Storage, obj Object) int64 {
	isFSStorage := reflect.TypeOf(storage) == reflect.TypeOf(&storageFS{})
	newObject, err := storage.CreateObject(obj.StreamingObject(), NoConditions{})
	if isFSStorage && obj.Generation != 0 {
		t.Log("FS should not support objects generation")
		shouldError(t, err)
		obj.Generation = 0
		newObject, err = storage.CreateObject(obj.StreamingObject(), NoConditions{})
	}
	noError(t, err)
	newObject.Close()
	activeObj, err := storage.GetObject(obj.BucketName, obj.Name)
	noError(t, err)
	if activeObj.Generation == 0 {
		t.Errorf("generation is empty, but we expect a unique int")
	}
	if err := compareStreamingObjects(activeObj, obj.StreamingObject()); err != nil {
		t.Errorf("object retrieved differs from the created one. Descr: %v", err)
	}
	activeObj.Close()
	objFromGeneration, err := storage.GetObjectWithGeneration(obj.BucketName, obj.Name, activeObj.Generation)
	noError(t, err)
	if err := compareStreamingObjects(objFromGeneration, obj.StreamingObject()); err != nil {
		t.Errorf("object retrieved differs from the created one. Descr: %v", err)
	}
	objFromGeneration.Close()
	return activeObj.Generation
}

func TestObjectCRUD(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("time resolution on Windows makes this test flaky on that platform")
	}

	const bucketName = "prod-bucket"
	const objectName = "video/hi-res/best_video_1080p.mp4"
	content1 := []byte("content1")
	crc1 := checksum.EncodedCrc32cChecksum(content1)
	md51 := checksum.EncodedMd5Hash(content1)
	content2 := []byte("content2")
	crc2 := checksum.EncodedCrc32cChecksum(content2)
	md52 := checksum.EncodedMd5Hash(content2)
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		testForStorageBackends(t, func(t *testing.T, storage Storage) {
			// Get in non-existent case
			_, err := storage.GetObject(bucketName, objectName)
			shouldError(t, err)
			// Delete in non-existent case
			err = storage.DeleteObject(bucketName, objectName)
			shouldError(t, err)
			err = storage.CreateBucket(bucketName, BucketAttrs{VersioningEnabled: versioningEnabled})
			if reflect.TypeOf(storage) == reflect.TypeOf(&storageFS{}) && versioningEnabled {
				t.Log("FS storage type should not implement versioning")
				shouldError(t, err)
				return
			}

			initialObject := Object{
				ObjectAttrs: ObjectAttrs{
					BucketName: bucketName,
					Name:       objectName,
					Crc32c:     crc1,
					Md5Hash:    md51,
				},
				Content: content1,
			}
			t.Logf("create an initial object on an empty bucket with versioning %t", versioningEnabled)
			initialGeneration := uploadAndCompare(t, storage, initialObject)

			t.Logf("create (update) in existent case with explicit generation and versioning %t", versioningEnabled)
			secondVersionWithGeneration := Object{
				ObjectAttrs: ObjectAttrs{
					BucketName: bucketName,
					Name:       objectName,
					Generation: 1234,
					Crc32c:     crc2,
					Md5Hash:    md52,
				},
				Content: content2,
			}
			uploadAndCompare(t, storage, secondVersionWithGeneration)

			initialObjectFromGeneration, err := storage.GetObjectWithGeneration(initialObject.BucketName, initialObject.Name, initialGeneration)
			if !versioningEnabled {
				shouldError(t, err)
			} else {
				noError(t, err)
				if err := compareStreamingObjects(initialObjectFromGeneration, initialObject.StreamingObject()); err != nil {
					t.Errorf("get initial generation - object retrieved differs from the created one. Descr: %v", err)
				}
			}

			t.Logf("checking active object is the expected one when versioning is %t", versioningEnabled)
			objs, err := storage.ListObjects(bucketName, "", false)
			noError(t, err)
			if len(objs) != 1 {
				t.Errorf("wrong number of objects returned\nwant 1\ngot  %d", len(objs))
			}
			if objs[0].Name != objectName {
				t.Errorf("wrong object name\nwant %q\ngot  %q", objectName, objs[0].Name)
			}

			t.Logf("checking all object listing is the expected one when versioning is %t", versioningEnabled)
			objs, err = storage.ListObjects(bucketName, "", true)
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
			if err := compareStreamingObjects(retrievedObject, secondVersionWithGeneration.StreamingObject()); err != nil {
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
			err := storage.CreateBucket(bucketName, BucketAttrs{VersioningEnabled: versioningEnabled})
			if reflect.TypeOf(storage) == reflect.TypeOf(&storageFS{}) && versioningEnabled {
				t.Log("FS storage type should not implement versioning")
				shouldError(t, err)
				return
			}
			validObject := Object{
				ObjectAttrs: ObjectAttrs{
					BucketName: bucketName,
					Name:       "random-object",
				},
				Content: []byte("random-content"),
			}
			obj, err := storage.CreateObject(validObject.StreamingObject(), NoConditions{})
			noError(t, err)
			obj.Close()
			_, err = storage.GetObjectWithGeneration(validObject.BucketName, validObject.Name, 33333)
			shouldError(t, err)
		})
	}
}

func TestBucketAttrsUpdateVersioning(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		bucketName := "randombucket"
		initBucketAttrs := BucketAttrs{VersioningEnabled: false}
		updatedBucketAttrs := BucketAttrs{VersioningEnabled: true}
		err := storage.CreateBucket(bucketName, initBucketAttrs)
		if err != nil {
			t.Fatal(err)
		}
		err = storage.UpdateBucket(bucketName, updatedBucketAttrs)
		if reflect.TypeOf(storage) == reflect.TypeOf(&storageFS{}) {
			if err == nil {
				t.Fatal("fs storage should not accept updating buckets with versioning, but it's not failing")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			} else {
				bucket, err := storage.GetBucket(bucketName)
				if err != nil {
					t.Fatal(err)
				}
				if bucket.VersioningEnabled != updatedBucketAttrs.VersioningEnabled {
					t.Errorf("Expected versioning enabled to be %v, instead got %v", updatedBucketAttrs.VersioningEnabled, bucket.VersioningEnabled)
				}
			}
		}
	})
}

func TestBucketAttrsStoreRetrieveUpdate(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		bucketName := "randombucket"
		initBucketAttrs := BucketAttrs{DefaultEventBasedHold: true, VersioningEnabled: false}
		updatedBucketAttrs := BucketAttrs{DefaultEventBasedHold: false}
		err := storage.CreateBucket(bucketName, initBucketAttrs)
		if err != nil {
			t.Fatal(err)
		}
		bucket, err := storage.GetBucket(bucketName)
		if err != nil {
			t.Fatal(err)
		}
		if bucket.DefaultEventBasedHold != initBucketAttrs.DefaultEventBasedHold {
			t.Errorf("Expected bucket default event based hold to be true")
		}
		err = storage.UpdateBucket(bucketName, updatedBucketAttrs)
		if err != nil {
			t.Fatal(err)
		}
		bucket, err = storage.GetBucket(bucketName)
		if err != nil {
			t.Fatal(err)
		}
		if bucket.DefaultEventBasedHold != updatedBucketAttrs.DefaultEventBasedHold {
			t.Errorf("Expected bucket default event based hold to be false")
		}
	})
}

func TestBucketCreateGetListDelete(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		buckets, err := storage.ListBuckets()
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 0 {
			t.Fatalf("more than zero buckets found: %d, and expecting zero when starting the test", len(buckets))
		}
		bucketsToTest := []Bucket{
			{"prod-bucket", false, time.Time{}, false},
			{"prod-bucket-with-versioning", true, time.Time{}, false},
		}
		for _, bucket := range bucketsToTest {
			_, err := storage.GetBucket(bucket.Name)
			if err == nil {
				t.Fatalf("bucket %s, exists before being created", bucket.Name)
			}
			timeBeforeCreation := time.Now()
			err = storage.CreateBucket(bucket.Name, BucketAttrs{VersioningEnabled: bucket.VersioningEnabled})
			timeAfterCreation := time.Now()
			if reflect.TypeOf(storage) == reflect.TypeOf(&storageFS{}) && bucket.VersioningEnabled {
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
				t.Errorf("bucket %v does not have the expected props after retrieving. Expected %v and time roughly between %v and %v",
					bucketFromStorage, bucket, timeBeforeCreation, timeAfterCreation)
			}
			buckets, err = storage.ListBuckets()
			if err != nil {
				t.Fatal(err)
			}
			if len(buckets) != 1 {
				t.Errorf("found unexpected number of buckets. Expected 1, found %d", len(buckets))
			}
			if buckets[0].Name != bucket.Name {
				t.Errorf("listed bucket has unexpected name. Expected %s, actual: %v", bucket.Name, buckets[0].Name)
			}
			if !isTimeRoughlyInRange(buckets[0].TimeCreated, timeBeforeCreation, timeAfterCreation) {
				t.Errorf("listed bucket has unexpected creation time. Expected roughly between %v and %v, actual: %v", timeBeforeCreation, timeAfterCreation, buckets[0].TimeCreated)
			}
			err = storage.DeleteBucket(bucket.Name)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func isBucketEquivalentTo(a, b Bucket, before, after time.Time) bool {
	return a.Name == b.Name &&
		a.VersioningEnabled == b.VersioningEnabled &&
		isTimeRoughlyInRange(a.TimeCreated, before, after)
}

func isTimeRoughlyInRange(t, before, after time.Time) bool {
	// The FS backend uses filesystem timestamps to store bucket creation time.
	// Use a large +/- 5 second window to allow for an imperfectly synchronized
	// clock generating the filesystem timestamp and to reduce test flakes.
	earliest := before.Add(-5 * time.Second)
	latest := after.Add(5 * time.Second)
	return t.After(earliest) && t.Before(latest)
}

func TestBucketDuplication(t *testing.T) {
	const bucketName = "prod-bucket"
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		err := storage.CreateBucket(bucketName, BucketAttrs{VersioningEnabled: false})
		if err != nil {
			t.Fatal(err)
		}

		err = storage.CreateBucket(bucketName, BucketAttrs{VersioningEnabled: true})
		if err == nil {
			t.Fatal("we were expecting a bucket duplication error")
		}
	})
}

func TestParallelUploadsDifferentObjects(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		const bucketName = "parallel-test-bucket"
		const numObjects = 10

		err := storage.CreateBucket(bucketName, BucketAttrs{})
		noError(t, err)

		// Use a channel to synchronize goroutines starting together
		start := make(chan struct{})
		errCh := make(chan error, numObjects)

		for i := 0; i < numObjects; i++ {
			go func(idx int) {
				<-start // Wait for signal to start
				objectName := fmt.Sprintf("object-%d", idx)
				content := []byte(fmt.Sprintf("content for object %d", idx))
				obj := StreamingObject{
					ObjectAttrs: ObjectAttrs{
						BucketName: bucketName,
						Name:       objectName,
					},
					Content: newStreamingContent(content),
				}
				created, err := storage.CreateObject(obj, NoConditions{})
				if err != nil {
					errCh <- fmt.Errorf("failed to create object %d: %w", idx, err)
					return
				}
				created.Close()
				errCh <- nil
			}(i)
		}

		// Start all goroutines at once
		close(start)

		// Wait for all to complete
		for i := 0; i < numObjects; i++ {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}

		// Verify all objects were created
		objects, err := storage.ListObjects(bucketName, "", false)
		noError(t, err)
		if len(objects) != numObjects {
			t.Errorf("expected %d objects, got %d", numObjects, len(objects))
		}
	})
}

func TestParallelDownloadsSameObject(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		const bucketName = "parallel-download-bucket"
		const objectName = "shared-object"
		const numReaders = 10
		content := []byte("shared content for parallel reads")

		err := storage.CreateBucket(bucketName, BucketAttrs{})
		noError(t, err)

		obj := StreamingObject{
			ObjectAttrs: ObjectAttrs{
				BucketName: bucketName,
				Name:       objectName,
			},
			Content: newStreamingContent(content),
		}
		created, err := storage.CreateObject(obj, NoConditions{})
		noError(t, err)
		created.Close()

		// Use a channel to synchronize goroutines starting together
		start := make(chan struct{})
		errCh := make(chan error, numReaders)

		for i := 0; i < numReaders; i++ {
			go func(idx int) {
				<-start // Wait for signal to start
				retrieved, err := storage.GetObject(bucketName, objectName)
				if err != nil {
					errCh <- fmt.Errorf("reader %d failed to get object: %w", idx, err)
					return
				}
				defer retrieved.Close()

				data, err := io.ReadAll(retrieved.Content)
				if err != nil {
					errCh <- fmt.Errorf("reader %d failed to read content: %w", idx, err)
					return
				}
				if !bytes.Equal(data, content) {
					errCh <- fmt.Errorf("reader %d got wrong content: %q", idx, data)
					return
				}
				errCh <- nil
			}(i)
		}

		// Start all goroutines at once
		close(start)

		// Wait for all to complete
		for i := 0; i < numReaders; i++ {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}
	})
}

func TestParallelUploadAndDownloadDifferentObjects(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		const bucketName = "parallel-mixed-bucket"
		const existingObject = "existing-object"
		const newObject = "new-object"
		existingContent := []byte("existing content")
		newContent := []byte("new content being uploaded")

		err := storage.CreateBucket(bucketName, BucketAttrs{})
		noError(t, err)

		// Create an existing object to download
		obj := StreamingObject{
			ObjectAttrs: ObjectAttrs{
				BucketName: bucketName,
				Name:       existingObject,
			},
			Content: newStreamingContent(existingContent),
		}
		created, err := storage.CreateObject(obj, NoConditions{})
		noError(t, err)
		created.Close()

		// Use a channel to synchronize goroutines starting together
		start := make(chan struct{})
		errCh := make(chan error, 2)

		// Goroutine 1: Download existing object
		go func() {
			<-start
			retrieved, err := storage.GetObject(bucketName, existingObject)
			if err != nil {
				errCh <- fmt.Errorf("download failed: %w", err)
				return
			}
			defer retrieved.Close()

			data, err := io.ReadAll(retrieved.Content)
			if err != nil {
				errCh <- fmt.Errorf("read failed: %w", err)
				return
			}
			if !bytes.Equal(data, existingContent) {
				errCh <- fmt.Errorf("wrong content: %q", data)
				return
			}
			errCh <- nil
		}()

		// Goroutine 2: Upload new object
		go func() {
			<-start
			newObj := StreamingObject{
				ObjectAttrs: ObjectAttrs{
					BucketName: bucketName,
					Name:       newObject,
				},
				Content: newStreamingContent(newContent),
			}
			created, err := storage.CreateObject(newObj, NoConditions{})
			if err != nil {
				errCh <- fmt.Errorf("upload failed: %w", err)
				return
			}
			created.Close()
			errCh <- nil
		}()

		// Start both goroutines at once
		close(start)

		// Wait for both to complete
		for i := 0; i < 2; i++ {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}

		// Verify both objects exist
		objects, err := storage.ListObjects(bucketName, "", false)
		noError(t, err)
		if len(objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(objects))
		}
	})
}

func TestParallelUploadsSameObject(t *testing.T) {
	testForStorageBackends(t, func(t *testing.T, storage Storage) {
		const bucketName = "parallel-same-object-bucket"
		const objectName = "contested-object"
		const numWriters = 10

		err := storage.CreateBucket(bucketName, BucketAttrs{})
		noError(t, err)

		// Use a channel to synchronize goroutines starting together
		start := make(chan struct{})
		errCh := make(chan error, numWriters)

		for i := 0; i < numWriters; i++ {
			go func(idx int) {
				<-start // Wait for signal to start
				content := []byte(fmt.Sprintf("content from writer %d", idx))
				obj := StreamingObject{
					ObjectAttrs: ObjectAttrs{
						BucketName: bucketName,
						Name:       objectName,
					},
					Content: newStreamingContent(content),
				}
				created, err := storage.CreateObject(obj, NoConditions{})
				if err != nil {
					errCh <- fmt.Errorf("writer %d failed: %w", idx, err)
					return
				}
				created.Close()
				errCh <- nil
			}(i)
		}

		// Start all goroutines at once
		close(start)

		// Wait for all to complete - all should succeed (last write wins)
		for i := 0; i < numWriters; i++ {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}

		// Verify exactly one object exists
		objects, err := storage.ListObjects(bucketName, "", false)
		noError(t, err)
		if len(objects) != 1 {
			t.Errorf("expected 1 object, got %d", len(objects))
		}
	})
}

// newStreamingContent creates a ReadSeekCloser from a byte slice
func newStreamingContent(data []byte) io.ReadSeekCloser {
	return &bytesReadSeekCloser{Reader: bytes.NewReader(data)}
}

type bytesReadSeekCloser struct {
	*bytes.Reader
}

func (b *bytesReadSeekCloser) Close() error {
	return nil
}

func compareStreamingObjects(o1, o2 StreamingObject) error {
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
	content1, err := io.ReadAll(o1.Content)
	if err != nil {
		return fmt.Errorf("count not read content from o1: %w", err)
	}
	content2, err := io.ReadAll(o2.Content)
	if err != nil {
		return fmt.Errorf("count not read content from o2: %w", err)
	}
	if !bytes.Equal(content1, content2) {
		return fmt.Errorf("wrong object content:\nmain %q\narg  %q", content1, content2)
	}
	return nil
}
