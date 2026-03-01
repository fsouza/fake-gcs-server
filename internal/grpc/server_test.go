package grpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/fsouza/fake-gcs-server/internal/backend"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
	pb "google.golang.org/genproto/googleapis/storage/v1"
)

func makeStorageBackends(t *testing.T) (map[string]backend.Storage, func()) {
	tempDir, err := os.MkdirTemp("", "fakegcstest-grpc")
	if err != nil {
		t.Fatal(err)
	}
	storageFS, err := backend.NewStorageFS(nil, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	storageMemory, err := backend.NewStorageMemory(nil)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]backend.Storage{
			"memory":     storageMemory,
			"filesystem": storageFS,
		}, func() {
			os.RemoveAll(tempDir)
		}
}

func testForStorageBackends(t *testing.T, test func(t *testing.T, storage backend.Storage)) {
	backends, cleanup := makeStorageBackends(t)
	defer cleanup()
	for backendName, storage := range backends {
		storage := storage
		t.Run(fmt.Sprintf("storage backend %s", backendName), func(t *testing.T) {
			test(t, storage)
		})
	}
}

func contentEqual(t *testing.T, contentToRead io.Reader, expectedContent []byte) {
	givenContent, err := io.ReadAll(contentToRead)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expectedContent, givenContent) {
		t.Errorf("Wrong object content. Expected '%q', Got '%q'", expectedContent, givenContent)
	}
}

func compareObjectAttrs(t *testing.T, expectedAttrs, givenAttrs backend.ObjectAttrs) {
	if expectedAttrs.BucketName != givenAttrs.BucketName {
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", givenAttrs.BucketName, expectedAttrs.BucketName)
	}
	if expectedAttrs.Name != givenAttrs.Name {
		t.Errorf("Unexpected object name '%q'. Expected '%q'", givenAttrs.Name, expectedAttrs.Name)
	}
	if expectedAttrs.Md5Hash != givenAttrs.Md5Hash {
		t.Errorf("Wrong Md5. Got '%q', Expected '%q'", givenAttrs.Md5Hash, expectedAttrs.Md5Hash)
	}
	if expectedAttrs.Crc32c != givenAttrs.Crc32c {
		t.Errorf("Wrong Crc32c. Got '%q', Expected '%q'", givenAttrs.Crc32c, expectedAttrs.Crc32c)
	}
}

func compareGrpcObjAttrs(t *testing.T, grpcObj *pb.Object, expectedAttrs backend.ObjectAttrs) {
	if grpcObj.Bucket != expectedAttrs.BucketName {
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", grpcObj.Bucket, expectedAttrs.BucketName)
	}
	if grpcObj.Name != expectedAttrs.Name {
		t.Errorf("Unexpected object name '%q'. Expected '%q'", grpcObj.Name, expectedAttrs.Name)
	}
	if grpcObj.Md5Hash != expectedAttrs.Md5Hash {
		t.Errorf("Unexpected Md5Hash '%q'. Expected '%q'", grpcObj.Md5Hash, expectedAttrs.Md5Hash)
	}
	if grpcObj.Generation != expectedAttrs.Generation {
		t.Errorf("Unexpected Generation '%d'. Expected '%d'", grpcObj.Generation, expectedAttrs.Generation)
	}
	if grpcObj.ContentType != expectedAttrs.ContentType {
		t.Errorf("Unexpected ContentType '%q'. Expected '%q'", grpcObj.ContentType, expectedAttrs.ContentType)
	}
	if grpcObj.ContentEncoding != expectedAttrs.ContentEncoding {
		t.Errorf("Unexpected ContentEncoding '%q'. Expected '%q'", grpcObj.ContentEncoding, expectedAttrs.ContentEncoding)
	}
	if grpcObj.ContentDisposition != expectedAttrs.ContentDisposition {
		t.Errorf("Unexpected ContentDisposition '%q'. Expected '%q'", grpcObj.ContentDisposition, expectedAttrs.ContentDisposition)
	}
	if grpcObj.ContentLanguage != expectedAttrs.ContentLanguage {
		t.Errorf("Unexpected ContentLanguage '%q'. Expected '%q'", grpcObj.ContentLanguage, expectedAttrs.ContentLanguage)
	}
	if grpcObj.CacheControl != expectedAttrs.CacheControl {
		t.Errorf("Unexpected CacheControl '%q'. Expected '%q'", grpcObj.CacheControl, expectedAttrs.CacheControl)
	}
	if !reflect.DeepEqual(grpcObj.Metadata, expectedAttrs.Metadata) {
		t.Errorf("Unexpected Metadata '%v'. Expected '%v'", grpcObj.Metadata, expectedAttrs.Metadata)
	}
}

func TestObjectInsertGetUpdateCompose(t *testing.T) {
	ctx := context.Background()

	testForStorageBackends(t, func(t *testing.T, storage backend.Storage) {
		// Set up
		grpcServer := InitServer(storage)
		bucketName := "bucket1"
		storage.CreateBucket(bucketName, backend.BucketAttrs{})

		// Object Attributes
		obj1Name := "object1"
		content := []byte("object1-content")
		obj1Crc := checksum.EncodedCrc32cChecksum(content)
		obj1Hash := checksum.EncodedMd5Hash(content)
		origMetadata := map[string]string{
			"1-key": "1",
			"2-key": "2",
		}

		expectedObjectAttrs := backend.ObjectAttrs{
			BucketName: bucketName,
			Name:       obj1Name,
			Md5Hash:    obj1Hash,
			Crc32c:     obj1Crc,
			Metadata:   origMetadata,
		}
		// Test Insert Object
		req := &pb.InsertObjectRequest{
			FirstMessage: &pb.InsertObjectRequest_InsertObjectSpec{
				InsertObjectSpec: &pb.InsertObjectSpec{
					Resource: &pb.Object{
						Bucket:   bucketName,
						Name:     obj1Name,
						Metadata: origMetadata,
					},
				},
			},
			Data: &pb.InsertObjectRequest_ChecksummedData{
				ChecksummedData: &pb.ChecksummedData{
					Content: content,
				},
			},
		}
		err := grpcServer.InsertObject(&fakeInsertObjectServer{req: req})
		if err != nil {
			t.Fatal(err)
		}

		obj, err := storage.GetObject(bucketName, obj1Name)
		if err != nil {
			t.Fatal(err)
		}
		compareObjectAttrs(t, expectedObjectAttrs, obj.ObjectAttrs)
		contentEqual(t, obj.Content, content)

		// Test Get Object
		grpcObj, err := grpcServer.GetObject(ctx, &pb.GetObjectRequest{
			Bucket: bucketName,
			Object: obj1Name,
		})
		if err != nil {
			t.Fatal(err)
		}
		compareGrpcObjAttrs(t, grpcObj, obj.ObjectAttrs)

		// Test List Objects
		objs, err := grpcServer.ListObjects(ctx, &pb.ListObjectsRequest{
			Bucket:   bucketName,
			Prefix:   "object",
			Versions: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(objs.Items) != 1 {
			t.Errorf("Wrong object length. Expected 1, got %q", len(objs.Items))
		}
		compareGrpcObjAttrs(t, objs.Items[0], obj.ObjectAttrs)

		// Test Patch Object
		newMetadata := map[string]string{
			"1-key": "3",
			"2-key": "4",
		}
		_, err = grpcServer.PatchObject(ctx, &pb.PatchObjectRequest{
			Bucket: bucketName,
			Object: obj1Name,
			Metadata: &pb.Object{
				Metadata: newMetadata,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		obj, err = storage.GetObject(bucketName, obj1Name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(obj.Metadata, newMetadata) {
			t.Errorf("Wrong object metadata. Expected '%q', Got '%q'", obj.ObjectAttrs.Metadata, newMetadata)
		}

		// Insert another object
		obj2Name := "object2"
		obj2Content := []byte("object2-content")
		req = &pb.InsertObjectRequest{
			FirstMessage: &pb.InsertObjectRequest_InsertObjectSpec{
				InsertObjectSpec: &pb.InsertObjectSpec{
					Resource: &pb.Object{
						Bucket:   bucketName,
						Name:     obj2Name,
						Metadata: origMetadata,
					},
				},
			},
			Data: &pb.InsertObjectRequest_ChecksummedData{
				ChecksummedData: &pb.ChecksummedData{
					Content: obj2Content,
				},
			},
		}
		err = grpcServer.InsertObject(&fakeInsertObjectServer{req: req})
		if err != nil {
			t.Fatal(err)
		}

		// Test Compose
		composedObjName := "object3"
		_, err = grpcServer.ComposeObject(ctx, &pb.ComposeObjectRequest{
			DestinationBucket: bucketName,
			DestinationObject: composedObjName,
			SourceObjects: []*pb.ComposeObjectRequest_SourceObjects{
				{
					Name: obj1Name,
				},
				{
					Name: obj2Name,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		obj, err = storage.GetObject(bucketName, "object3")
		if err != nil {
			t.Fatal(err)
		}
		expectedContent := []byte("object1-contentobject2-content")
		contentEqual(t, obj.Content, expectedContent)
	})
}

func TestBucketInsertGetListUpdateDelete(t *testing.T) {
	ctx := context.Background()

	testForStorageBackends(t, func(t *testing.T, storage backend.Storage) {
		// Insert Bucket and test if it gets placed in backend
		grpcServer := InitServer(storage)
		bucketName := "bucket5"
		_, err := grpcServer.InsertBucket(ctx, &pb.InsertBucketRequest{
			Bucket: &pb.Bucket{
				Name:                  bucketName,
				DefaultEventBasedHold: true,
			},
		})
		if err != nil {
			t.Errorf("Error while inserting bucket: %v", err)
		}
		bucket, storageErr := storage.GetBucket(bucketName)
		if storageErr != nil {
			t.Errorf("Error while getting bucket from backend: %v", storageErr)
		}
		if bucket.Name != bucketName {
			t.Errorf("Expected '%s', got '%s'", bucketName, bucket.Name)
		}

		// Test GRPC GetBucket endpoint
		grpcBucket, getErr := grpcServer.GetBucket(ctx, &pb.GetBucketRequest{
			Bucket: bucketName,
		})
		if getErr != nil {
			t.Errorf("Error while getting bucket from grpc: %v", err)
		}
		if grpcBucket.Name != bucketName {
			t.Errorf("Expected '%s', got '%s'", bucketName, grpcBucket.Name)
		}
		if !grpcBucket.DefaultEventBasedHold {
			t.Errorf("Expected true, got '%v'", grpcBucket.DefaultEventBasedHold)
		}

		// Test GRPC ListBucket endpoint
		grpcBuckets, listErr := grpcServer.ListBuckets(ctx, &pb.ListBucketsRequest{})
		if listErr != nil {
			t.Errorf("List bucket error")
		}
		if len(grpcBuckets.Items) != 1 {
			t.Fatal("List buckets wrong length")
		}
		if grpcBuckets.Items[0].Name != bucketName {
			t.Errorf("Wrong bucket name")
		}

		// Test GRPC UpdateBucket endpoint
		_, updateErr := grpcServer.UpdateBucket(ctx, &pb.UpdateBucketRequest{
			Bucket: bucketName,
			Metadata: &pb.Bucket{
				DefaultEventBasedHold: false,
			},
		})
		if updateErr != nil {
			t.Fatal(updateErr)
		}
		grpcBucket, err = grpcServer.GetBucket(ctx, &pb.GetBucketRequest{
			Bucket: bucketName,
		})
		if err != nil {
			t.Fatal(err)
		}
		if grpcBucket.DefaultEventBasedHold {
			t.Errorf("After update, expected false, got '%v'", grpcBucket.DefaultEventBasedHold)
		}

		// Test GRPC DeleteBucket endpoint
		_, deleteErr := grpcServer.DeleteBucket(ctx, &pb.DeleteBucketRequest{
			Bucket: bucketName,
		})
		if deleteErr != nil {
			t.Errorf("Got unexpected error when call 'DeleteBucket' endpoint of the GRPC server: %v", deleteErr)
		}
		_, storageErr = storage.GetBucket(bucketName)
		if storageErr == nil {
			t.Errorf("Expected to get err when getting '%s' from backend, but got none", bucketName)
		}
	})
}
