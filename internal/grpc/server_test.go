package grpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"testing"

	pb "github.com/fsouza/fake-gcs-server/genproto/googleapis/storage/v1"
	"github.com/fsouza/fake-gcs-server/internal/backend"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
)

func tempDir() string {
	if runtime.GOOS == "linux" {
		return "/var/tmp"
	} else {
		return os.TempDir()
	}
}

func makeStorageBackends(t *testing.T) (map[string]backend.Storage, func()) {
	tempDir, err := os.MkdirTemp(tempDir(), "fakegcstest-grpc")
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
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", grpcObj.Name, expectedAttrs.Name)
	}
	if grpcObj.Md5Hash != expectedAttrs.Md5Hash {
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", grpcObj.Md5Hash, expectedAttrs.Md5Hash)
	}
	if grpcObj.Generation != expectedAttrs.Generation {
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", grpcObj.Generation, expectedAttrs.Generation)
	}
	if grpcObj.ContentType != expectedAttrs.ContentType {
		t.Errorf("Unexpected bucket name '%q'. Expected '%q'", grpcObj.ContentType, expectedAttrs.ContentType)
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
		_, err := grpcServer.InsertObject(ctx, &pb.InsertObjectRequest{
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
		})
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
		grpc_get_obj_resp, err := grpcServer.GetObject(ctx, &pb.GetObjectRequest{
			Bucket: bucketName,
			Object: obj1Name,
		})
		if err != nil {
			t.Fatal(err)
		}
		compareGrpcObjAttrs(t, grpc_get_obj_resp.Metadata, obj.ObjectAttrs)
		if !bytes.Equal(grpc_get_obj_resp.ChecksummedData.Content, content) {
			t.Errorf("Wrong object content. Expected '%q', Got '%q'", grpc_get_obj_resp.ChecksummedData.Content, content)
		}

		// Test List Objects
		objs, err := grpcServer.ListObjects(ctx, &pb.ListObjectsRequest{
			Bucket:   bucketName,
			Prefix:   "object",
			Versions: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(objs.Object) != 1 {
			t.Errorf("Wrong object length. Expected 1, got %q", len(objs.Object))
		}
		compareGrpcObjAttrs(t, objs.Object[0], obj.ObjectAttrs)

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
		if !reflect.DeepEqual(obj.ObjectAttrs.Metadata, newMetadata) {
			t.Errorf("Wrong object metadata. Expected '%q', Got '%q'", obj.ObjectAttrs.Metadata, newMetadata)
		}

		// Insert another object
		obj2Name := "object2"
		obj2Content := []byte("object2-content")
		_, err = grpcServer.InsertObject(ctx, &pb.InsertObjectRequest{
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
		})
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
		expected_content := []byte("object1-contentobject2-content")
		contentEqual(t, obj.Content, expected_content)
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
				Name: bucketName,
				DefaultEventBasedHold: true,
			},
		})
		if err != nil {
			t.Errorf("Error while inserting bucket: %v", err)
		}
		bucket, storage_err := storage.GetBucket(bucketName)
		if storage_err != nil {
			t.Errorf("Error while getting bucket from backend: %v", storage_err)
		}
		if bucket.Name != bucketName {
			t.Errorf("Expected '%s', got '%s'", bucketName, bucket.Name)
		}

		// Test GRPC GetBucket endpoint
		grpc_bucket, get_err := grpcServer.GetBucket(ctx, &pb.GetBucketRequest{
			Bucket: bucketName,
		})
		if get_err != nil {
			t.Errorf("Error while getting bucket from grpc: %v", err)
		}
		if grpc_bucket.Name != bucketName {
			t.Errorf("Expected '%s', got '%s'", bucketName, grpc_bucket.Name)
		}
		if !grpc_bucket.DefaultEventBasedHold {
			t.Errorf("Expected true, got '%v'", grpc_bucket.DefaultEventBasedHold)
		}

		// Test GRPC ListBucket endpoint
		grpc_buckets, list_err := grpcServer.ListBuckets(ctx, &pb.ListBucketsRequest{})
		if list_err != nil {
			t.Errorf("List bucket error")
		}
		if len(grpc_buckets.Bucket) != 1 {
			t.Fatal("List buckets wrong length")
		}
		if grpc_buckets.Bucket[0].Name != bucketName {
			t.Errorf("Wrong bucket name")
		}

		// Test GRPC UpdateBucket endpoint
		_, update_err := grpcServer.UpdateBucket(ctx, &pb.UpdateBucketRequest{
			Bucket: bucketName,
			Metadata: &pb.Bucket{
				DefaultEventBasedHold: false,
			},
		})
		if update_err != nil {
			t.Fatal(update_err)
		}
		grpc_bucket, err = grpcServer.GetBucket(ctx, &pb.GetBucketRequest{
			Bucket: bucketName,
		})
		if err != nil {
			t.Fatal(err)
		}
		if grpc_bucket.DefaultEventBasedHold {
			t.Errorf("After update, expected false, got '%v'", grpc_bucket.DefaultEventBasedHold)
		}

		// Test GRPC DeleteBucket endpoint
		_, delete_err := grpcServer.DeleteBucket(ctx, &pb.DeleteBucketRequest{
			Bucket: bucketName,
		})
		if delete_err != nil {
			t.Errorf("Got unexpected error when call 'DeleteBucket' endpoint of the GRPC server: %v", delete_err)
		}
		_, storage_err = storage.GetBucket(bucketName)
		if storage_err == nil {
			t.Errorf("Expected to get err when getting '%s' from backend, but got none", bucketName)
		}
	})
}
