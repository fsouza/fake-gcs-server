package fakestorage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/jonmseaman/gcs-xml-multipart-client/multipartclient"
)

func TestInitiateMultipartUpload(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	tests := []struct {
		req      *multipartclient.InitiateMultipartUploadRequest
		wantResp *multipartclient.InitiateMultipartUploadResult
	}{
		{
			req: &multipartclient.InitiateMultipartUploadRequest{
				Bucket: "test-bucket",
				Key:    "file1.txt",
			},
			wantResp: &multipartclient.InitiateMultipartUploadResult{
				Bucket:   "test-bucket",
				Key:      "file1.txt",
				UploadID: "*",
			},
		},
		{
			req: &multipartclient.InitiateMultipartUploadRequest{
				Bucket: "test-bucket",
				Key:    "file2.txt",
			},
		},
		{
			req: &multipartclient.InitiateMultipartUploadRequest{
				Bucket: "test-bucket",
				Key:    "filee.txt",
			},
		},
		{
			// Repeating an object should still work.
			req: &multipartclient.InitiateMultipartUploadRequest{
				Bucket: "test-bucket",
				Key:    "filee.txt",
			},
		},
	}

	for _, tc := range tests {
		mpuc := multipartclient.New(client)
		ctx := context.Background()
		resp, err := mpuc.InitiateMultipartUpload(ctx, tc.req)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Response: %+v", resp)
		if resp.Bucket != tc.req.Bucket {
			t.Errorf("unexpected bucket: got %v, want %v", resp.Bucket, tc.req.Bucket)
		}
		if resp.Key != tc.req.Key {
			t.Errorf("unexpected object key: got %v, want %v", resp.Key, tc.req.Key)
		}
	}

	// Verify uploads stored in server:
	uploadCount := 0
	server.mpus.Range(func(key, value any) bool {
		uploadCount++
		return true
	})
	if uploadCount != len(tests) {
		t.Errorf("unexpected upload count, got %v, want %v", uploadCount, len(tests))
	}
}

func strToReadCloser(str string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(str))
}

func TestUploadObjectPart(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	// Create an upload to use.
	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "object.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadId := resp.UploadID

	// Upload a part.
	err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 1,
		Body:       strToReadCloser("my content"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the part is in the server.
	val, ok := server.mpus.Load(uploadId)
	if !ok {
		t.Fatalf("upload id not found in server")
	}

	mpu := val.(*multipartUpload)
	_, ok = mpu.parts[1]
	if !ok {
		t.Fatalf("part not found in upload")
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	// Create an upload to use.
	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "object.txt",
	})
	if err != nil {
		t.Fatal(err)
	}

	uploadId := resp.UploadID
	abortReq := &multipartclient.AbortMultipartUploadRequest{
		Bucket:   "test-bucket",
		Key:      "object.txt",
		UploadID: uploadId,
	}
	err = mpuc.AbortMultipartUpload(ctx, abortReq)
	if err != nil {
		t.Fatalf("Failed to abort the upload: %v", err)
	}

	err = mpuc.AbortMultipartUpload(ctx, abortReq)
	if err == nil || !strings.Contains(err.Error(), "Not Found") {
		t.Fatalf("Abort should fail if the upload id does not exist")
	}
}

func TestListMultipartUploads(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	// Create uploads to use.
	mpuc := multipartclient.New(client)
	ctx := context.Background()
	initCount := 3
	for i := 0; i < initCount; i++ {
		_, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "object.txt",
		})
		if err != nil {
			t.Fatal(err)

		}
	}

	// List uploads
	resp, err := mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Uploads) != initCount {
		t.Errorf("unexpected number of uploads: got %v, want %v", len(resp.Uploads), initCount)

	}
}

func TestListObjectParts(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "object.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadId := resp.UploadID
	// Upload a part.
	err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 1,
		Body:       strToReadCloser("my content"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// List object parts
	listResp, err := mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:   "test-bucket",
		Key:      "object.txt",
		UploadID: uploadId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 1 {
		t.Errorf("unexpected number of parts: got %v, want %v", len(listResp.Parts), 1)
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "object.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadId := resp.UploadID
	// Upload a part.
	err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 1,
		Body:       strToReadCloser("my content"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Complete the upload.
	completeResp, err := mpuc.CompleteMultipartUpload(ctx, &multipartclient.CompleteMultipartUploadRequest{
		Bucket:   "test-bucket",
		Key:      "object.txt",
		UploadID: uploadId,
		Body: multipartclient.CompleteMultipartUploadBody{
			Parts: []multipartclient.CompletePart{
				{
					PartNumber: 1,
					Etag:       "*",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to complete the upload: %v", err)
	}
	if completeResp.Bucket != "test-bucket" {
		t.Errorf("unexpected bucket: got %v, want %v", completeResp.Bucket, "test-bucket")
	}
	if completeResp.Key != "object.txt" {
		t.Errorf("unexpected object key: got %v, want %v", completeResp.Key, "object.txt")
	}
}
