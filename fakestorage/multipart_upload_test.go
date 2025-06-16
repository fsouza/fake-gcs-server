package fakestorage

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"net/http"
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
				Key:    "file.txt",
			},
		},
		{
			// Repeating an object should still work.
			req: &multipartclient.InitiateMultipartUploadRequest{
				Bucket: "test-bucket",
				Key:    "file.txt",
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

func TestInitiateMultipartUploadSameObject(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	req := &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "file.txt",
	}

	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp1, err := mpuc.InitiateMultipartUpload(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	resp2, err := mpuc.InitiateMultipartUpload(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	if resp1.UploadID == resp2.UploadID {
		t.Errorf("expected different upload IDs for the same object, got %v and %v", resp1.UploadID, resp2.UploadID)
	}
}

func TestInitiateMultipartUploadCustomMetadata(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	req := &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "file.txt",
		CustomMetadata: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	mpuc := multipartclient.New(client)
	ctx := context.Background()
	resp, err := mpuc.InitiateMultipartUpload(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	val, ok := server.mpus.Load(resp.UploadID)
	if !ok {
		t.Fatalf("upload id not found in server")
	}
	mpu := val.(*multipartUpload)
	if mpu.Metadata["Key1"] != "value1" {
		t.Errorf("expected custom metadata key1 to be value1, got %v", mpu.Metadata["key1"])
	}
	if mpu.Metadata["Key2"] != "value2" {
		t.Errorf("expected custom metadata key2 to be value2, got %v", mpu.Metadata["key2"])
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
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
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
	part := mpu.parts[1]

	table := crc32.MakeTable(crc32.Castagnoli)
	crc32c := crc32.Checksum(part.Content, table)
	if crc32c != part.CRC32C {
		t.Errorf("expected crc32c to be %v, got %v", part.CRC32C, crc32c)
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
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
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
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
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

func TestListObjectPartsPagination(t *testing.T) {
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

	// Upload multiple parts in non-sequential order to test sorting
	partContents := map[int]string{
		1: "part 1 content",
		3: "part 3 content",
		2: "part 2 content",
		5: "part 5 content",
		4: "part 4 content",
	}

	for partNum, content := range partContents {
		_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
			Bucket:     "test-bucket",
			Key:        "object.txt",
			UploadID:   uploadId,
			PartNumber: partNum,
			Body:       strToReadCloser(content),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test 1: List all parts without pagination
	listResp, err := mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:   "test-bucket",
		Key:      "object.txt",
		UploadID: uploadId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 5 {
		t.Errorf("expected 5 parts, got %d", len(listResp.Parts))
	}
	if listResp.IsTruncated {
		t.Error("expected IsTruncated to be false for all parts")
	}
	if listResp.MaxParts != 1000 {
		t.Errorf("expected MaxParts to be 1000, got %d", listResp.MaxParts)
	}
	// Verify parts are sorted by part number
	expectedOrder := []int{1, 2, 3, 4, 5}
	for i, part := range listResp.Parts {
		if part.PartNumber != expectedOrder[i] {
			t.Errorf("expected part number %d at index %d, got %d", expectedOrder[i], i, part.PartNumber)
		}
	}

	// Test 2: List with max-parts=2
	listResp, err = mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:   "test-bucket",
		Key:      "object.txt",
		UploadID: uploadId,
		MaxParts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 2 {
		t.Errorf("expected 2 parts with max-parts=2, got %d", len(listResp.Parts))
	}
	if !listResp.IsTruncated {
		t.Error("expected IsTruncated to be true with max-parts=2")
	}
	if listResp.MaxParts != 2 {
		t.Errorf("expected MaxParts to be 2, got %d", listResp.MaxParts)
	}
	if listResp.NextPartNumberMarker != 3 {
		t.Errorf("expected NextPartNumberMarker to be 3, got %d", listResp.NextPartNumberMarker)
	}

	// Test 3: List with part-number-marker=2
	listResp, err = mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:           "test-bucket",
		Key:              "object.txt",
		UploadID:         uploadId,
		PartNumberMarker: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 3 {
		t.Errorf("expected 3 parts with part-number-marker=2, got %d", len(listResp.Parts))
	}
	if listResp.IsTruncated {
		t.Error("expected IsTruncated to be false when listing remaining parts")
	}
	if listResp.PartNumberMarker != 2 {
		t.Errorf("expected PartNumberMarker to be 2, got %d", listResp.PartNumberMarker)
	}
	// Should return parts 3, 4, 5
	expectedPartNumbers := []int{3, 4, 5}
	for i, part := range listResp.Parts {
		if part.PartNumber != expectedPartNumbers[i] {
			t.Errorf("expected part number %d at index %d, got %d", expectedPartNumbers[i], i, part.PartNumber)
		}
	}

	// Test 4: Combine part-number-marker and max-parts
	listResp, err = mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:           "test-bucket",
		Key:              "object.txt",
		UploadID:         uploadId,
		PartNumberMarker: 1,
		MaxParts:         2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 2 {
		t.Errorf("expected 2 parts with marker=1 and max-parts=2, got %d", len(listResp.Parts))
	}
	if !listResp.IsTruncated {
		t.Error("expected IsTruncated to be true with marker=1 and max-parts=2")
	}
	if listResp.NextPartNumberMarker != 4 {
		t.Errorf("expected NextPartNumberMarker to be 4, got %d", listResp.NextPartNumberMarker)
	}
	// Should return parts 2, 3
	expectedPartNumbers = []int{2, 3}
	for i, part := range listResp.Parts {
		if part.PartNumber != expectedPartNumbers[i] {
			t.Errorf("expected part number %d at index %d, got %d", expectedPartNumbers[i], i, part.PartNumber)
		}
	}

	// Test 5: part-number-marker beyond all parts
	listResp, err = mpuc.ListObjectParts(ctx, &multipartclient.ListObjectPartsRequest{
		Bucket:           "test-bucket",
		Key:              "object.txt",
		UploadID:         uploadId,
		PartNumberMarker: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Parts) != 0 {
		t.Errorf("expected 0 parts with marker=10, got %d", len(listResp.Parts))
	}
	if listResp.IsTruncated {
		t.Error("expected IsTruncated to be false when no parts remain")
	}
}

func TestUploadObjectPartValidation(t *testing.T) {
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

	// Test 1: Valid part upload should succeed
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 1,
		Body:       strToReadCloser("valid content"),
	})
	if err != nil {
		t.Errorf("Expected valid part upload to succeed, but got error: %v", err)
	}

	// Test 2: Invalid part number (too low) should fail
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 0, // Invalid: below minimum
		Body:       strToReadCloser("content"),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidPartNumber") {
		t.Errorf("Expected InvalidPartNumber error for part number 0, got: %v", err)
	}

	// Test 3: Invalid part number (too high) should fail
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 10001, // Invalid: above maximum
		Body:       strToReadCloser("content"),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidPartNumber") {
		t.Errorf("Expected InvalidPartNumber error for part number 10001, got: %v", err)
	}

	// Test 4: Part size too large should fail (>5GB)
	// Note: Not testing this because it would use a lot of memory.

	// Test 5: Verify part was actually stored
	val, ok := server.mpus.Load(uploadId)
	if !ok {
		t.Fatalf("upload id not found in server")
	}
	mpu := val.(*multipartUpload)
	if _, ok := mpu.parts[1]; !ok {
		t.Errorf("expected part 1 to be stored")
	}
}

func TestUploadObjectPartSizeValidation(t *testing.T) {
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

	// Test 1: Try uploading a part that's too large (simulate with ContentLength)
	// We can't actually create a 5GB+ body in memory, so we'll test with a smaller body
	// but set the ContentLength to exceed the limit
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:        "test-bucket",
		Key:           "object.txt",
		UploadID:      uploadId,
		PartNumber:    1,
		ContentLength: int64(6 * 1024 * 1024 * 1024), // 6GB - exceeds 5GB limit
		Body:          strToReadCloser("small content"),
	})
	if err == nil || !strings.Contains(err.Error(), "ContentLengthMismatch") {
		t.Errorf("Expected ContentLengthMismatch error for mismatched Content-Length, got: %v", err)
	}

	// Test 2: Upload a normal-sized part should work
	_, err = mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "object.txt",
		UploadID:   uploadId,
		PartNumber: 2,
		Body:       strToReadCloser("normal content"),
	})
	if err != nil {
		t.Errorf("Expected normal part upload to succeed, but got error: %v", err)
	}
}

func TestValidateUploadObjectPartFunction(t *testing.T) {
	// Test the validation function directly
	request := &http.Request{
		Header: make(http.Header),
	}

	// Test 1: Valid part number and size
	result := validateUploadObjectPart(request, 1, 1024)
	if result != nil {
		t.Errorf("Expected nil for valid input, got error: %s", result.errorMessage)
	}

	// Test 2: Invalid part number (too low)
	result = validateUploadObjectPart(request, 0, 1024)
	if result == nil || !strings.Contains(result.errorMessage, "InvalidPartNumber") {
		t.Errorf("Expected InvalidPartNumber error for part number 0")
	}

	// Test 3: Invalid part number (too high)
	result = validateUploadObjectPart(request, 10001, 1024)
	if result == nil || !strings.Contains(result.errorMessage, "InvalidPartNumber") {
		t.Errorf("Expected InvalidPartNumber error for part number 10001")
	}

	// Test 4: Part size too large
	result = validateUploadObjectPart(request, 1, int64(6*1024*1024*1024)) // 6GB
	if result == nil || !strings.Contains(result.errorMessage, "EntityTooLarge") {
		t.Errorf("Expected EntityTooLarge error for oversized part")
	}

	// Test 5: Content-Length mismatch
	request.Header.Set("Content-Length", "2048")
	result = validateUploadObjectPart(request, 1, 1024) // Content-Length says 2048, actual is 1024
	if result == nil || !strings.Contains(result.errorMessage, "ContentLengthMismatch") {
		t.Errorf("Expected ContentLengthMismatch error for mismatched Content-Length")
	}
}

func TestLargeMultipartUploadAndDownload(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()

	// Create a 130 MiB object split into 3 parts of ~43.3 MiB each
	const totalSize = 130 * 1024 * 1024 // 130 MiB
	const partSize = totalSize / 3      // ~43.3 MiB per part

	// Generate test data for each part
	part1Data := make([]byte, partSize)
	part2Data := make([]byte, partSize)
	part3Data := make([]byte, totalSize-2*partSize) // remaining bytes

	// Fill with predictable patterns to verify integrity
	for i := range part1Data {
		part1Data[i] = byte(i % 256)
	}
	for i := range part2Data {
		part2Data[i] = byte((i + 1000) % 256)
	}
	for i := range part3Data {
		part3Data[i] = byte((i + 2000) % 256)
	}

	// Initiate multipart upload
	resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "large-object.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := resp.UploadID

	// Upload part 1
	part1Resp, err := mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "large-object.bin",
		UploadID:   uploadID,
		PartNumber: 1,
		Body:       io.NopCloser(bytes.NewReader(part1Data)),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Upload part 2
	part2Resp, err := mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "large-object.bin",
		UploadID:   uploadID,
		PartNumber: 2,
		Body:       io.NopCloser(bytes.NewReader(part2Data)),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Upload part 3
	part3Resp, err := mpuc.UploadObjectPart(ctx, &multipartclient.UploadObjectPartRequest{
		Bucket:     "test-bucket",
		Key:        "large-object.bin",
		UploadID:   uploadID,
		PartNumber: 3,
		Body:       io.NopCloser(bytes.NewReader(part3Data)),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Complete the multipart upload
	_, err = mpuc.CompleteMultipartUpload(ctx, &multipartclient.CompleteMultipartUploadRequest{
		Bucket:   "test-bucket",
		Key:      "large-object.bin",
		UploadID: uploadID,
		Body: multipartclient.CompleteMultipartUploadBody{
			Parts: []multipartclient.CompletePart{
				{PartNumber: 1, Etag: part1Resp.ETag},
				{PartNumber: 2, Etag: part2Resp.ETag},
				{PartNumber: 3, Etag: part3Resp.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload: %v", err)
	}

	// Now read the object back using the regular (non-multipart) API
	obj, err := server.GetObject("test-bucket", "large-object.bin")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}

	// Verify the object size
	expectedSize := int64(totalSize)
	if obj.Size != expectedSize {
		t.Errorf("Expected object size %d, got %d", expectedSize, obj.Size)
	}

	// Verify the content matches our original parts concatenated
	expectedContent := append(append(part1Data, part2Data...), part3Data...)
	if !bytes.Equal(obj.Content, expectedContent) {
		t.Error("Object content does not match expected concatenated parts")

		// Additional debugging info
		if len(obj.Content) != len(expectedContent) {
			t.Errorf("Content length mismatch: expected %d, got %d", len(expectedContent), len(obj.Content))
		} else {
			// Find first differing byte
			for i := 0; i < len(expectedContent); i++ {
				if obj.Content[i] != expectedContent[i] {
					t.Errorf("First difference at byte %d: expected %d, got %d", i, expectedContent[i], obj.Content[i])
					break
				}
			}
		}
	}

	t.Logf("Successfully uploaded and downloaded %d byte object via multipart upload", totalSize)
}

func TestListMultipartUploadsQueryParameters(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()

	// Create multiple uploads in different "directories" to test filtering
	uploads := []struct {
		bucket string
		key    string
	}{
		{"test-bucket", "folder1/file1.txt"},
		{"test-bucket", "folder1/file2.txt"},
		{"test-bucket", "folder2/file1.txt"},
		{"test-bucket", "folder2/subfolder/file1.txt"},
		{"test-bucket", "root-file.txt"},
		{"other-bucket", "other-file.txt"}, // Different bucket
	}

	var uploadIDs []string
	for _, upload := range uploads {
		resp, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
			Bucket: upload.bucket,
			Key:    upload.key,
		})
		if err != nil {
			t.Fatal(err)
		}
		uploadIDs = append(uploadIDs, resp.UploadID)
	}

	// Test 1: List all uploads in test-bucket (should get 5, not 6)
	resp, err := mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Uploads) != 5 {
		t.Errorf("Expected 5 uploads in test-bucket, got %d", len(resp.Uploads))
	}
	if resp.MaxUploads != 1000 {
		t.Errorf("Expected MaxUploads to be 1000, got %d", resp.MaxUploads)
	}
	if resp.IsTruncated {
		t.Error("Expected IsTruncated to be false")
	}

	// Verify uploads have all required fields and are sorted
	for i, upload := range resp.Uploads {
		if upload.Key == "" {
			t.Errorf("Upload %d missing Key field", i)
		}
		if upload.UploadID == "" {
			t.Errorf("Upload %d missing UploadID field", i)
		}
		if upload.StorageClass == "" {
			t.Errorf("Upload %d missing StorageClass field", i)
		}
		if upload.Initiated.IsZero() {
			t.Errorf("Upload %d missing Initiated field", i)
		}

		// Check sorting (lexicographically by key)
		if i > 0 && upload.Key < resp.Uploads[i-1].Key {
			t.Errorf("Uploads not sorted: %s should come before %s", resp.Uploads[i-1].Key, upload.Key)
		}
	}

	// Test 2: Filter by prefix
	resp, err = mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket: "test-bucket",
		Prefix: "folder1/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Uploads) != 2 {
		t.Errorf("Expected 2 uploads with prefix 'folder1/', got %d", len(resp.Uploads))
	}
	for _, upload := range resp.Uploads {
		if !strings.HasPrefix(upload.Key, "folder1/") {
			t.Errorf("Upload key '%s' doesn't match prefix 'folder1/'", upload.Key)
		}
	}

	// Test 3: Limit results with max-uploads
	resp, err = mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket:     "test-bucket",
		MaxUploads: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Uploads) > 2 {
		t.Errorf("Expected at most 2 uploads with MaxUploads=2, got %d", len(resp.Uploads))
	}
	if resp.MaxUploads != 2 {
		t.Errorf("Expected MaxUploads to be 2, got %d", resp.MaxUploads)
	}
	if !resp.IsTruncated {
		t.Error("Expected IsTruncated to be true with MaxUploads=2")
	}
	if resp.NextKeyMarker == "" {
		t.Error("Expected NextKeyMarker to be set when IsTruncated is true")
	}
}

func TestListMultipartUploadsPagination(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()

	// Create multiple uploads to test pagination
	objectNames := []string{
		"file-a.txt",
		"file-b.txt",
		"file-c.txt",
		"file-d.txt",
		"file-e.txt",
	}

	for _, name := range objectNames {
		_, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    name,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test pagination with max-uploads=2
	var allUploadKeys []string
	var keyMarker string
	var uploadIDMarker string

	for {
		req := &multipartclient.ListMultipartUploadsRequest{
			Bucket:     "test-bucket",
			MaxUploads: 2,
		}
		if keyMarker != "" {
			req.KeyMarker = keyMarker
		}
		if uploadIDMarker != "" {
			req.UploadIdMarker = uploadIDMarker
		}

		resp, err := mpuc.ListMultipartUploads(ctx, req)
		if err != nil {
			t.Fatal(err)
		}

		for _, upload := range resp.Uploads {
			allUploadKeys = append(allUploadKeys, upload.Key)
		}

		if !resp.IsTruncated {
			break
		}

		keyMarker = resp.NextKeyMarker
		uploadIDMarker = resp.NextUploadIdMarker
	}

	// Should have collected all 5 uploads
	if len(allUploadKeys) != 5 {
		t.Errorf("Expected to collect 5 uploads through pagination, got %d", len(allUploadKeys))
	}

	// Verify they're in sorted order
	for i := 1; i < len(allUploadKeys); i++ {
		if allUploadKeys[i] < allUploadKeys[i-1] {
			t.Errorf("Pagination results not sorted: %s should come before %s", allUploadKeys[i-1], allUploadKeys[i])
		}
	}
}

func TestListMultipartUploadsSpecialCharacters(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()

	// Create upload with special characters in name
	objectName := "folder with spaces/file (1).txt"
	_, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    objectName,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test listing uploads with special characters
	resp, err := mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Uploads) != 1 {
		t.Fatalf("Expected 1 upload, got %d", len(resp.Uploads))
	}

	// The key should match the original name
	if resp.Uploads[0].Key != objectName {
		t.Errorf("Expected key '%s', got '%s'", objectName, resp.Uploads[0].Key)
	}
}

func TestListMultipartUploadsEmptyResults(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	mpuc := multipartclient.New(client)
	ctx := context.Background()

	// Test listing uploads in empty bucket
	resp, err := mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket: "empty-bucket",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Uploads) != 0 {
		t.Errorf("Expected 0 uploads in empty bucket, got %d", len(resp.Uploads))
	}
	if resp.IsTruncated {
		t.Error("Expected IsTruncated to be false for empty results")
	}
	if resp.NextKeyMarker != "" {
		t.Error("Expected NextKeyMarker to be empty for empty results")
	}
}

func TestListMultipartUploadsXMLResponse(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()

	// Create the bucket first
	server.CreateBucketWithOpts(CreateBucketOpts{Name: "test-bucket"})

	mpuc := multipartclient.New(server.HTTPClient())
	ctx := context.Background()

	// Create a few uploads
	_, err := mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "file1.txt",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = mpuc.InitiateMultipartUpload(ctx, &multipartclient.InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "folder/file2.txt",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test pagination with prefix
	resp, err := mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket:     "test-bucket",
		MaxUploads: 1,
		Prefix:     "f",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response has all required fields and correct values
	if resp.Bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", resp.Bucket)
	}
	if resp.MaxUploads != 1 {
		t.Errorf("Expected MaxUploads 1, got %d", resp.MaxUploads)
	}
	if resp.Prefix != "f" {
		t.Errorf("Expected Prefix 'f', got '%s'", resp.Prefix)
	}
	if !resp.IsTruncated {
		t.Error("Expected IsTruncated to be true with MaxUploads=1 and prefix filter")
	}
	if resp.NextKeyMarker == "" {
		t.Error("Expected NextKeyMarker to be set when IsTruncated is true")
	}
	if len(resp.Uploads) != 1 {
		t.Errorf("Expected 1 upload with MaxUploads=1, got %d", len(resp.Uploads))
	}

	// Verify upload has all required fields
	upload := resp.Uploads[0]
	if upload.Key == "" {
		t.Error("Upload missing Key field")
	}
	if upload.UploadID == "" {
		t.Error("Upload missing UploadID field")
	}
	if upload.StorageClass != "STANDARD" {
		t.Errorf("Expected StorageClass 'STANDARD', got '%s'", upload.StorageClass)
	}
	if upload.Initiated.IsZero() {
		t.Error("Upload missing Initiated field")
	}

	// Test with delimiter parameter
	resp, err = mpuc.ListMultipartUploads(ctx, &multipartclient.ListMultipartUploadsRequest{
		Bucket:    "test-bucket",
		Delimiter: "/",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should include delimiter and common prefixes
	if resp.Delimiter != "/" {
		t.Errorf("Expected Delimiter '/', got '%s'", resp.Delimiter)
	}
}
