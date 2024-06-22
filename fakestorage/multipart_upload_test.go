package fakestorage

import (
	"net/http"
	"testing"
)

func TestUnimplementedHandlers(t *testing.T) {
	server := NewServer(nil)
	defer server.Stop()
	client := server.HTTPClient()

	tests := []struct {
		name   string
		method string
		url    string
	}{
		{
			name:   "Inititiate Multipart Upload",
			method: "POST",
			url:    "/obj.txt?uploads",
		},
		{
			name:   "Upload object parts",
			method: "PUT",
			url:    "/obj.txt?partNumber=1&uploadId=my-upload-id",
		},
		{
			name:   "Complete Multipart Upload",
			method: "POST",
			url:    "/obj.txt?uploadId=my-upload-id",
		},
		{
			name:   "List Multipart Uploads",
			method: "GET",
			url:    "/?uploads",
		},
		{
			name:   "List Object Parts",
			method: "GET",
			url:    "/obj.txt?uploadId=my-upload-id",
		},
		{
			name:   "Abort Multipart Uploads",
			method: "DELETE",
			url:    "/obj.txt?uploadId=my-upload-id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test with bucket host:
			req, err := http.NewRequest(tc.method, tc.url, http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = "test-bucket.storage.googleapis.com"
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusNotImplemented {
				t.Errorf("Unexpected status with bucket host: got %v, want %v", resp.StatusCode, http.StatusNotImplemented)
			}

			// Test with storage.googleapis.com/bucketName/
			url := "/test-buckets" + tc.url
			req, err = http.NewRequest(tc.method, url, http.NoBody)
			req.Host = "storage.googleapis.com"
			if err != nil {
				t.Fatal(err)
			}
			resp, err = client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusNotImplemented {
				t.Errorf("Unexpected status with storage.googleapis.com: got %v, want %v", resp.StatusCode, http.StatusNotImplemented)
			}
		})
	}
}
