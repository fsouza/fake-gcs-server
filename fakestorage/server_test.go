// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

func TestNewServer(t *testing.T) {
	t.Parallel()
	server := NewServer([]Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
		{BucketName: "other-bucket", Name: "static/css/website.css"},
	})
	defer server.Stop()
	url := server.URL()
	if url != server.ts.URL {
		t.Errorf("wrong url returned\nwant %q\ngot  %q", server.ts.URL, url)
	}
}

func TestNewServerNoListener(t *testing.T) {
	t.Parallel()
	server, err := NewServerWithOptions(Options{NoListener: true})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	url := server.URL()
	if url != "" {
		t.Errorf("unexpected non-empty url: %q", url)
	}
}

func TestNewServerExternalHost(t *testing.T) {
	t.Parallel()
	server, err := NewServerWithOptions(Options{ExternalURL: "https://gcs.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	url := server.URL()
	if url != "https://gcs.example.com" {
		t.Errorf("wrong url returned\n want %q\ngot  %q", server.externalURL, url)
	}
}

func TestNewServerLogging(t *testing.T) {
	t.Parallel()
	buf := new(bytes.Buffer)
	server, err := NewServerWithOptions(Options{Writer: buf})
	if err != nil {
		t.Fatal(err)
	}

	defer server.Stop()
	client := server.HTTPClient()
	req, err := http.NewRequest(http.MethodGet, "https://storage.googleapis.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(ioutil.Discard, res.Body)
	if buf.Len() == 0 {
		t.Error("Log was not written to buffer.")
	}
}

func TestDownloadObject(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "files/txt/text-01.txt", Content: []byte("something")},
		{BucketName: "some-bucket", Name: "files/txt/text-02.txt"},
		{BucketName: "some-bucket", Name: "files/txt/text-03.txt"},
		{BucketName: "other-bucket", Name: "static/css/website.css", Content: []byte("body {display: none;}")},
	}
	runServersTest(t, objs, testDownloadObject)
	runServersTest(t, objs, testDownloadObjectRange)
}

func testDownloadObject(t *testing.T, server *Server) {
	tests := []struct {
		name            string
		method          string
		url             string
		expectedHeaders map[string]string
		expectedBody    string
	}{
		{
			"GET: bucket in the path",
			http.MethodGet,
			"https://storage.googleapis.com/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"something",
		},
		{
			"GET: bucket in the host",
			http.MethodGet,
			"https://other-bucket.storage.googleapis.com/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"body {display: none;}",
		},
		{
			"GET: using storage api",
			http.MethodGet,
			"https://storage.googleapis.com/storage/v1/b/some-bucket/o/files/txt/text-01.txt?alt=media",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"something",
		},
		{
			"HEAD: bucket in the path",
			http.MethodHead,
			"https://storage.googleapis.com/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"",
		},
		{
			"HEAD: bucket in the host",
			http.MethodHead,
			"https://other-bucket.storage.googleapis.com/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"",
		},
	}
	expected := "https://storage.googleapis.com"
	if server.PublicURL() != expected {
		t.Fatalf("Expected PublicURL \"%s\", is \"%s\".", expected, server.PublicURL())
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			client := server.HTTPClient()
			req, err := http.NewRequest(test.method, test.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
			}
			for k, expectedV := range test.expectedHeaders {
				if v := resp.Header.Get(k); v != expectedV {
					t.Errorf("wrong value for header %q:\nwant %q\ngot  %q", k, expectedV, v)
				}
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if body := string(data); body != test.expectedBody {
				t.Errorf("wrong body\nwant %q\ngot  %q", test.expectedBody, body)
			}
		})
	}
}

func testDownloadObjectRange(t *testing.T, server *Server) {
	tests := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
		expectedBody   string
	}{
		{"No range specified", map[string]string{}, http.StatusOK, "something"},
		{"Partial range specified", map[string]string{"Range": "bytes=1-4"}, http.StatusPartialContent, "omet"},
		{"Exact range specified", map[string]string{"Range": "bytes=0-8"}, http.StatusOK, "something"},
		{"Too-long range specified", map[string]string{"Range": "bytes=0-100"}, http.StatusOK, "something"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			client := server.HTTPClient()
			req, err := http.NewRequest("GET", "https://storage.googleapis.com/some-bucket/files/txt/text-01.txt", nil)
			if err != nil {
				t.Fatal(err)
			}
			for header, value := range test.headers {
				req.Header.Add(header, value)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != test.expectedStatus {
				t.Errorf("wrong status returned\nwant %d\ngot  %d", test.expectedStatus, resp.StatusCode)
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if body := string(data); body != test.expectedBody {
				t.Errorf("wrong body\nwant %q\ngot  %q", test.expectedBody, body)
			}
		})
	}
}

func TestDownloadObjectAlternatePublicHost(t *testing.T) {
	tests := []struct {
		name            string
		method          string
		url             string
		expectedHeaders map[string]string
		expectedBody    string
	}{
		{
			"GET: bucket in the path",
			http.MethodGet,
			"https://storage.gcs.127.0.0.1.nip.io:4443/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"something",
		},
		{
			"GET: bucket in the host",
			http.MethodGet,
			"https://other-bucket.storage.gcs.127.0.0.1.nip.io:4443/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"body {display: none;}",
		},
		{
			"HEAD: bucket in the path",
			http.MethodHead,
			"https://storage.gcs.127.0.0.1.nip.io:4443/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"",
		},
		{
			"HEAD: bucket in the host",
			http.MethodHead,
			"https://other-bucket.storage.gcs.127.0.0.1.nip.io:4443/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"",
		},
	}
	objs := []Object{
		{BucketName: "some-bucket", Name: "files/txt/text-01.txt", Content: []byte("something")},
		{BucketName: "other-bucket", Name: "static/css/website.css", Content: []byte("body {display: none;}")},
	}
	opts := Options{
		InitialObjects: objs,
		PublicHost:     "storage.gcs.127.0.0.1.nip.io:4443",
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	expected := "https://storage.gcs.127.0.0.1.nip.io:4443"
	if server.PublicURL() != expected {
		t.Fatalf("Expected PublicURL \"%s\", is \"%s\".", expected, server.PublicURL())
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			client := server.HTTPClient()
			req, err := http.NewRequest(test.method, test.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
			}
			for k, expectedV := range test.expectedHeaders {
				if v := resp.Header.Get(k); v != expectedV {
					t.Errorf("wrong value for header %q:\nwant %q\ngot  %q", k, expectedV, v)
				}
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if body := string(data); body != test.expectedBody {
				t.Errorf("wrong body\nwant %q\ngot  %q", test.expectedBody, body)
			}
		})
	}
}

func TestSignedUploadBucketCNAME(t *testing.T) {
	url := "https://mybucket.mydomain.com:4443/files/txt/text-02.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=fake-gcs&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=fake-gc"
	expectedName := "files/txt/text-02.txt"
	expectedContentType := "text/plain"
	expectedHash := "bHupxaFBQh4cA8uYB8l8dA=="
	opts := Options{
		InitialObjects: []Object{
			{BucketName: "mybucket.mydomain.com", Name: "files/txt/text-01.txt", Content: []byte("something")},
		},
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	client := server.HTTPClient()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader("something else"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var obj Object
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if obj.Name != expectedName {
		t.Errorf("wrong filename\nwant %q\ngot  %q", expectedName, obj.Name)
	}
	if obj.ContentType != expectedContentType {
		t.Errorf("wrong content type\nwant %q\ngot  %q", expectedContentType, obj.ContentType)
	}
	if obj.Md5Hash != expectedHash {
		t.Errorf("wrong md5 hash\nwant %q\ngot  %q", expectedHash, obj.Md5Hash)
	}
}

func TestObjectDownloadBucketCNAME(t *testing.T) {
	url := "https://mybucket.mydomain.com:4443/files/txt/text-01.txt"
	expectedHeaders := map[string]string{"accept-ranges": "bytes", "content-length": "9"}
	expectedBody := "something"
	opts := Options{
		InitialObjects: []Object{
			{BucketName: "mybucket.mydomain.com", Name: "files/txt/text-01.txt", Content: []byte("something")},
		},
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	client := server.HTTPClient()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
	}
	for k, expectedV := range expectedHeaders {
		if v := resp.Header.Get(k); v != expectedV {
			t.Errorf("wrong value for header %q:\nwant %q\ngot  %q", k, expectedV, v)
		}
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if body := string(data); body != expectedBody {
		t.Errorf("wrong body\nwant %q\ngot  %q", expectedBody, body)
	}
}

func runServersTest(t *testing.T, objs []Object, fn func(*testing.T, *Server)) {
	t.Run("tcp listener", func(t *testing.T) {
		t.Parallel()
		tcpServer, err := NewServerWithOptions(Options{NoListener: false, InitialObjects: objs})
		if err != nil {
			t.Fatal(err)
		}
		defer tcpServer.Stop()
		fn(t, tcpServer)
	})
	t.Run("no listener", func(t *testing.T) {
		t.Parallel()
		noListenerServer, err := NewServerWithOptions(Options{NoListener: true, InitialObjects: objs})
		if err != nil {
			t.Fatal(err)
		}
		fn(t, noListenerServer)
	})
}
