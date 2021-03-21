// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
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

func TestPublicURL(t *testing.T) {
	t.Parallel()
	var tests = []struct {
		name     string
		options  Options
		expected string
	}{
		{
			name:     "https scheme",
			options:  Options{Scheme: "https", NoListener: true},
			expected: "https://storage.googleapis.com",
		},
		{
			name:     "http scheme",
			options:  Options{Scheme: "http", NoListener: true},
			expected: "http://storage.googleapis.com",
		},
		{
			name:     "no scheme",
			options:  Options{NoListener: true},
			expected: "https://storage.googleapis.com",
		},
		{
			name:     "https scheme - custom public host",
			options:  Options{Scheme: "https", NoListener: true, PublicHost: "localhost:8080"},
			expected: "https://localhost:8080",
		},
		{
			name:     "no scheme - custom public host",
			options:  Options{NoListener: true, PublicHost: "localhost:8080"},
			expected: "https://localhost:8080",
		},
		{
			name:     "http scheme - custom public host",
			options:  Options{Scheme: "http", NoListener: true, PublicHost: "localhost:8080"},
			expected: "http://localhost:8080",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server, err := NewServerWithOptions(test.options)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Stop()
			if publicURL := server.PublicURL(); publicURL != test.expected {
				t.Errorf("wrong public url returned\nwant %q\ngot  %q", test.expected, publicURL)
			}
		})
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
			"://storage.googleapis.com/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"something",
		},
		{
			"GET: bucket in the host",
			http.MethodGet,
			"://other-bucket.storage.googleapis.com/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"body {display: none;}",
		},
		{
			"GET: using storage api",
			http.MethodGet,
			"://storage.googleapis.com/storage/v1/b/some-bucket/o/files/txt/text-01.txt?alt=media",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"something",
		},
		{
			"HEAD: bucket in the path",
			http.MethodHead,
			"://storage.googleapis.com/some-bucket/files/txt/text-01.txt",
			map[string]string{"accept-ranges": "bytes", "content-length": "9"},
			"",
		},
		{
			"HEAD: bucket in the host",
			http.MethodHead,
			"://other-bucket.storage.googleapis.com/static/css/website.css",
			map[string]string{"accept-ranges": "bytes", "content-length": "21"},
			"",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := server.HTTPClient()
			url := server.scheme() + test.url
			req, err := http.NewRequest(test.method, url, nil)
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
			req, err := http.NewRequest("GET", server.scheme()+"://storage.googleapis.com/some-bucket/files/txt/text-01.txt", nil)
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

func TestCORSRequests(t *testing.T) {
	tests := []struct {
		name    string
		origin  string
		method  string
		headers []string
	}{
		{
			"Allow GET Requests",
			"http://example.com",
			http.MethodGet,
			[]string{},
		},
		{
			"Allow DELETE Requests",
			"http://example.com",
			http.MethodDelete,
			[]string{},
		},
		{
			"Accept Allowlisted Custom Headers",
			"http://example.com/",
			http.MethodGet,
			[]string{"X-Goog-Meta-Uploader"},
		},
		{
			"Accept Alternate Origin",
			"http://megazord.com/",
			http.MethodGet,
			[]string{},
		},
	}
	opts := Options{
		AllowedCORSHeaders: []string{"X-Goog-Meta-Uploader"},
	}
	server, err := NewServerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			client := server.HTTPClient()
			req, err := http.NewRequest(http.MethodOptions, "https://127.0.0.1/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Origin", test.origin)
			req.Header.Set("Access-Control-Request-Method", test.method)
			if len(test.headers) > 0 {
				headers := strings.Join(test.headers, ",")
				req.Header.Set("Access-Control-Request-Headers", headers)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("wrong status returned\nwant %d\ngot  %d", http.StatusOK, resp.StatusCode)
			}
			for name, values := range resp.Header {
				// Loop over all values for the name.
				for _, value := range values {
					t.Logf("%s: %v", name, value)
				}
			}
			if v := resp.Header.Get("Access-Control-Allow-Origin"); v != "*" {
				t.Errorf("wrong value for Access-Control-Allow-Origin: got %q", v)
			}
		})
	}
}

func runServersTest(t *testing.T, objs []Object, fn func(*testing.T, *Server)) {
	var testScenarios = []struct {
		name    string
		options Options
	}{
		{
			name:    "https listener",
			options: Options{NoListener: false, InitialObjects: objs},
		},
		{
			name:    "http listener",
			options: Options{Scheme: "http", NoListener: false, InitialObjects: objs},
		},
		{
			name:    "no listener",
			options: Options{NoListener: true, InitialObjects: objs},
		},
	}
	for _, test := range testScenarios {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server, err := NewServerWithOptions(test.options)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Stop()
			fn(t, server)
		})
	}
}
