// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"io/ioutil"
	"net/http"
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

func TestDownloadObject(t *testing.T) {
	server := NewServer([]Object{
		{BucketName: "some-bucket", Name: "files/txt/text-01.txt", Content: []byte("something")},
		{BucketName: "some-bucket", Name: "files/txt/text-02.txt"},
		{BucketName: "some-bucket", Name: "files/txt/text-03.txt"},
		{BucketName: "other-bucket", Name: "static/css/website.css", Content: []byte("body {display: none;}")},
	})
	defer server.Stop()

	var tests = []struct {
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
