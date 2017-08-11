// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// Server is the fake server.
//
// It provides a fake implementation of the Google Cloud Storage API.
type Server struct {
	buckets   map[string][]Object
	uploads   map[string]Object
	transport *http.Transport
	ts        *httptest.Server
	mux       *mux.Router
	mtx       sync.RWMutex
}

// NewServer creates a new instance of the server, pre-loaded with the given
// objects.
func NewServer(objects []Object) *Server {
	s := Server{buckets: make(map[string][]Object), uploads: make(map[string]Object)}
	tlsConfig := tls.Config{InsecureSkipVerify: true}
	s.buildMuxer()
	s.ts = httptest.NewTLSServer(s.mux)
	s.transport = &http.Transport{
		TLSClientConfig: &tlsConfig,
		DialTLS: func(string, string) (net.Conn, error) {
			return tls.Dial("tcp", s.ts.Listener.Addr().String(), &tlsConfig)
		},
	}
	for _, o := range objects {
		s.buckets[o.BucketName] = append(s.buckets[o.BucketName], o)
	}
	return &s
}

func (s *Server) buildMuxer() {
	s.mux = mux.NewRouter()
	s.mux.Host("storage.googleapis.com").Path("/{bucketName}/{objectName:.+}").Methods("GET").HandlerFunc(s.downloadObject)
	r := s.mux.PathPrefix("/storage/v1").Subrouter()
	r.Path("/b").Methods("GET").HandlerFunc(s.listBuckets)
	r.Path("/b/{bucketName}").Methods("GET").HandlerFunc(s.getBucket)
	r.Path("/b/{bucketName}/o").Methods("GET").HandlerFunc(s.listObjects)
	r.Path("/b/{bucketName}/o").Methods("POST").HandlerFunc(s.insertObject)
	r.Path("/b/{bucketName}/o/{objectName:.+}").Methods("GET").HandlerFunc(s.getObject)
	r.Path("/b/{bucketName}/o/{objectName:.+}").Methods("DELETE").HandlerFunc(s.deleteObject)
	r.Path("/b/{sourceBucket}/o/{sourceObject:.+}/rewriteTo/b/{destinationBucket}/o/{destinationObject:.+}").HandlerFunc(s.rewriteObject)
	s.mux.Path("/upload/storage/v1/b/{bucketName}/o").Methods("POST").HandlerFunc(s.insertObject)
	s.mux.Path("/upload/resumable/{uploadId}").Methods("PUT", "POST").HandlerFunc(s.uploadFileContent)
}

// Stop stops the server, closing all connections.
func (s *Server) Stop() {
	s.transport.CloseIdleConnections()
	s.ts.Close()
}

// URL returns the server URL.
func (s *Server) URL() string {
	return s.ts.URL
}

// Client returns a GCS client configured to talk to the server.
func (s *Server) Client() *storage.Client {
	opt := option.WithHTTPClient(&http.Client{Transport: s.transport})
	client, _ := storage.NewClient(context.Background(), opt)
	return client
}
