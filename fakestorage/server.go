// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/internal/backend"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"google.golang.org/api/option"
)

const defaultPublicHost = "storage.googleapis.com"

// Server is the fake server.
//
// It provides a fake implementation of the Google Cloud Storage API.
type Server struct {
	backend     backend.Storage
	uploads     sync.Map
	transport   http.RoundTripper
	ts          *httptest.Server
	mux         *mux.Router
	options     Options
	externalURL string
	publicHost  string
}

// NewServer creates a new instance of the server, pre-loaded with the given
// objects.
func NewServer(objects []Object) *Server {
	s, _ := NewServerWithOptions(Options{
		InitialObjects: objects,
	})
	return s
}

// NewServerWithHostPort creates a new server that listens on a custom host and port
//
// Deprecated: use NewServerWithOptions.
func NewServerWithHostPort(objects []Object, host string, port uint16) (*Server, error) {
	return NewServerWithOptions(Options{
		InitialObjects: objects,
		Host:           host,
		Port:           port,
	})
}

// Options are used to configure the server on creation.
type Options struct {
	InitialObjects []Object
	StorageRoot    string
	Scheme         string
	Host           string
	Port           uint16

	// when set to true, the server will not actually start a TCP listener,
	// client requests will get processed by an internal mocked transport.
	NoListener bool

	// Optional external URL, such as https://gcs.127.0.0.1.nip.io:4443
	// Returned in the Location header for resumable uploads
	// The "real" value is https://www.googleapis.com, the JSON API
	// The default is whatever the server is bound to, such as https://0.0.0.0:4443
	ExternalURL string

	// Optional URL for public access
	// An example is "storage.gcs.127.0.0.1.nip.io:4443", which will configure
	// the server to serve objects at:
	// https://storage.gcs.127.0.0.1.nip.io:4443/<bucket>/<object>
	// https://<bucket>.storage.gcs.127.0.0.1.nip.io:4443>/<bucket>/<object>
	// If unset, the default is "storage.googleapis.com", the XML API
	PublicHost string

	// Optional list of headers to add to the CORS header allowlist
	// An example is "X-Goog-Meta-Uploader", which will allow a
	// custom metadata header named "X-Goog-Meta-Uploader" to be
	// sent through the browser
	AllowedCORSHeaders []string

	// Destination for writing log.
	Writer io.Writer
}

// NewServerWithOptions creates a new server configured according to the
// provided options.
func NewServerWithOptions(options Options) (*Server, error) {
	s, err := newServer(options)
	if err != nil {
		return nil, err
	}

	allowedHeaders := []string{"Content-Type", "Content-Encoding", "Range"}
	allowedHeaders = append(allowedHeaders, options.AllowedCORSHeaders...)

	cors := handlers.CORS(
		handlers.AllowedMethods([]string{
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		}),
		handlers.AllowedHeaders(allowedHeaders),
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowCredentials(),
	)

	handler := cors(s.mux)
	if options.Writer != nil {
		handler = handlers.LoggingHandler(options.Writer, handler)
	}
	handler = requestCompressHandler(handler)
	s.transport = &muxTransport{handler: handler}
	if options.NoListener {
		return s, nil
	}

	s.ts = httptest.NewUnstartedServer(handler)
	startFunc := s.ts.StartTLS
	if options.Scheme == "http" {
		startFunc = s.ts.Start
	}
	if options.Port != 0 {
		addr := fmt.Sprintf("%s:%d", options.Host, options.Port)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		s.ts.Listener.Close()
		s.ts.Listener = l
	}
	startFunc()

	return s, nil
}

func newServer(options Options) (*Server, error) {
	backendObjects := toBackendObjects(options.InitialObjects)
	var backendStorage backend.Storage
	var err error
	if options.StorageRoot != "" {
		backendStorage, err = backend.NewStorageFS(backendObjects, options.StorageRoot)
	} else {
		backendStorage = backend.NewStorageMemory(backendObjects)
	}
	if err != nil {
		return nil, err
	}
	publicHost := options.PublicHost
	if publicHost == "" {
		publicHost = defaultPublicHost
	}
	s := Server{
		backend:     backendStorage,
		uploads:     sync.Map{},
		externalURL: options.ExternalURL,
		publicHost:  publicHost,
		options:     options,
	}
	s.buildMuxer()
	return &s, nil
}

func (s *Server) buildMuxer() {
	const apiPrefix = "/storage/v1"
	s.mux = mux.NewRouter()

	routers := []*mux.Router{
		s.mux.PathPrefix(apiPrefix).Subrouter(),
		s.mux.Host(s.publicHost).PathPrefix(apiPrefix).Subrouter(),
	}

	for _, r := range routers {
		r.Path("/b").Methods(http.MethodGet).HandlerFunc(jsonToHTTPHandler(s.listBuckets))
		r.Path("/b").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.createBucketByPost))
		r.Path("/b/{bucketName}").Methods(http.MethodGet).HandlerFunc(jsonToHTTPHandler(s.getBucket))
		r.Path("/b/{bucketName}").Methods(http.MethodDelete).HandlerFunc(jsonToHTTPHandler(s.deleteBucket))
		r.Path("/b/{bucketName}/o").Methods(http.MethodGet).HandlerFunc(jsonToHTTPHandler(s.listObjects))
		r.Path("/b/{bucketName}/o").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.insertObject))
		r.Path("/b/{bucketName}/o/{objectName:.+}").Methods(http.MethodPatch).HandlerFunc(jsonToHTTPHandler(s.patchObject))
		r.Path("/b/{bucketName}/o/{objectName:.+}/acl").Methods(http.MethodGet).HandlerFunc(jsonToHTTPHandler(s.listObjectACL))
		r.Path("/b/{bucketName}/o/{objectName:.+}/acl").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.setObjectACL))
		r.Path("/b/{bucketName}/o/{objectName:.+}/acl/{entity}").Methods(http.MethodPut).HandlerFunc(jsonToHTTPHandler(s.setObjectACL))
		r.Path("/b/{bucketName}/o/{objectName:.+}").Methods(http.MethodGet).HandlerFunc(s.getObject)
		r.Path("/b/{bucketName}/o/{objectName:.+}").Methods(http.MethodDelete).HandlerFunc(jsonToHTTPHandler(s.deleteObject))
		r.Path("/b/{sourceBucket}/o/{sourceObject:.+}/copyTo/b/{destinationBucket}/o/{destinationObject:.+}").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.rewriteObject))
		r.Path("/b/{sourceBucket}/o/{sourceObject:.+}/rewriteTo/b/{destinationBucket}/o/{destinationObject:.+}").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.rewriteObject))
		r.Path("/b/{bucketName}/o/{destinationObject:.+}/compose").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.composeObject))
	}

	bucketHost := fmt.Sprintf("{bucketName}.%s", s.publicHost)
	s.mux.Host(bucketHost).Path("/{objectName:.+}").Methods(http.MethodGet, http.MethodHead).HandlerFunc(s.downloadObject)
	s.mux.Path("/download/storage/v1/b/{bucketName}/o/{objectName:.+}").Methods(http.MethodGet).HandlerFunc(s.downloadObject)
	s.mux.Path("/upload/storage/v1/b/{bucketName}/o").Methods(http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.insertObject))
	s.mux.Path("/upload/resumable/{uploadId}").Methods(http.MethodPut, http.MethodPost).HandlerFunc(jsonToHTTPHandler(s.uploadFileContent))

	s.mux.Host(s.publicHost).Path("/{bucketName}/{objectName:.+}").Methods(http.MethodGet, http.MethodHead).HandlerFunc(s.downloadObject)
	s.mux.Host("{bucketName:.+}").Path("/{objectName:.+}").Methods(http.MethodGet, http.MethodHead).HandlerFunc(s.downloadObject)

	// Form Uploads
	s.mux.Host(s.publicHost).Path("/{bucketName}").MatcherFunc(matchFormData).Methods(http.MethodPost, http.MethodPut).HandlerFunc(xmlToHTTPHandler(s.insertFormObject))
	s.mux.Host(bucketHost).MatcherFunc(matchFormData).Methods(http.MethodPost, http.MethodPut).HandlerFunc(xmlToHTTPHandler(s.insertFormObject))

	// Signed URL Uploads
	s.mux.Host(s.publicHost).Path("/{bucketName}/{objectName:.+}").Methods(http.MethodPost, http.MethodPut).HandlerFunc(jsonToHTTPHandler(s.insertObject))
	s.mux.Host(bucketHost).Path("/{objectName:.+}").Methods(http.MethodPost, http.MethodPut).HandlerFunc(jsonToHTTPHandler(s.insertObject))
	s.mux.Host("{bucketName:.+}").Path("/{objectName:.+}").Methods(http.MethodPost, http.MethodPut).HandlerFunc(jsonToHTTPHandler(s.insertObject))
}

// Stop stops the server, closing all connections.
func (s *Server) Stop() {
	if s.ts != nil {
		if transport, ok := s.transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
		s.ts.Close()
	}
}

// URL returns the server URL.
func (s *Server) URL() string {
	if s.externalURL != "" {
		return s.externalURL
	}
	if s.ts != nil {
		return s.ts.URL
	}
	return ""
}

// PublicURL returns the server's public download URL.
func (s *Server) PublicURL() string {
	return fmt.Sprintf("%s://%s", s.scheme(), s.publicHost)
}

func (s *Server) scheme() string {
	if s.options.Scheme == "http" {
		return "http"
	}
	return "https"
}

// HTTPClient returns an HTTP client configured to talk to the server.
func (s *Server) HTTPClient() *http.Client {
	return &http.Client{Transport: s.transport}
}

// Client returns a GCS client configured to talk to the server.
func (s *Server) Client() *storage.Client {
	client, _ := storage.NewClient(context.Background(), option.WithHTTPClient(s.HTTPClient()))
	return client
}

func requestCompressHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("content-encoding") == "gzip" {
			gzipReader, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			r.Body = gzipReader
		}
		h.ServeHTTP(w, r)
	})
}

func matchFormData(r *http.Request, _ *mux.RouteMatch) bool {
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return contentType == "multipart/form-data"
}
