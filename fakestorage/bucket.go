// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	"github.com/gorilla/mux"
)

var bucketRegexp = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9]$`)

// CreateBucket creates a bucket inside the server, so any API calls that
// require the bucket name will recognize this bucket.
//
// If the bucket already exists, this method does nothing.
//
// Deprecated: use CreateBucketWithOpts.
func (s *Server) CreateBucket(name string) {
	err := s.backend.CreateBucket(name, false)
	if err != nil {
		panic(err)
	}
}

// CreateBucketOpts defines the properties of a bucket you can create with
// CreateBucketWithOpts.
type CreateBucketOpts struct {
	Name              string
	VersioningEnabled bool
}

// CreateBucketWithOpts creates a bucket inside the server, so any API calls that
// require the bucket name will recognize this bucket. Use CreateBucketOpts to
// customize the options for this bucket
//
// If the underlying backend returns an error, this method panics.
func (s *Server) CreateBucketWithOpts(opts CreateBucketOpts) {
	err := s.backend.CreateBucket(opts.Name, opts.VersioningEnabled)
	if err != nil {
		panic(err)
	}
}

func (s *Server) createBucketByPost(r *http.Request) jsonResponse {
	// Minimal version of Bucket from google.golang.org/api/storage/v1

	var data struct {
		Name       string            `json:"name,omitempty"`
		Versioning *bucketVersioning `json:"versioning,omitempty"`
	}

	// Read the bucket props from the request body JSON
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&data); err != nil {
		return jsonResponse{err: err, status: http.StatusBadRequest}
	}
	name := data.Name
	versioning := false
	if data.Versioning != nil {
		versioning = data.Versioning.Enabled
	}
	if err := validateBucketName(name); err != nil {
		return jsonResponse{err: err, status: http.StatusBadRequest}
	}

	// Create the named bucket
	if err := s.backend.CreateBucket(name, versioning); err != nil {
		return jsonResponse{err: err, status: http.StatusInternalServerError}
	}

	// Return the created bucket:
	bucket, err := s.backend.GetBucket(name)
	if err != nil {
		return jsonResponse{err: err, status: http.StatusInternalServerError}
	}
	return jsonResponse{data: newBucketResponse(bucket)}
}

func (s *Server) listBuckets(r *http.Request) jsonResponse {
	buckets, err := s.backend.ListBuckets()
	if err != nil {
		return jsonResponse{err: err, status: http.StatusInternalServerError}
	}
	return jsonResponse{data: newListBucketsResponse(buckets)}
}

func (s *Server) getBucket(r *http.Request) jsonResponse {
	bucketName := mux.Vars(r)["bucketName"]
	bucket, err := s.backend.GetBucket(bucketName)
	if err != nil {
		return jsonResponse{data: err, status: http.StatusNotFound}
	}
	return jsonResponse{data: newBucketResponse(bucket)}
}

func validateBucketName(bucketName string) error {
	if !bucketRegexp.MatchString(bucketName) {
		return errors.New("invalid bucket name")
	}
	return nil
}
