// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"io/ioutil"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/sirupsen/logrus"
)

func TestGenerateObjectsFromFiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		folder               string
		expectedObjects      []fakestorage.Object
		expectedEmptyBuckets []string
	}{
		{
			name:   "should load from sample folder",
			folder: "testdata/basic",
			expectedObjects: []fakestorage.Object{
				{
					BucketName: "sample-bucket",
					Name:       "some_file.txt",
					Content:    []byte("Some amazing content to be loaded"),
				},
			},
			expectedEmptyBuckets: []string{"empty-bucket"},
		},
		{
			name:   "should skip inexistent folder",
			folder: "testdata/i-dont-exist",
		},
		{
			name:   "should skip a regular file",
			folder: "testdata/basic/sample-bucket/some_file.txt",
		},
		{
			name:   "should skip invalid directories and files",
			folder: "testdata/chaos",
			expectedObjects: []fakestorage.Object{
				{
					BucketName: "bucket1",
					Name:       "object1.txt",
					Content:    []byte("object 1\n"),
				},
				{
					BucketName: "bucket1",
					Name:       "object2.txt",
					Content:    []byte("object 2\n"),
				},
				{
					BucketName: "bucket2",
					Name:       "object1.txt",
					Content:    []byte("object 1\n"),
				},
				{
					BucketName: "bucket2",
					Name:       "object2.txt",
					Content:    []byte("object 2\n"),
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logger := discardLogger()

			objects, emptyBuckets := generateObjectsFromFiles(logger, test.folder)
			cmpOpts := []cmp.Option{
				cmpopts.IgnoreFields(fakestorage.Object{}, "Crc32c", "Md5Hash"),
				cmpopts.IgnoreUnexported(fakestorage.Object{}),
			}
			if diff := cmp.Diff(objects, test.expectedObjects, cmpOpts...); diff != "" {
				t.Errorf("wrong list of objects returned\nwant %#v\ngot  %#v\ndiff: %s", test.expectedObjects, objects, diff)
			}
			if diff := cmp.Diff(emptyBuckets, test.expectedEmptyBuckets); diff != "" {
				t.Errorf("wrong list of empty buckets returned\nwant %#v\ngot  %#v", test.expectedEmptyBuckets, emptyBuckets)
			}
		})
	}
}

func discardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Out = ioutil.Discard
	logger.Level = logrus.PanicLevel
	return logger
}
