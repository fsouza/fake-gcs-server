// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"os"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/sirupsen/logrus"
)

const testContentType = "text/plain; charset=utf-8"

func TestMain(m *testing.M) {
	mime.AddExtensionType(".txt", testContentType)
	const emptyBucketDir = "testdata/basic/empty-bucket"
	err := ensureEmptyDir(emptyBucketDir)
	if err != nil {
		panic(err)
	}
	var status int
	defer func() {
		os.Remove(emptyBucketDir)
		os.Exit(status)
	}()
	status = m.Run()
}

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
					BucketName:  "sample-bucket",
					Name:        "some_file.txt",
					Content:     []byte("Some amazing content to be loaded"),
					ContentType: testContentType,
				},
			},
			expectedEmptyBuckets: []string{"empty-bucket"},
		},
		{
			name:   "should support multiple levels",
			folder: "testdata/multi-level",
			expectedObjects: []fakestorage.Object{
				{
					BucketName:  "some-bucket",
					Name:        "a/b/c/d/e/f/object1.txt",
					Content:     []byte("this is object 1\n"),
					ContentType: testContentType,
				},
				{
					BucketName:  "some-bucket",
					Name:        "a/b/c/d/e/f/object2.txt",
					Content:     []byte("this is object 2\n"),
					ContentType: testContentType,
				},
				{
					BucketName:  "some-bucket",
					Name:        "root-object.txt",
					Content:     []byte("r00t\n"),
					ContentType: testContentType,
				},
			},
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
					BucketName:  "bucket1",
					Name:        "object1.txt",
					Content:     []byte("object 1\n"),
					ContentType: testContentType,
				},
				{
					BucketName:  "bucket1",
					Name:        "object2.txt",
					Content:     []byte("object 2\n"),
					ContentType: testContentType,
				},
				{
					BucketName:  "bucket2",
					Name:        "object1.txt",
					Content:     []byte("object 1\n"),
					ContentType: testContentType,
				},
				{
					BucketName:  "bucket2",
					Name:        "object2.txt",
					Content:     []byte("object 2\n"),
					ContentType: testContentType,
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

func ensureEmptyDir(dirname string) error {
	err := os.Mkdir(dirname, 0o755)
	if err != nil {
		dir, direrr := os.Open(dirname)
		if direrr != nil {
			return fmt.Errorf("cannot create empty dir %q: mkdir failed with %v. open failed with %v", dirname, err, direrr)
		}
		defer dir.Close()

		_, direrr = dir.Readdirnames(1)
		if direrr == io.EOF {
			return nil
		}
		if direrr != nil {
			return fmt.Errorf("cannot create empty dir %q: mkdir failed with %v. readdir failed with %v", dirname, err, direrr)
		}
		return fmt.Errorf("cannot create empty dir %q: it already exists and is not empty", dirname)
	}
	return nil
}

func discardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Out = ioutil.Discard
	logger.Level = logrus.PanicLevel
	return logger
}
