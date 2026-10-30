// Copyright 2023 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGetAttributes(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	testBucket := filepath.Join(tempDir, "some-bucket")
	bucketAttrs := BucketAttrs{
		DefaultEventBasedHold: false,
		VersioningEnabled:     true,
	}
	data, _ := json.Marshal(bucketAttrs)
	err := os.WriteFile(testBucket+bucketMetadataSuffix, data, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	notABucket := filepath.Join(tempDir, "not-a-bucket")
	err = os.WriteFile(notABucket+bucketMetadataSuffix, []byte("this is not valid json"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		inputPath     string
		expectedAttrs BucketAttrs
		expectErr     bool
	}{
		{
			name:      "file not found",
			inputPath: filepath.Join(tempDir, "unknown-bucket"),
		},
		{
			name:          "existing bucket",
			inputPath:     testBucket,
			expectedAttrs: bucketAttrs,
		},
		{
			name:      "invalid file",
			inputPath: notABucket,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			attrs, err := getBucketAttributes(test.inputPath)
			if test.expectErr && err == nil {
				t.Fatal("expected error, but got <nil>")
			}

			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(attrs, test.expectedAttrs); diff != "" {
				t.Errorf("incorrect attributes returned\nwant: %#v\ngot:  %#v\ndiff: %s", test.expectedAttrs, attrs, diff)
			}
		})
	}
}

func TestListObjectsWithPrefix(t *testing.T) {
	t.Parallel()

	store, err := NewStorageFS(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const bucketName = "some-bucket"
	for _, name := range []string{"a/b.txt", "a/bb.txt", "other.txt"} {
		obj, err := store.CreateObject(Object{
			ObjectAttrs: ObjectAttrs{BucketName: bucketName, Name: name},
			Content:     []byte("content"),
		}.StreamingObject(), NoConditions{})
		if err != nil {
			t.Fatal(err)
		}
		obj.Close()
	}

	tests := []struct {
		name          string
		prefix        string
		expectedNames []string
	}{
		{name: "empty prefix", expectedNames: []string{"a/b.txt", "a/bb.txt", "other.txt"}},
		{name: "partial path segment", prefix: "a/b", expectedNames: []string{"a/b.txt", "a/bb.txt"}},
		{name: "missing subtree", prefix: "a/missing/"},
		{name: "object name followed by slash", prefix: "a/b.txt/"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects, err := store.ListObjects(bucketName, test.prefix, false)
			if err != nil {
				t.Fatal(err)
			}

			var names []string
			for _, object := range objects {
				names = append(names, object.Name)
			}
			if diff := cmp.Diff(test.expectedNames, names); diff != "" {
				t.Errorf("unexpected object names (-want +got):\n%s", diff)
			}
		})
	}

	if _, err := store.ListObjects("missing-bucket", "a/missing/", false); err == nil {
		t.Fatal("expected an error when listing objects in a missing bucket")
	}
}
