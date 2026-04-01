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


func TestCleanupRecursiveMetadataFiles(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bucketDir := filepath.Join(rootDir, "test-bucket")
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create legitimate files.
	legitimate := []string{
		"file.txt",
		"file.txt.metadata",
		"other.csv",
		"other.csv.metadata",
	}
	for _, name := range legitimate {
		if err := os.WriteFile(filepath.Join(bucketDir, name), []byte("ok"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Create recursive metadata chains that the bug would have produced.
	bogus := []string{
		"file.txt.metadata.metadata",
		"file.txt.metadata.metadata.metadata",
		".DS_Store.metadata.metadata",
		".DS_Store.metadata.metadata.metadata.metadata",
	}
	for _, name := range bogus {
		if err := os.WriteFile(filepath.Join(bucketDir, name), []byte("bogus"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	cleanupRecursiveMetadataFiles(rootDir)

	// Verify legitimate files still exist.
	for _, name := range legitimate {
		if _, err := os.Stat(filepath.Join(bucketDir, name)); err != nil {
			t.Errorf("legitimate file %q was removed: %v", name, err)
		}
	}

	// Verify bogus files were removed.
	for _, name := range bogus {
		if _, err := os.Stat(filepath.Join(bucketDir, name)); !os.IsNotExist(err) {
			t.Errorf("bogus file %q should have been removed, err=%v", name, err)
		}
	}
}
