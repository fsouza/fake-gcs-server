// Copyright 2023 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"encoding/json"
	"io"
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

// On Windows, TestStorageFSReplaceWhileOpen needs openFile and rename.
func TestStorageFSReplaceWhileOpen(t *testing.T) {
	const small, large = 10, 3 << 20
	create := func(t *testing.T, s Storage, content io.ReadSeeker) {
		t.Helper()
		obj, err := s.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "object"},
			Content:     noopSeekCloser{content},
		}, NoConditions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = obj.Close()
	}
	for _, sizes := range [][2]int{{small, small}, {small, large}, {large, small}, {large, large}} {
		s, err := NewStorageFS(nil, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		old := bytes.Repeat([]byte("o"), sizes[0])
		create(t, s, bytes.NewReader(old))
		reader, err := s.GetObject("bucket", "object")
		if err != nil {
			t.Fatal(err)
		}
		first := make([]byte, 5)
		if _, err := io.ReadFull(reader.Content, first); err != nil {
			t.Fatal(err)
		}
		create(t, s, bytes.NewReader(bytes.Repeat([]byte("n"), sizes[1])))
		rest, err := io.ReadAll(reader.Content)
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := append(first, rest...); !bytes.Equal(got, old) {
			t.Errorf("%d bytes replaced by %d: open reader read %d bytes, want the old %d", sizes[0], sizes[1], len(got), len(old))
		}

		// Copy the new content onto itself.
		src, err := s.GetObject("bucket", "object")
		if err != nil {
			t.Fatal(err)
		}
		create(t, s, src.Content)
		_ = src.Close()
		got, err := s.GetObject("bucket", "object")
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(got.Content)
		_ = got.Close()
		if err != nil || !bytes.Equal(content, bytes.Repeat([]byte("n"), sizes[1])) {
			t.Errorf("%d bytes copied onto itself: read %d bytes (%v)", sizes[1], len(content), err)
		}
	}
}
