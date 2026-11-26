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
	"time"

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

func TestNewStorageFSRemovesStaleTempFiles(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, tmpDirName)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	stale, fresh := filepath.Join(dir, "stale"), filepath.Join(dir, "fresh")
	for _, name := range []string{stale, fresh} {
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStorageFS(nil, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale temporary file still exists: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh temporary file removed: %v", err)
	}
}

func TestStorageFSLargeObjectAfterDeleteAllFiles(t *testing.T) {
	s, err := NewStorageFS(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAllFiles(); err != nil {
		t.Fatal(err)
	}
	obj := StreamingObject{
		ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "object"},
		Content:     noopSeekCloser{bytes.NewReader(make([]byte, 2<<20))},
	}
	created, err := s.CreateObject(obj, NoConditions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = created.Close()
}

func TestStorageFSPatchObjectKeepsContent(t *testing.T) {
	root := t.TempDir()
	s, err := NewStorageFS(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.CreateObject(StreamingObject{
		ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "object"},
		Content:     noopSeekCloser{bytes.NewReader([]byte("content"))},
	}, NoConditions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = created.Close()
	path := filepath.Join(root, "bucket", "object")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	patched, err := s.PatchObject("bucket", "object", ObjectAttrs{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	_ = patched.Close()
	if patched.Generation == created.Generation {
		t.Error("patch kept the generation")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Error("patch rewrote the content file")
	}
	got, err := s.GetObject("bucket", "object")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.ContentType != "text/plain" || got.Generation != patched.Generation {
		t.Errorf("stored attributes: content type %q, generation %d; want %q, %d", got.ContentType, got.Generation, "text/plain", patched.Generation)
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
