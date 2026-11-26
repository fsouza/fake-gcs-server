// Copyright 2023 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// TestObjectNames stores, lists, reads and deletes objects whose names didn't work as
// paths: one ending in a slash, one that is a prefix of other names, and one longer
// than a file name may be.
func TestObjectNames(t *testing.T) {
	t.Parallel()

	long := "dir/.\xf3\xbb\xbe\x80/S/" + strings.Repeat("b -C0+J+'-ihd|U;}VU8Q{Me_-oqb*?|>I+", 8)
	// "a\\b" is a different object from "a/b", also on Windows. 125 uppercase
	// letters escape to a path that leaves no room for a temporary file.
	names := []string{"dir/", "dir", "dir/file", long, "a/b", "a\\b", strings.Repeat("X", 125)}
	root := t.TempDir()
	storage, err := NewStorageFS(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		obj, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: name},
			Content:     noopSeekCloser{bytes.NewReader([]byte(name))},
		}, NoConditions{})
		if err != nil {
			t.Fatalf("creating %q: %v", name, err)
		}
		obj.Close()
	}
	// A file whose metadata wasn't written, as after a crash, isn't an object.
	if err := os.WriteFile(filepath.Join(root, "bucket", "orphan"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	objects, err := storage.ListObjects("bucket", "dir/", false)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, o := range objects {
		listed = append(listed, o.Name)
	}
	if diff := cmp.Diff([]string{"dir/", long, "dir/file"}, listed); diff != "" {
		t.Errorf("listed objects with prefix dir/ (-want +got):\n%s", diff)
	}

	for _, name := range names {
		obj, err := storage.GetObject("bucket", name)
		if err != nil {
			t.Fatalf("getting %q: %v", name, err)
		}
		content, err := io.ReadAll(obj.Content)
		obj.Close()
		if err != nil || obj.Name != name || string(content) != name {
			t.Errorf("got %q with %q (%v), want its name as content", obj.Name, content, err)
		}
	}

	if _, err := storage.PatchObject("bucket", "a\\b", ObjectAttrs{ContentType: "text/x"}); err != nil {
		t.Fatal(err)
	}
	if obj, err := storage.GetObject("bucket", "a/b"); err != nil || obj.ContentType == "text/x" {
		t.Errorf("patching a\\b changed a/b: %v, %q", err, obj.ContentType)
	} else {
		obj.Close()
	}

	for _, name := range []string{"dir/", long} {
		if err := storage.DeleteObject("bucket", name); err != nil {
			t.Fatal(err)
		}
		if _, err := storage.GetObject("bucket", name); err == nil {
			t.Errorf("%q is still there after DeleteObject", name)
		}
	}
	if objects, _ := storage.ListObjects("bucket", "", false); len(objects) != len(names)-2 {
		t.Errorf("%d objects left after deleting two of %d", len(objects), len(names))
	}
	path, _ := storage.(*storageFS).objectPath("bucket", long)
	for _, p := range []string{path, path + nameSuffix} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s is still there after deleting the long name: %v", p, err)
		}
	}
}

// TestObjectNamesRandom stores objects with random names and checks that they
// are listed in order and read back. On a file system that ignores case, it
// also checks that no two names share a file.
func TestObjectNamesRandom(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(1))
	alphabet := []byte{' ', '!', '%', '.', '/', '~', '1', 'A', 'a', 'c', 'o', 'n', 0xc3, 0xa9, 0xff}
	seen := map[string]bool{}
	var names []string
	for len(names) < 500 {
		b := make([]byte, 1+rnd.Intn(10))
		for i := range b {
			b[i] = alphabet[rnd.Intn(len(alphabet))]
		}
		if !seen[string(b)] {
			seen[string(b)] = true
			names = append(names, string(b))
		}
	}
	storage, err := NewStorageFS(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		obj, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: name},
			Content:     noopSeekCloser{bytes.NewReader([]byte(name))},
		}, NoConditions{})
		if err != nil {
			t.Fatalf("creating %q: %v", name, err)
		}
		obj.Close()
	}
	objects, err := storage.ListObjects("bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, o := range objects {
		listed = append(listed, o.Name)
	}
	slices.Sort(names)
	if diff := cmp.Diff(names, listed); diff != "" {
		t.Errorf("listed objects (-want +got):\n%s", diff)
	}
	for _, name := range names {
		obj, err := storage.GetObject("bucket", name)
		if err != nil {
			t.Fatalf("getting %q: %v", name, err)
		}
		content, err := io.ReadAll(obj.Content)
		obj.Close()
		if err != nil || string(content) != name {
			t.Errorf("%q has content %q (%v)", name, content, err)
		}
	}
}

// TestMigrateObjectPaths opens roots written by earlier versions, which stored
// each object under its name as a path, and checks that their objects are found:
// in a root no migration has started on, and in one where a migration stopped
// after moving some objects.
func TestMigrateObjectPaths(t *testing.T) {
	t.Parallel()

	probe, err := NewStorageFS(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mh := probe.(*storageFS).mh
	// "a/b" moves to a%/b, where the old layout has "a%/b".
	names := []string{"Upper/File.txt", "a%/b", "a/b", "dir/file", "dir/sub/file", "top", strings.Repeat("abcdefghi/", 60) + "f"}
	writeOld := func(t *testing.T, bucketPath, name string) {
		t.Helper()
		path := filepath.Join(bucketPath, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(ObjectAttrs{BucketName: "bucket", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := mh.(metadataFile); ok {
			err = os.WriteFile(path+oldMetadataSuffix, encoded, 0o600)
		} else {
			err = mh.write(path, encoded)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	check := func(t *testing.T, root string) {
		t.Helper()
		storage, err := NewStorageFS(nil, root)
		if err != nil {
			t.Fatal(err)
		}
		objects, err := storage.ListObjects("bucket", "", false)
		if err != nil {
			t.Fatal(err)
		}
		var listed []string
		for _, o := range objects {
			listed = append(listed, o.Name)
		}
		want := slices.Sorted(slices.Values(names))
		if diff := cmp.Diff(want, listed); diff != "" {
			t.Errorf("listed objects (-want +got):\n%s", diff)
		}
		for _, name := range names {
			obj, err := storage.GetObject("bucket", name)
			if err != nil {
				t.Fatalf("getting %q: %v", name, err)
			}
			content, err := io.ReadAll(obj.Content)
			obj.Close()
			if err != nil || string(content) != name {
				t.Errorf("%q has content %q (%v)", name, content, err)
			}
		}
		for _, name := range []string{migrationDir, migratingFile, filepath.Join("bucket", "dir")} {
			if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
				t.Errorf("%s is still there: %v", name, err)
			}
		}

		// A root emptied by DeleteAllFiles isn't migrated again.
		if err := storage.DeleteAllFiles(); err != nil {
			t.Fatal(err)
		}
		obj, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "a/b"},
			Content:     noopSeekCloser{bytes.NewReader(nil)},
		}, NoConditions{})
		if err != nil {
			t.Fatal(err)
		}
		obj.Close()
		if storage, err = NewStorageFS(nil, root); err != nil {
			t.Fatal(err)
		}
		if obj, err := storage.GetObject("bucket", "a/b"); err != nil {
			t.Errorf("after DeleteAllFiles and a restart: %v", err)
		} else {
			obj.Close()
		}
	}

	t.Run("not started", func(t *testing.T) {
		root := t.TempDir()
		for _, name := range names {
			writeOld(t, filepath.Join(root, "bucket"), name)
		}
		check(t, root)
	})

	t.Run("stopped part way", func(t *testing.T) {
		root := t.TempDir()
		storage := &storageFS{rootDir: root, mh: mh}
		for _, name := range names[1:] {
			writeOld(t, filepath.Join(root, migrationDir, "bucket"), name)
		}
		if err := os.WriteFile(filepath.Join(root, migratingFile), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		// names[0] was moved before the migration stopped.
		obj, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: names[0]},
			Content:     noopSeekCloser{bytes.NewReader([]byte(names[0]))},
		}, NoConditions{})
		if err != nil {
			t.Fatal(err)
		}
		obj.Close()
		check(t, root)
	})
}

// TestMigrateObjectPathsMetadataFiles migrates a root with metadata in files,
// where objects named "x.metadata" have theirs in "x.metadata.metadata".
func TestMigrateObjectPathsMetadataFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	names := []string{"dir/file", "dir/x.metadata", "foo", "plain.metadata", "y.metadata.metadata"}
	for _, name := range names {
		path := filepath.Join(root, "bucket", filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(ObjectAttrs{BucketName: "bucket", Name: name, ContentType: "text/" + name})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+oldMetadataSuffix, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	storage := &storageFS{rootDir: root, mh: metadataFile{}}
	if err := storage.migrateObjectPaths(); err != nil {
		t.Fatal(err)
	}
	objects, err := storage.ListObjects("bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, o := range objects {
		listed = append(listed, o.Name)
		if o.ContentType != "text/"+o.Name {
			t.Errorf("%q has content type %q, want the metadata written for it", o.Name, o.ContentType)
		}
	}
	if diff := cmp.Diff(names, listed); diff != "" {
		t.Errorf("listed objects (-want +got):\n%s", diff)
	}
	for _, name := range names {
		obj, err := storage.GetObject("bucket", name)
		if err != nil {
			t.Fatalf("getting %q: %v", name, err)
		}
		content, err := io.ReadAll(obj.Content)
		obj.Close()
		if err != nil || string(content) != name {
			t.Errorf("%q has content %q (%v)", name, content, err)
		}
	}
}
