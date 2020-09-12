// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func TestServerClientBucketAttrs(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
		{BucketName: "other-bucket", Name: "static/css/website.css"},
	}
	startTime := time.Now()
	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		attrs, err := client.Bucket("some-bucket").Attrs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		expectedName := "some-bucket"
		if attrs.Name != expectedName {
			t.Errorf("wrong bucket name returned\nwant %q\ngot  %q", expectedName, attrs.Name)
		}
		if attrs.VersioningEnabled != false {
			t.Errorf("wrong bucket props for %q\nexpecting no versioning by default, got it enabled", expectedName)
		}
		if attrs.Created.Before(startTime.Truncate(time.Second)) || time.Now().Before(attrs.Created) {
			t.Errorf("expecting bucket creation date between test start time %v and now %v, got %v", startTime, time.Now(), attrs.Created)
		}
	})
}

func TestServerClientBucketAttrsAfterCreateBucket(t *testing.T) {
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		runServersTest(t, nil, func(t *testing.T, server *Server) {
			const bucketName = "best-bucket-ever"
			server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, VersioningEnabled: versioningEnabled})
			client := server.Client()
			attrs, err := client.Bucket(bucketName).Attrs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if attrs.Name != bucketName {
				t.Errorf("wrong bucket name returned\nwant %q\ngot  %q", bucketName, attrs.Name)
			}
			if attrs.VersioningEnabled != versioningEnabled {
				t.Errorf("wrong bucket props for %q:\nwant versioningEnabled: %t\ngot versioningEnabled: %t", bucketName, versioningEnabled, attrs.VersioningEnabled)
			}
		})
	}
}

func TestServerClientBucketAttrsAfterCreateBucketByPost(t *testing.T) {
	t.Parallel()
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		runServersTest(t, nil, func(t *testing.T, server *Server) {
			const bucketName = "post-bucket"
			client := server.Client()
			bucket := client.Bucket(bucketName)

			bucketAttrs := storage.BucketAttrs{
				VersioningEnabled: versioningEnabled,
			}
			if err := bucket.Create(context.Background(), "whatever", &bucketAttrs); err != nil {
				t.Fatal(err)
			}
			attrs, err := client.Bucket(bucketName).Attrs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if attrs.Name != bucketName {
				t.Errorf("wrong bucket name returned\nwant %q\ngot  %q", bucketName, attrs.Name)
			}

			if attrs.VersioningEnabled != bucketAttrs.VersioningEnabled {
				t.Errorf("wrong bucket props for %q:\nwant versioningEnabled: %t\ngot versioningEnabled: %t", bucketName, bucketAttrs.VersioningEnabled, attrs.VersioningEnabled)
			}
		})
	}
}

func TestServerClientBucketCreateValidation(t *testing.T) {
	bucketNames := []string{
		"..what-is-this",
		".host.name.cant.start.with.dot",
		"no_underlines_either",
		"or spaces",
		"don't even try",
		"no/slashes/either",
	}

	for _, bucketName := range bucketNames {
		bucketName := bucketName
		runServersTest(t, nil, func(t *testing.T, server *Server) {
			client := server.Client()
			err := client.Bucket(bucketName).Create(context.Background(), "whatever", nil)
			if err == nil {
				t.Error("unexpected <nil> error")
			}
		})
	}
}

func TestServerClientBucketAttrsNotFound(t *testing.T) {
	runServersTest(t, nil, func(t *testing.T, server *Server) {
		client := server.Client()
		attrs, err := client.Bucket("some-bucket").Attrs(context.Background())
		if err == nil {
			t.Error("unexpected <nil> error")
		}
		if attrs != nil {
			t.Errorf("unexpected non-nil attrs: %#v", attrs)
		}
	})
}

func TestServerClientListBuckets(t *testing.T) {
	objs := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
		{BucketName: "other-bucket", Name: "static/css/website.css"},
	}

	runServersTest(t, objs, func(t *testing.T, server *Server) {
		client := server.Client()
		const versionedBucketName = "post-bucket-with-versioning"
		versionedBucketAttrs := storage.BucketAttrs{
			VersioningEnabled: true,
		}
		if err := client.Bucket(versionedBucketName).Create(context.Background(), "whatever", &versionedBucketAttrs); err != nil {
			t.Fatal(err)
		}
		it := client.Buckets(context.Background(), "whatever")
		expectedBuckets := map[string]bool{
			"other-bucket": false, "some-bucket": false, versionedBucketName: true,
		}
		b, err := it.Next()
		numberOfBuckets := 0
		for ; err == nil; b, err = it.Next() {
			numberOfBuckets++
			versioning, found := expectedBuckets[b.Name]
			if !found {
				t.Errorf("unexpected bucket found\nname %s", b.Name)
				continue
			}
			if versioning != b.VersioningEnabled {
				t.Errorf("unexpected versioning value for %s\nwant %t\ngot  %t", b.Name, versioning, b.VersioningEnabled)
			}
		}
		if err != iterator.Done {
			t.Fatal(err)
		}

		if len(expectedBuckets) != numberOfBuckets {
			t.Errorf("wrong number of buckets returned\nwant %d\ngot  %d", len(expectedBuckets), numberOfBuckets)
		}
	})
}

func TestServerClientListObjects(t *testing.T) {
	objects := []Object{
		{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"},
		{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"},
	}
	dir, err := ioutil.TempDir("", "fakestorage-test-root-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	serverOptions := []Options{
		{InitialObjects: objects},
		{InitialObjects: objects, StorageRoot: dir},
		{InitialObjects: objects, NoListener: true},
		{InitialObjects: objects, NoListener: true, StorageRoot: dir},
	}
	for _, options := range serverOptions {
		options := options
		t.Run("", func(t *testing.T) {
			server, err := NewServerWithOptions(options)
			if err != nil {
				t.Error(err)
			}
			defer server.Stop()
			client := server.Client()
			seenFiles := map[string]struct{}{}
			it := client.Bucket("some-bucket").Objects(context.Background(), nil)
			objAttrs, err := it.Next()
			for ; err == nil; objAttrs, err = it.Next() {
				seenFiles[objAttrs.Name] = struct{}{}
				t.Logf("seen file %s", objAttrs.Name)
			}
			if len(objects) != len(seenFiles) {
				t.Errorf("wrong number of files\nwant %d\ngot %d", len(objects), len(seenFiles))
			}
		})
	}
}
