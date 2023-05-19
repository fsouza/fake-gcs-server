// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	/* remove later */
	//"fmt"
)

func tempDir() string {
	if runtime.GOOS == "linux" {
		return "/var/tmp"
	} else {
		return os.TempDir()
	}
}

func TestServerClientUpdateBucketAttrs(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const bucketName = "best-bucket-ever"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, DefaultEventBasedHold: false})
		client := server.Client()
		_, err := client.Bucket(bucketName).Update(context.TODO(), storage.BucketAttrsToUpdate{DefaultEventBasedHold: true})
		if err != nil {
			t.Fatal(err)
		}
		attrs, err := client.Bucket(bucketName).Attrs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !attrs.DefaultEventBasedHold {
			t.Errorf("expected default event based hold to be true, instead got: %v", attrs.DefaultEventBasedHold)
		}
	})
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		const bucketName = "best-bucket-ever"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, VersioningEnabled: false})
		client := server.Client()
		_, err := client.Bucket(bucketName).Update(context.TODO(), storage.BucketAttrsToUpdate{VersioningEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		attrs, err := client.Bucket(bucketName).Attrs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !attrs.VersioningEnabled {
			t.Errorf("expected VersioningEnabled hold to be true, instead got: %v", attrs.VersioningEnabled)
		}
	})
}

func TestServerClientStoreAndRetrieveBucketAttrs(t *testing.T) {
	for _, defaultEventBasedHold := range []bool{true, false} {
		defaultEventBasedHold := defaultEventBasedHold

		runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
			const bucketName = "best-bucket-ever"
			server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName, DefaultEventBasedHold: defaultEventBasedHold})
			client := server.Client()
			attrs, err := client.Bucket(bucketName).Attrs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if attrs.DefaultEventBasedHold != defaultEventBasedHold {
				t.Errorf("expected default event based hold to be: %v", defaultEventBasedHold)
			}
		})
	}
}

func TestServerClientBucketAlreadyExists(t *testing.T) {
	objs := []Object{
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"}},
	}
	runServersTest(t, runServersOptions{objs: objs}, func(t *testing.T, server *Server) {
		client := server.Client()
		err := client.Bucket("some-bucket").Create(context.Background(), "whatever", nil)
		if err == nil {
			t.Errorf("expected a 409 error")
		}
		apiErr := new(googleapi.Error)
		if !errors.As(err, &apiErr) {
			t.Errorf("expected a google API error, got %T", err)
		}
		if apiErr.Code != 409 {
			t.Errorf("expected a google API error with code 409, got %v", apiErr.Code)
		}
	})
}

func TestServerClientBucketAttrs(t *testing.T) {
	objs := []Object{
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "other_bucket", Name: "static/css/website.css"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "dot.bucket", Name: "static/js/app.js"}},
	}
	startTime := time.Now()
	runServersTest(t, runServersOptions{objs: objs}, func(t *testing.T, server *Server) {
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
		runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
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

func TestServerClientDeleteBucket(t *testing.T) {
	t.Run("it deletes empty buckets", func(t *testing.T) {
		const bucketName = "bucket-to-delete"
		runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
			server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})
			client := server.Client()
			err := client.Bucket(bucketName).Delete(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			attrs, err := client.Bucket(bucketName).Attrs(context.Background())
			if err == nil {
				t.Error("unexpected <nil> error")
			}
			if attrs != nil {
				t.Errorf("unexpected non-nil attrs: %#v", attrs)
			}
		})
	})

	t.Run("it returns an error for non-empty buckets", func(t *testing.T) {
		const bucketName = "non-empty-bucket"
		objs := []Object{{ObjectAttrs: ObjectAttrs{BucketName: bucketName, Name: "static/js/app.js"}}}
		runServersTest(t, runServersOptions{objs: objs, enableFSBackend: true}, func(t *testing.T, server *Server) {
			client := server.Client()
			err := client.Bucket(bucketName).Delete(context.Background())
			if err == nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("it returns an error for unknown buckets", func(t *testing.T) {
		const bucketName = "non-existent-bucket"
		runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
			client := server.Client()
			err := client.Bucket(bucketName).Delete(context.Background())
			if err == nil {
				t.Fatal(err)
			}
		})
	})
}

func TestServerClientBucketAttrsAfterCreateBucketByPost(t *testing.T) {
	t.Parallel()
	for _, versioningEnabled := range []bool{true, false} {
		versioningEnabled := versioningEnabled
		runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
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
		"-hostname-cant-start-with-dash",
		"_hostname-cant-start-with-underscore",
		"hostname-cant-end-with-dash-",
		"hostname-cant-end-with-underscore_",
		".host.name.cant.start.with.dot",
		"host.name.cant.end.with.dot.",
		"or spaces",
		"don't even try",
		"no/slashes/either",
	}

	for _, bucketName := range bucketNames {
		bucketName := bucketName
		runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
			client := server.Client()
			err := client.Bucket(bucketName).Create(context.Background(), "whatever", nil)
			if err == nil {
				t.Error("unexpected <nil> error")
			}
		})
	}
}

func TestServerClientBucketAttrsNotFound(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
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
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "other_bucket", Name: "static/css/website.css"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "dot.bucket", Name: "static/js/app.js"}},
	}

	runServersTest(t, runServersOptions{objs: objs}, func(t *testing.T, server *Server) {
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
			"other_bucket": false, "dot.bucket": false, "some-bucket": false, versionedBucketName: true,
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
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-01.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-02.jpg"}},
		{ObjectAttrs: ObjectAttrs{BucketName: "some-bucket", Name: "img/hi-res/party-03.jpg"}},
	}
	dir, err := os.MkdirTemp(tempDir(), "fakestorage-test-root-")
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
				t.Fatal(err)
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
