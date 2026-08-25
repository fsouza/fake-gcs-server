// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

func assertPreconditionFailed(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the precondition to fail, got no error")
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a googleapi error, got %v", err)
	}
	if apiErr.Code != http.StatusPreconditionFailed {
		t.Errorf("wrong status\nwant %d\ngot  %d", http.StatusPreconditionFailed, apiErr.Code)
	}
}

// seedObject puts an object with known content and hands back a handle on it
// along with the generation it was created at.
func seedObject(t *testing.T, server *Server, bucketName, name string) (*storage.ObjectHandle, int64) {
	t.Helper()
	server.CreateObject(Object{
		ObjectAttrs: ObjectAttrs{BucketName: bucketName, Name: name, ContentType: "text/plain"},
		Content:     []byte("some content"),
	})
	handle := server.Client().Bucket(bucketName).Object(name)
	attrs, err := handle.Attrs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return handle, attrs.Generation
}

func TestServerObjectDeletePreconditions(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const bucketName = "some-bucket"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})
		ctx := context.Background()

		t.Run("generation that does not match", func(t *testing.T) {
			handle, generation := seedObject(t, server, bucketName, "delete-wrong-generation.txt")
			err := handle.If(storage.Conditions{GenerationMatch: generation + 1}).Delete(ctx)
			assertPreconditionFailed(t, err)
			if _, err := handle.Attrs(ctx); err != nil {
				t.Errorf("object went away despite the failed precondition: %v", err)
			}
		})

		t.Run("generation that matches", func(t *testing.T) {
			handle, generation := seedObject(t, server, bucketName, "delete-right-generation.txt")
			if err := handle.If(storage.Conditions{GenerationMatch: generation}).Delete(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := handle.Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
				t.Errorf("object outlived a delete its precondition allowed: %v", err)
			}
		})

		t.Run("metageneration that does not match", func(t *testing.T) {
			handle, _ := seedObject(t, server, bucketName, "delete-wrong-metageneration.txt")
			err := handle.If(storage.Conditions{MetagenerationMatch: objectMetageneration + 1}).Delete(ctx)
			assertPreconditionFailed(t, err)
			if _, err := handle.Attrs(ctx); err != nil {
				t.Errorf("object went away despite the failed precondition: %v", err)
			}
		})

		t.Run("generation that must not match", func(t *testing.T) {
			handle, generation := seedObject(t, server, bucketName, "delete-generation-not-match.txt")
			err := handle.If(storage.Conditions{GenerationNotMatch: generation}).Delete(ctx)
			assertPreconditionFailed(t, err)
			if _, err := handle.Attrs(ctx); err != nil {
				t.Errorf("object went away despite the failed precondition: %v", err)
			}
		})
	})
}

func TestServerObjectUpdatePreconditions(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const bucketName = "some-bucket"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})
		ctx := context.Background()
		update := storage.ObjectAttrsToUpdate{ContentType: "application/json"}

		t.Run("metageneration that does not match", func(t *testing.T) {
			handle, _ := seedObject(t, server, bucketName, "update-wrong-metageneration.txt")
			_, err := handle.If(storage.Conditions{
				MetagenerationMatch: objectMetageneration + 1,
			}).Update(ctx, update)
			assertPreconditionFailed(t, err)
			attrs, err := handle.Attrs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if attrs.ContentType != "text/plain" {
				t.Errorf("object was updated despite the failed precondition: content type is %q", attrs.ContentType)
			}
		})

		t.Run("metageneration that matches", func(t *testing.T) {
			handle, _ := seedObject(t, server, bucketName, "update-right-metageneration.txt")
			attrs, err := handle.If(storage.Conditions{
				MetagenerationMatch: objectMetageneration,
			}).Update(ctx, update)
			if err != nil {
				t.Fatal(err)
			}
			if attrs.ContentType != "application/json" {
				t.Errorf("wrong content type\nwant %q\ngot  %q", "application/json", attrs.ContentType)
			}
		})

		t.Run("generation that does not match", func(t *testing.T) {
			handle, generation := seedObject(t, server, bucketName, "update-wrong-generation.txt")
			_, err := handle.If(storage.Conditions{GenerationMatch: generation + 1}).Update(ctx, update)
			assertPreconditionFailed(t, err)
		})
	})
}

func TestServerObjectCopyPreconditions(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const bucketName = "some-bucket"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})
		ctx := context.Background()
		bucket := server.Client().Bucket(bucketName)

		t.Run("source generation that does not match", func(t *testing.T) {
			source, generation := seedObject(t, server, bucketName, "copy-source-wrong.txt")
			destination := bucket.Object("copy-source-wrong-destination.txt")
			_, err := destination.CopierFrom(
				source.If(storage.Conditions{GenerationMatch: generation + 1})).Run(ctx)
			assertPreconditionFailed(t, err)
			if _, err := destination.Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
				t.Errorf("destination was written despite the failed precondition: %v", err)
			}
		})

		t.Run("destination generation that does not match", func(t *testing.T) {
			source, _ := seedObject(t, server, bucketName, "copy-destination-wrong-source.txt")
			destination, generation := seedObject(t, server, bucketName, "copy-destination-wrong.txt")
			_, err := destination.If(storage.Conditions{
				GenerationMatch: generation + 1,
			}).CopierFrom(source).Run(ctx)
			assertPreconditionFailed(t, err)
		})

		t.Run("destination that must not exist but does", func(t *testing.T) {
			source, _ := seedObject(t, server, bucketName, "copy-exists-source.txt")
			destination, _ := seedObject(t, server, bucketName, "copy-exists-destination.txt")
			_, err := destination.If(storage.Conditions{
				DoesNotExist: true,
			}).CopierFrom(source).Run(ctx)
			assertPreconditionFailed(t, err)
		})

		t.Run("conditions that hold", func(t *testing.T) {
			source, sourceGeneration := seedObject(t, server, bucketName, "copy-ok-source.txt")
			destination := bucket.Object("copy-ok-destination.txt")
			_, err := destination.If(storage.Conditions{DoesNotExist: true}).CopierFrom(
				source.If(storage.Conditions{GenerationMatch: sourceGeneration})).Run(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := destination.Attrs(ctx); err != nil {
				t.Errorf("destination was not written though its preconditions held: %v", err)
			}
		})
	})
}

func TestServerObjectComposePreconditions(t *testing.T) {
	runServersTest(t, runServersOptions{enableFSBackend: true}, func(t *testing.T, server *Server) {
		const bucketName = "some-bucket"
		server.CreateBucketWithOpts(CreateBucketOpts{Name: bucketName})
		ctx := context.Background()
		bucket := server.Client().Bucket(bucketName)
		first, _ := seedObject(t, server, bucketName, "compose-source-1.txt")
		second, _ := seedObject(t, server, bucketName, "compose-source-2.txt")

		t.Run("generation that does not match", func(t *testing.T) {
			destination, generation := seedObject(t, server, bucketName, "compose-wrong-generation.txt")
			_, err := destination.If(storage.Conditions{
				GenerationMatch: generation + 1,
			}).ComposerFrom(first, second).Run(ctx)
			assertPreconditionFailed(t, err)
			attrs, err := destination.Attrs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if attrs.Generation != generation {
				t.Errorf("destination was composed despite the failed precondition")
			}
		})

		t.Run("destination that must not exist but does", func(t *testing.T) {
			destination, _ := seedObject(t, server, bucketName, "compose-exists.txt")
			_, err := destination.If(storage.Conditions{
				DoesNotExist: true,
			}).ComposerFrom(first, second).Run(ctx)
			assertPreconditionFailed(t, err)
		})

		t.Run("destination that must not exist and does not", func(t *testing.T) {
			destination := bucket.Object("compose-new.txt")
			composer := destination.If(storage.Conditions{DoesNotExist: true}).ComposerFrom(first, second)
			composer.ContentType = "text/plain"
			if _, err := composer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := destination.Attrs(ctx); err != nil {
				t.Errorf("destination was not composed though its precondition held: %v", err)
			}
		})
	})
}

func TestParsePreconditions(t *testing.T) {
	generation := func(value int64) *int64 { return &value }
	tests := []struct {
		name   string
		query  string
		prefix string
		want   preconditions
	}{
		{"nothing named", "", "", preconditions{}},
		{
			"the whole family",
			"ifGenerationMatch=1&ifGenerationNotMatch=2&ifMetagenerationMatch=3&ifMetagenerationNotMatch=4",
			"",
			preconditions{
				ifGenerationMatch:        generation(1),
				ifGenerationNotMatch:     generation(2),
				ifMetagenerationMatch:    generation(3),
				ifMetagenerationNotMatch: generation(4),
			},
		},
		{
			"the source family, which the plain prefix must not see",
			"ifSourceGenerationMatch=5&ifGenerationMatch=6",
			"Source",
			preconditions{ifGenerationMatch: generation(5)},
		},
		{
			"the plain family, which the source prefix must not see",
			"ifSourceGenerationMatch=5&ifGenerationMatch=6",
			"",
			preconditions{ifGenerationMatch: generation(6)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := url.ParseQuery(test.query)
			if err != nil {
				t.Fatal(err)
			}
			got, err := parsePreconditions(query, test.prefix)
			if err != nil {
				t.Fatal(err)
			}
			assertSameConditions(t, test.want, got)
		})
	}

	t.Run("a value that is not a number", func(t *testing.T) {
		query, err := url.ParseQuery("ifGenerationMatch=notanumber")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parsePreconditions(query, ""); err == nil {
			t.Error("expected a malformed condition to be rejected")
		}
	})
}

func assertSameConditions(t *testing.T, want, got preconditions) {
	t.Helper()
	for _, field := range []struct {
		name string
		want *int64
		got  *int64
	}{
		{"ifGenerationMatch", want.ifGenerationMatch, got.ifGenerationMatch},
		{"ifGenerationNotMatch", want.ifGenerationNotMatch, got.ifGenerationNotMatch},
		{"ifMetagenerationMatch", want.ifMetagenerationMatch, got.ifMetagenerationMatch},
		{"ifMetagenerationNotMatch", want.ifMetagenerationNotMatch, got.ifMetagenerationNotMatch},
	} {
		switch {
		case field.want == nil && field.got == nil:
		case field.want == nil || field.got == nil:
			t.Errorf("%s: want %v, got %v", field.name, field.want, field.got)
		case *field.want != *field.got:
			t.Errorf("%s: want %d, got %d", field.name, *field.want, *field.got)
		}
	}
}

func TestPreconditionsConditionsMet(t *testing.T) {
	value := func(v int64) *int64 { return &v }
	tests := []struct {
		name       string
		conditions preconditions
		generation int64
		want       bool
	}{
		{"nothing named", preconditions{}, 7, true},
		{"generation matches", preconditions{ifGenerationMatch: value(7)}, 7, true},
		{"generation differs", preconditions{ifGenerationMatch: value(8)}, 7, false},
		{"must not exist and does not", preconditions{ifGenerationMatch: value(0)}, 0, true},
		{"must not exist but does", preconditions{ifGenerationMatch: value(0)}, 7, false},
		{"generation must differ and does", preconditions{ifGenerationNotMatch: value(8)}, 7, true},
		{"generation must differ but matches", preconditions{ifGenerationNotMatch: value(7)}, 7, false},
		{"metageneration matches", preconditions{ifMetagenerationMatch: value(objectMetageneration)}, 7, true},
		{"metageneration differs", preconditions{ifMetagenerationMatch: value(objectMetageneration + 1)}, 7, false},
		{"metageneration must differ but matches", preconditions{ifMetagenerationNotMatch: value(objectMetageneration)}, 7, false},
		{
			"an absent object has no metageneration to compare",
			preconditions{ifMetagenerationMatch: value(objectMetageneration + 1)},
			0,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.conditions.ConditionsMet(test.generation); got != test.want {
				t.Errorf("wrong answer for generation %d\nwant %v\ngot  %v", test.generation, test.want, got)
			}
		})
	}
}
