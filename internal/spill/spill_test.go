// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spill

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestBuffer(t *testing.T) {
	for _, limit := range []int64{100, 4} {
		b := New(t.TempDir(), limit)
		if _, err := b.ReadFrom(strings.NewReader("abcdef")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.WriteAt([]byte("XY"), 2); err != nil {
			t.Fatal(err)
		}
		if _, err := b.WriteAt([]byte("Z"), 7); err == nil {
			t.Error("write past the end succeeded")
		}
		if err := b.Truncate(5); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ReadFrom(strings.NewReader("gh")); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(b.Reader())
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "abXYegh" || b.Size() != 7 {
			t.Errorf("limit %d: content %q (size %d), want %q", limit, got, b.Size(), "abXYegh")
		}
		_, inMemory := b.Bytes()
		name, err := b.TakeFile()
		if err != nil {
			t.Fatal(err)
		}
		if inMemory != (limit == 100) || inMemory != (name == "") {
			t.Errorf("limit %d: in memory %v, file %q", limit, inMemory, name)
		}
		if name != "" {
			if got, err := os.ReadFile(name); err != nil || string(got) != "abXYegh" {
				t.Errorf("limit %d: taken file has %q (%v)", limit, got, err)
			}
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if name != "" {
			if _, err := os.Stat(name); !os.IsNotExist(err) {
				t.Errorf("limit %d: %s still exists after Close: %v", limit, name, err)
			}
		}
	}
}
