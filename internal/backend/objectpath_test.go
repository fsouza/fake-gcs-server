// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
)

func TestObjectRelPath(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, rel string }{
		{"photos/2024/a.jpg", "photos%/2024%/a.jpg"},
		{"photos/", "photos%/%_"},
		{"Photos/IMG_01.JPG", "!photos%/!i!m!g_01.!j!p!g"},
		{"my file (1).txt", "my file (1).txt"},
		{"con/nul.txt", "%63on%/%6eul.txt"},
		{"conin$", "%63onin$"},
		{"conout$.log", "%63onout$.log"},
		{"café.txt", "caf%c3%a9.txt"},
		{"a~1/b.", "a%7e1%/b%2e"},
		{"100%!", "100%25%21"},
		{"/", "%/%_"},
	} {
		rel, hashed := objectRelPath(tc.name)
		if filepath.ToSlash(rel) != tc.rel || hashed {
			t.Errorf("%q: got %q (hashed %v), want %q", tc.name, filepath.ToSlash(rel), hashed, tc.rel)
		}
	}
	// 125 uppercase letters escape to 250 bytes, which leave no room for a
	// temporary file's name.
	for _, name := range []string{strings.Repeat("x", 256), strings.Repeat("X", 125), strings.Repeat("abcdefghi/", 60)} {
		if rel, hashed := objectRelPath(name); !hashed || !strings.HasPrefix(rel, hashedDir) {
			t.Errorf("%d-byte name: got %q, want a path in %s", len(name), rel, hashedDir)
		}
	}
}

// TestObjectRelPathNames checks random names: each path is valid on every file
// system, decodes back to its name, and differs from every other path in more
// than case.
func TestObjectRelPathNames(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(1))
	alphabet := []byte{0, ' ', '!', '%', '.', '/', '~', '1', ':', '\\', 'A', 'a', 'C', 'c', 'o', 'n', 'u', 'l', 0xc3, 0xa9, 0xff}
	names := []string{"", "a", "a/", "a//b", "//", ".", "..", "a/./b", "a/../b", "CON", "nul.txt", "a~1.txt", "a ", "%h", "%_", "a%m", "a%n"}
	for range 20000 {
		b := make([]byte, 1+rnd.Intn(14))
		for i := range b {
			b[i] = alphabet[rnd.Intn(len(alphabet))]
		}
		names = append(names, string(b))
	}

	paths := map[string]string{}
	for _, name := range names {
		rel, hashed := objectRelPath(name)
		if hashed {
			continue
		}
		if got, ok := objectNameFromRelPath(rel); !ok || got != name {
			t.Fatalf("%q: path %q decodes to %q (%v)", name, rel, got, ok)
		}
		folded := strings.ToLower(rel)
		if other, ok := paths[folded]; ok && other != name {
			t.Fatalf("%q and %q have the path %q", name, other, rel)
		}
		paths[folded] = name
		for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
			if err := checkPathPart(part); err != "" {
				t.Fatalf("%q: path %q: %s", name, rel, err)
			}
		}
	}

	for _, rel := range []string{"A", "a%2fb", "a%41", "!a%", "a/b", "a%/%_x", "%h"} {
		if name, ok := objectNameFromRelPath(rel); ok {
			t.Errorf("%q decodes to %q, want it rejected", rel, name)
		}
	}
}

func checkPathPart(part string) string {
	switch {
	case part == "" || part == "." || part == "..":
		return "empty or dot part"
	case len(part) > 255:
		return "part longer than 255 bytes"
	case strings.ToLower(part) != part:
		return "uppercase"
	case strings.HasSuffix(part, ".") || strings.HasSuffix(part, " "):
		return "trailing dot or space"
	case strings.ContainsAny(part, "<>:\"\\|?*"):
		return "character Windows doesn't allow"
	}
	short, _, _ := strings.Cut(part, ".")
	if reservedWindowsNames[short] {
		return "reserved Windows name"
	}
	if i := strings.LastIndexByte(short, '~'); i >= 0 && i < len(short)-1 && strings.Trim(short[i+1:], "0123456789") == "" {
		return "Windows short name"
	}
	for _, c := range []byte(part) {
		if c < 0x20 || c >= 0x7f {
			return "control or non-ASCII byte"
		}
	}
	return ""
}
