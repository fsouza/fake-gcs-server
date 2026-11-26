// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// Object names are stored as paths in the bucket directory, escaped so that
// any name works on every file system and common names stay readable:
//
//	photos/2024/a.jpg  ->  photos%/2024%/a.jpg
//	photos/            ->  photos%/%_
//	Photos/IMG_01.JPG  ->  !photos%/!i!m!g_01.!j!p!g
//	my file (1).txt    ->  my file (1).txt
//	con/nul.txt        ->  %63on%/%6eul.txt
//	café.txt           ->  caf%c3%a9.txt
//
// Each segment between slashes keeps a-z, 0-9 and the punctuation that every
// file system allows. An uppercase letter becomes "!" and the lowercase letter,
// as in Go's module cache, so no two paths differ only in case. Any other byte
// becomes "%" and two hex digits. Escaping never puts anything else after a
// "%", so "%" followed by anything else marks structure: a directory ends in
// "%", an empty last segment is "%_", and hashedDir and the suffixes of the
// files next to an object can't be an object.
//
// Windows doesn't allow some names, so the first letter of a reserved device
// name (before the first dot), a '~' followed by digits there (an 8.3 short
// name), and a trailing '.' or ' ' are escaped too.
//
// A name whose path would be too long, with a segment over maxObjectPart bytes
// or more than maxObjectPath bytes in all, is stored in hashedDir under the
// SHA-256 of the name instead, with the name in a file next to it.

const (
	// hashedDir holds the objects whose names are too long for a path.
	hashedDir = "%h"
	// nameSuffix is the suffix of the file that holds the name of an object in
	// hashedDir.
	nameSuffix = "%n"
	// maxObjectPart leaves room in a 255-byte file name for the metadata
	// file's suffix and for the temporary file renameio writes before renaming
	// it into place: "." and up to 19 digits.
	maxObjectPart = 255 - len(metadataSuffix) - len(".") - 19
	// maxObjectPath keeps an object's path, with the root before it, within
	// macOS's 1024-byte limit on paths.
	maxObjectPath = 512
)

// unescapedPunctuation is the punctuation an object path keeps as it is.
const unescapedPunctuation = " #$&'()+,-.;=@[]^_`{}~"

var reservedWindowsNames = map[string]bool{"con": true, "prn": true, "aux": true, "nul": true, "conin$": true, "conout$": true}

func init() {
	for i := 1; i <= 9; i++ {
		reservedWindowsNames["com"+strconv.Itoa(i)] = true
		reservedWindowsNames["lpt"+strconv.Itoa(i)] = true
	}
}

// objectRelPath returns the path of an object relative to its bucket
// directory, and whether the path is in hashedDir.
func objectRelPath(name string) (rel string, hashed bool) {
	segments := strings.Split(name, "/")
	parts := make([]string, len(segments))
	for i, segment := range segments {
		var b strings.Builder
		for j := 0; j < len(segment); j++ {
			b.WriteString(escapeObjectByte(segment[j]))
		}
		switch part := windowsSafe(b.String()); {
		case i < len(segments)-1:
			parts[i] = part + "%"
		case part == "":
			parts[i] = "%_"
		default:
			parts[i] = part
		}
		if len(parts[i]) > maxObjectPart {
			return hashedRelPath(name), true
		}
	}
	rel = filepath.Join(parts...)
	if len(rel) > maxObjectPath {
		return hashedRelPath(name), true
	}
	return rel, false
}

func hashedRelPath(name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(hashedDir, hex.EncodeToString(sum[:]))
}

func escapeObjectByte(b byte) string {
	switch {
	case 'a' <= b && b <= 'z' || '0' <= b && b <= '9' || strings.IndexByte(unescapedPunctuation, b) >= 0:
		return string(b)
	case 'A' <= b && b <= 'Z':
		return "!" + string(b+'a'-'A')
	default:
		return fmt.Sprintf("%%%02x", b)
	}
}

func windowsSafe(part string) string {
	short, _, _ := strings.Cut(part, ".")
	if reservedWindowsNames[short] {
		part = fmt.Sprintf("%%%02x", part[0]) + part[1:]
	} else if i := strings.LastIndexByte(short, '~'); i >= 0 && i < len(short)-1 && strings.Trim(short[i+1:], "0123456789") == "" {
		part = part[:i] + "%7e" + part[i+1:]
	}
	if n := len(part); n > 0 && (part[n-1] == '.' || part[n-1] == ' ') {
		part = part[:n-1] + fmt.Sprintf("%%%02x", part[n-1])
	}
	return part
}

// objectNameFromRelPath returns the name of the object at rel, a path outside
// hashedDir that objectRelPath returned. It reports false for any other path.
func objectNameFromRelPath(rel string) (string, bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	var name strings.Builder
	for i, part := range parts {
		last := i == len(parts)-1
		switch {
		case !last && strings.HasSuffix(part, "%"):
			part = strings.TrimSuffix(part, "%")
		case last && part == "%_":
			part = ""
		case last:
		default:
			return "", false
		}
		segment, ok := unescapeObjectPart(part)
		if !ok {
			return "", false
		}
		name.WriteString(segment)
		if !last {
			name.WriteByte('/')
		}
	}
	// Reject other spellings of the name, like an escape objectRelPath doesn't
	// use, so each object has one path.
	if r, hashed := objectRelPath(name.String()); hashed || r != filepath.FromSlash(rel) {
		return "", false
	}
	return name.String(), true
}

func unescapeObjectPart(part string) (string, bool) {
	var b []byte
	for i := 0; i < len(part); i++ {
		switch c := part[i]; {
		case c == '!' && i+1 < len(part) && 'a' <= part[i+1] && part[i+1] <= 'z':
			b = append(b, part[i+1]-'a'+'A')
			i++
		case c == '%' && i+2 < len(part) && isLowerHex(part[i+1]) && isLowerHex(part[i+2]):
			v, _ := strconv.ParseUint(part[i+1:i+3], 16, 8)
			b = append(b, byte(v))
			i += 2
		case 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte(unescapedPunctuation, c) >= 0:
			b = append(b, c)
		default:
			return "", false
		}
	}
	return string(b), true
}

func isLowerHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f'
}
