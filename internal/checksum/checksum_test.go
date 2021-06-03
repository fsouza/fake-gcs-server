// Copyright 2021 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package checksum

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func TestEncodedCrc32cChecksum(t *testing.T) {
	var data [32]byte
	_, err := rand.Read(data[:])
	if err != nil {
		t.Fatal(err)
	}

	encoded := EncodedCrc32cChecksum(data[:])
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}

	expected := crc32cChecksum(data[:])
	if !bytes.Equal(decoded, expected) {
		t.Errorf("incorrect value after decoding\nwant %x, got  %x", expected, decoded)
	}
}

func TestEncodedMd5Hash(t *testing.T) {
	var data [32]byte
	_, err := rand.Read(data[:])
	if err != nil {
		t.Fatal(err)
	}

	encoded := EncodedMd5Hash(data[:])
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}

	expected := MD5Hash(data[:])
	if !bytes.Equal(decoded, expected) {
		t.Errorf("incorrect value after decoding\nwant %x, got  %x", expected, decoded)
	}
}
