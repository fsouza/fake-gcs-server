// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TODO: this package works around missing Windows support in xattr:
// https://github.com/pkg/xattr/issues/47

package backend

import (
	"io/ioutil"
	"os"
	"strings"
)

const xattrKey = ".metadata"

func writeXattr(path string, encoded []byte) error {
	return ioutil.WriteFile(path+xattrKey, encoded, 0o600)
}

func readXattr(path string) ([]byte, error) {
	return ioutil.ReadFile(path + xattrKey)
}

func isXattrFile(path string) bool {
	return strings.HasSuffix(path, xattrKey)
}

func removeXattrFile(path string) error {
	return os.Remove(path + xattrKey)
}
