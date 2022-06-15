// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !windows
// +build !windows

package backend

import (
	"github.com/pkg/xattr"
)

const xattrKey = "user.metadata"

func writeXattr(path string, encoded []byte) error {
	return xattr.Set(path, xattrKey, encoded)
}

func readXattr(path string) ([]byte, error) {
	return xattr.Get(path, xattrKey)
}

func isXattrFile(path string) bool {
	return false
}

func removeXattrFile(path string) error {
	return nil
}

func renameXAttrFile(pathSrc, pathDst string) error {
	return nil
}
