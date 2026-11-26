// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Unlike os.Open, openFile shares the file for deletion, so a new version of
// the object can be renamed over it while it is being read.
func openFile(name string) (*os.File, error) {
	path, err := extendedPath(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return os.NewFile(uintptr(h), name), nil
}

// extendedPath adds the \\?\ prefix, which lifts the 260-character limit on
// paths, as os.Open does.
func extendedPath(name string) (string, error) {
	path, err := filepath.Abs(name)
	if err != nil || strings.HasPrefix(path, `\\`) {
		return path, err
	}
	return `\\?\` + path, nil
}
