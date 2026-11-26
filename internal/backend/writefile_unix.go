// Copyright 2022 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !windows

package backend

import (
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/google/renameio/v2"
)

func writeFile(filename string, data []byte, perm os.FileMode) error {
	return renameio.WriteFile(filename, data, perm)
}

func replaceFile(filename string, r io.Reader, perm os.FileMode) error {
	f, err := renameio.NewPendingFile(filename, renameio.WithPermissions(perm))
	if err != nil {
		return err
	}
	defer f.Cleanup()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	return f.CloseAtomicallyReplace()
}

func rename(from, to string) error {
	return os.Rename(from, to)
}

func isCrossDevice(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}
