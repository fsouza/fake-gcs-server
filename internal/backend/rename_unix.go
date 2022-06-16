//go:build !windows
// +build !windows

package backend

// This file contains open and rename functions for Windows compatibility. See
// rename_windoes.go for details.

import "os"

func compatOpenAllowingRename(path string) (*os.File, error) {
	return os.Open(path)
}

func compatOpenForWritingAllowingRename(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func compatRename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
