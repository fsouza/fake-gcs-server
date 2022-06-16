package backend

// This file contains open and rename functions for Windows compatibility. See
// rename_windoes.go for details.

import "os"

func compatOpenAllowingRename(path string) (*os.File, error) {
	return os.Open(path)
}

func compatRename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
