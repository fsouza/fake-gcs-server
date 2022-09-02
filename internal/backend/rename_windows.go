package backend

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/alexbrainman/goissue34681"
)

// compatOpenAllowingRename opens the file at the given path using the
// FILE_SHARE_DELETE flag, which allows opened files to be renamed and deleted
// on Windows.
func compatOpenAllowingRename(path string) (*os.File, error) {
	return goissue34681.Open(path)
}

func compatOpenForWritingAllowingRename(path string) (*os.File, error) {
	return goissue34681.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

// compatRename renames a file and handles the case where the rename
// destination already exists and might be open (open files must have been
// opened using compatOpenAllowingRename).
//
// See the following for more information:
// https://boostgsoc13.github.io/boost.afio/doc/html/afio/FAQ/deleting_open_files.html
// https://github.com/golang/go/issues/34681
// https://github.com/golang/go/issues/32088
func compatRename(oldpath, newpath string) error {
	_, err := os.Stat(newpath)
	if os.IsExist(err) {
		tempDeletePath := filepath.Join(
			os.TempDir(),
			fmt.Sprintf("%s-overwritten-%d", newpath, time.Now().UnixMilli()))
		// Move the destination to a "temporary delete path" so newpath is
		// freed up before we perform the rename below.
		err = os.Rename(newpath, tempDeletePath)
		if err != nil {
			return fmt.Errorf("count not move destination file during rename, %w", err)
		}
		// Delete the "temporary delete path". If there are any open file
		// descriptors (e.g. for objects currently being read), it's still
		// possible to read the original contents from the file; the file will
		// be removed from the directory sometime after the last descriptor is
		// closed.
		err = os.Remove(tempDeletePath)
		if err != nil {
			return fmt.Errorf("count not delete destination file during rename, %w", err)
		}
	}
	return os.Rename(oldpath, newpath)
}
