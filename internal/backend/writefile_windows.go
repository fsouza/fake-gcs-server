// Copyright 2022 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Like renameio.WriteFile on other systems, writeFile renames a temporary file
// over filename, so an open reader keeps the old content.
func writeFile(filename string, data []byte, perm os.FileMode) error {
	return replaceFile(filename, bytes.NewReader(data), perm)
}

func replaceFile(filename string, r io.Reader, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(filename), "."+filepath.Base(filename)+"*")
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(f.Name(), perm)
	}
	if err == nil {
		err = rename(f.Name(), filename)
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

// os.Rename uses MoveFileEx, which can't replace an open file. POSIX semantics
// can, if every open handle shares the file for deletion, as openFile does.
// Filesystems without them, such as FAT, fall back to os.Rename.
func rename(from, to string) error {
	err := posixRename(from, to)
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) ||
		errors.Is(err, windows.ERROR_INVALID_FUNCTION) ||
		errors.Is(err, windows.ERROR_NOT_SUPPORTED) {
		return os.Rename(from, to)
	}
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	return nil
}

// fileRenameInfo is FILE_RENAME_INFO, with Flags in place of ReplaceIfExists
// as FileRenameInfoEx takes it. FileName runs past the end of the struct.
type fileRenameInfo struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

const (
	fileRenameFlagReplaceIfExists = 0x1 // FILE_RENAME_FLAG_REPLACE_IF_EXISTS
	fileRenameFlagPosixSemantics  = 0x2 // FILE_RENAME_FLAG_POSIX_SEMANTICS
)

func posixRename(from, to string) error {
	fromPath, err := extendedPath(from)
	if err != nil {
		return err
	}
	fromName, err := windows.UTF16PtrFromString(fromPath)
	if err != nil {
		return err
	}
	toPath, err := extendedPath(to)
	if err != nil {
		return err
	}
	toName, err := windows.UTF16FromString(toPath) // ends with a NUL
	if err != nil {
		return err
	}

	h, err := windows.CreateFile(fromName, windows.DELETE|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)

	size := unsafe.Offsetof(fileRenameInfo{}.FileName) + uintptr(len(toName))*2
	size = max(size, unsafe.Sizeof(fileRenameInfo{}))
	buf := make([]uint64, (size+7)/8) // uint64 keeps the struct aligned
	info := (*fileRenameInfo)(unsafe.Pointer(&buf[0]))
	info.Flags = fileRenameFlagReplaceIfExists | fileRenameFlagPosixSemantics
	info.FileNameLength = uint32(len(toName)-1) * 2 // in bytes, without the NUL
	copy(unsafe.Slice(&info.FileName[0], len(toName)), toName)
	return windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, (*byte)(unsafe.Pointer(&buf[0])), uint32(size))
}
