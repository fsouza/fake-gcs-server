// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package spill provides a buffer that keeps its content in memory up to a
// limit and in a temporary file beyond it.
package spill

import (
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
)

// DefaultLimit is the size up to which a Buffer keeps its content in memory.
const DefaultLimit = 1 << 20

// Buffer holds content written at any offset up to its current size.
// Its zero value isn't usable; create one with New.
type Buffer struct {
	dir   string
	limit int64
	mem   []byte
	file  *os.File
	size  int64
	taken string // the file's name after TakeFile
}

// New returns an empty Buffer that moves its content to a temporary file in
// dir (os.TempDir() if empty) once it grows past limit bytes.
func New(dir string, limit int64) *Buffer {
	return &Buffer{dir: dir, limit: limit}
}

// Size returns the size of the content.
func (b *Buffer) Size() int64 { return b.size }

// WriteAt writes p at off, which must not be past the end of the content, and
// extends the content if p reaches past its end.
func (b *Buffer) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > b.size {
		return 0, errors.New("spill: write past the end of the content")
	}
	end := off + int64(len(p))
	if b.file == nil && end > b.limit {
		if err := b.spill(); err != nil {
			return 0, err
		}
	}
	var n int
	if b.file != nil {
		var err error
		if n, err = b.file.WriteAt(p, off); err != nil {
			b.size = max(b.size, off+int64(n))
			return n, err
		}
	} else {
		if end > int64(len(b.mem)) {
			// off <= len(b.mem), so copy fills every byte added here.
			b.mem = slices.Grow(b.mem, int(end)-len(b.mem))[:end]
		}
		n = copy(b.mem[off:], p)
	}
	b.size = max(b.size, end)
	return n, nil
}

// ReadFrom appends everything read from r to the content.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(io.NewOffsetWriter(b, b.size), r)
}

// Truncate shortens the content to size bytes; it does nothing if the content
// is already that short.
func (b *Buffer) Truncate(size int64) error {
	if size < 0 || size >= b.size {
		return nil
	}
	if b.file != nil {
		if err := b.file.Truncate(size); err != nil {
			return err
		}
	} else {
		b.mem = b.mem[:size]
	}
	b.size = size
	return nil
}

// Reader returns a reader over the content. It is valid until the next write
// or Close.
func (b *Buffer) Reader() io.ReadSeeker {
	if b.file != nil {
		return io.NewSectionReader(b.file, 0, b.size)
	}
	return bytes.NewReader(b.mem[:b.size])
}

// Bytes returns the content if it is in memory, and false if it is in a file.
func (b *Buffer) Bytes() ([]byte, bool) {
	if b.file != nil || b.taken != "" {
		return nil, false
	}
	return b.mem[:b.size], true
}

// TakeFile closes the temporary file that holds the content and returns its
// name, for the caller to rename away. Close removes the file if it is still
// there. TakeFile returns "" if the content is in memory. The Buffer can't be
// used after TakeFile, other than to Close it.
func (b *Buffer) TakeFile() (string, error) {
	if b.file == nil {
		return "", nil
	}
	b.taken = b.file.Name()
	err := b.file.Close()
	b.file = nil
	return b.taken, err
}

// Close releases the content and removes the temporary file, if there is one.
func (b *Buffer) Close() error {
	b.mem = nil
	b.size = 0
	name := b.taken
	var err error
	if b.file != nil {
		name = b.file.Name()
		err = b.file.Close()
		b.file = nil
	}
	if name == "" {
		return err
	}
	if rmErr := os.Remove(name); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) && err == nil {
		err = rmErr
	}
	return err
}

func (b *Buffer) spill() error {
	file, err := os.CreateTemp(b.dir, "spill-*")
	if err != nil {
		return err
	}
	if _, err := file.Write(b.mem[:b.size]); err != nil {
		file.Close()
		os.Remove(file.Name())
		return err
	}
	b.file = file
	b.mem = nil
	return nil
}
