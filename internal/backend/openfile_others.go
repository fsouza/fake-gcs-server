// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !windows

package backend

import "os"

func openFile(name string) (*os.File, error) {
	return os.Open(name)
}
