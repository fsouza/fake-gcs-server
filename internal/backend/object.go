// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"

	"cloud.google.com/go/storage"
)

// Object represents the object that is stored within the fake server.
type Object struct {
	BucketName      string `json:"-"`
	Name            string `json:"-"`
	ContentType     string
	ContentEncoding string
	Content         []byte
	Crc32c          string
	Md5Hash         string
	ACL             []storage.ACLRule
	Metadata        map[string]string
	Created         string
	Deleted         string
	Updated         string
	Generation      int64
}

// ID is useful for comparing objects
func (o *Object) ID() string {
	return fmt.Sprintf("%s#%d", o.IDNoGen(), o.Generation)
}

// IDNoGen does not consider the generation field
func (o *Object) IDNoGen() string {
	return fmt.Sprintf("%s/%s", o.BucketName, o.Name)
}

// compare also bears in mind most of the attributes, but generation if one is 0 and timestamp fields
func (o *Object) compare(o2 Object) error {
	if o.BucketName != o2.BucketName {
		return fmt.Errorf("bucket name differs:\nmain %q\narg  %q", o.BucketName, o2.BucketName)
	}
	if o.Name != o2.Name {
		return fmt.Errorf("wrong object name:\nmain %q\narg  %q", o.Name, o2.Name)
	}
	if o.ContentType != o2.ContentType {
		return fmt.Errorf("wrong object contenttype:\nmain %q\narg  %q", o.ContentType, o2.ContentType)
	}
	if o.Crc32c != o2.Crc32c {
		return fmt.Errorf("wrong crc:\nmain %q\narg  %q", o.Crc32c, o2.Crc32c)
	}
	if o.Md5Hash != o2.Md5Hash {
		return fmt.Errorf("wrong md5:\nmain %q\narg  %q", o.Md5Hash, o2.Md5Hash)
	}
	if o.Generation != 0 && o2.Generation != 0 && o.Generation != o2.Generation {
		return fmt.Errorf("generations different from 0, but not equal:\nmain %q\narg  %q", o.Generation, o2.Generation)
	}
	if !bytes.Equal(o.Content, o2.Content) {
		return fmt.Errorf("wrong object content:\nmain %q\narg  %q", o.Content, o2.Content)
	}
	return nil
}
