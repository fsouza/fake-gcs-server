// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/internal/checksum"
)

// The behaviours below were checked against Cloud Storage itself, with a
// browser and with curl, on resumable sessions initiated through the JSON API.

type uploadSession struct {
	t      *testing.T
	server *Server
	url    string
}

// openSession initiates a resumable upload of name, declaring size in
// X-Upload-Content-Length when it is not negative, and metadata as the body.
func openSession(t *testing.T, server *Server, name string, size int, metadata string) uploadSession {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		server.URL()+"/upload/storage/v1/b/session-bucket/o?uploadType=resumable&name="+name,
		strings.NewReader(metadata))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if size >= 0 {
		req.Header.Set("X-Upload-Content-Length", fmt.Sprint(size))
	}
	resp, err := server.HTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initiating: want %d, got %d", http.StatusOK, resp.StatusCode)
	}
	return uploadSession{t: t, server: server, url: resp.Header.Get("Location")}
}

// put sends body with contentRange, none when empty, and answers the status
// and the Range header.
func (u uploadSession) put(contentRange string, body []byte) (int, string) {
	u.t.Helper()
	req, err := http.NewRequest(http.MethodPut, u.url, bytes.NewReader(body))
	if err != nil {
		u.t.Fatal(err)
	}
	if contentRange != "" {
		req.Header.Set("Content-Range", contentRange)
	}
	resp, err := u.server.HTTPClient().Do(req)
	if err != nil {
		u.t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("Range")
}

func (u uploadSession) want(what string, wantStatus int, wantRange string, status int, gotRange string) {
	u.t.Helper()
	if status != wantStatus || gotRange != wantRange {
		u.t.Errorf("%s: want %d %q, got %d %q", what, wantStatus, wantRange, status, gotRange)
	}
}

func TestResumableUploadSession(t *testing.T) {
	runServersTest(t, runServersOptions{}, func(t *testing.T, server *Server) {
		server.CreateBucketWithOpts(CreateBucketOpts{Name: "session-bucket"})
		content := bytes.Repeat([]byte("0123456789abcdef"), 64) // 1024 bytes
		half := len(content) / 2

		exists := func(name string) bool {
			_, err := server.GetObject("session-bucket", name)
			return err == nil
		}

		t.Run("a status query answers what is held and keeps the session", func(t *testing.T) {
			u := openSession(t, server, "query", len(content), "{}")
			status, got := u.put(fmt.Sprintf("bytes */%d", len(content)), nil)
			u.want("query before any chunk", http.StatusPermanentRedirect, "", status, got)

			status, got = u.put(fmt.Sprintf("bytes 0-%d/%d", half-1, len(content)), content[:half])
			u.want("first chunk", http.StatusPermanentRedirect, fmt.Sprintf("bytes=0-%d", half-1), status, got)

			status, got = u.put(fmt.Sprintf("bytes */%d", len(content)), nil)
			u.want("query after the first chunk", http.StatusPermanentRedirect, fmt.Sprintf("bytes=0-%d", half-1), status, got)
			if exists("query") {
				t.Fatal("a status query finished the object")
			}

			status, _ = u.put(fmt.Sprintf("bytes %d-%d/%d", half, len(content)-1, len(content)), content[half:])
			if status != http.StatusOK {
				t.Fatalf("last chunk: want %d, got %d", http.StatusOK, status)
			}
			obj, err := server.GetObject("session-bucket", "query")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(obj.Content, content) {
				t.Errorf("content: want %d bytes, got %d", len(content), len(obj.Content))
			}
		})

		t.Run("a streaming upload still ends with bytes */total once all is sent", func(t *testing.T) {
			u := openSession(t, server, "stream", -1, "{}")
			status, _ := u.put(fmt.Sprintf("bytes 0-%d/*", len(content)-1), content)
			if status != http.StatusPermanentRedirect {
				t.Fatalf("chunk: want %d, got %d", http.StatusPermanentRedirect, status)
			}
			status, _ = u.put(fmt.Sprintf("bytes */%d", len(content)), nil)
			if status != http.StatusOK || !exists("stream") {
				t.Fatalf("end of stream: want %d and the object, got %d", http.StatusOK, status)
			}
		})

		t.Run("a body shorter than its range is refused, not credited", func(t *testing.T) {
			u := openSession(t, server, "short", len(content), "{}")
			status, _ := u.put(fmt.Sprintf("bytes 0-%d/%d", half-1, len(content)), content[:half/2])
			if status != http.StatusBadRequest {
				t.Fatalf("short chunk: want %d, got %d", http.StatusBadRequest, status)
			}
			status, got := u.put(fmt.Sprintf("bytes */%d", len(content)), nil)
			u.want("query after the refused chunk", http.StatusPermanentRedirect, "", status, got)
		})

		t.Run("a size other than X-Upload-Content-Length is refused", func(t *testing.T) {
			u := openSession(t, server, "longer", 100, "{}")
			status, _ := u.put("", content[:200])
			if status != http.StatusBadRequest || exists("longer") {
				t.Errorf("200 bytes declared as 100, in one request: want %d and no object, got %d", http.StatusBadRequest, status)
			}

			u = openSession(t, server, "shorter", 300, "{}")
			status, _ = u.put("bytes 0-199/200", content[:200])
			if status != http.StatusBadRequest || exists("shorter") {
				t.Errorf("a total of 200 declared as 300: want %d and no object, got %d", http.StatusBadRequest, status)
			}
		})

		t.Run("a declared checksum outlives the chunks before the last", func(t *testing.T) {
			metadata := `{"crc32c": "AAAAAA=="}`
			u := openSession(t, server, "crc-wrong", len(content), metadata)
			u.put(fmt.Sprintf("bytes 0-%d/%d", half-1, len(content)), content[:half])
			status, _ := u.put(fmt.Sprintf("bytes %d-%d/%d", half, len(content)-1, len(content)), content[half:])
			if status != http.StatusBadRequest || exists("crc-wrong") {
				t.Errorf("wrong crc32c over two chunks: want %d and no object, got %d", http.StatusBadRequest, status)
			}

			metadata = fmt.Sprintf(`{"crc32c": %q}`, checksum.EncodedCrc32cChecksum(content))
			u = openSession(t, server, "crc-right", len(content), metadata)
			u.put(fmt.Sprintf("bytes 0-%d/%d", half-1, len(content)), content[:half])
			status, _ = u.put(fmt.Sprintf("bytes %d-%d/%d", half, len(content)-1, len(content)), content[half:])
			if status != http.StatusOK || !exists("crc-right") {
				t.Errorf("right crc32c over two chunks: want %d and the object, got %d", http.StatusOK, status)
			}
		})

		t.Run("a browser can read Range", func(t *testing.T) {
			u := openSession(t, server, "cors", len(content), "{}")
			req, err := http.NewRequest(http.MethodPut, u.url, bytes.NewReader(content[:half]))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Origin", "http://app.example")
			req.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", half-1, len(content)))
			resp, err := server.HTTPClient().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			exposed := resp.Header.Get("Access-Control-Expose-Headers")
			if !strings.Contains(exposed, "Range") {
				t.Errorf("Access-Control-Expose-Headers: want Range in it, got %q", exposed)
			}
		})
	})
}
