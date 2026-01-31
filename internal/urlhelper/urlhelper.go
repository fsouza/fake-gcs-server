package urlhelper

import (
	"fmt"
	"net/http"
	"strings"
)

// GetBaseURL returns the base URL (scheme + host) for the request,
// using X-Forwarded-Proto and X-Forwarded-Host when present (e.g. behind a proxy).
// Returns an empty string if the host cannot be determined.
func GetBaseURL(r *http.Request) string {
	scheme := "https"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = strings.ToLower(strings.TrimSpace(proto))
	} else if r.TLS == nil {
		scheme = "http"
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	return fmt.Sprintf("%s://%s", scheme, host)
}
