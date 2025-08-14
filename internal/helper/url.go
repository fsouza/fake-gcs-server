package helper

import (
	"fmt"
	"net/http"
)

func GetScheme(r *http.Request) string {
	// Check for the X-Forwarded-Proto header, common in production environments
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}

	// Fall back to r.URL.Scheme, which is set for TLS connections
	if r.URL.Scheme != "" {
		return r.URL.Scheme
	}

	// Default to "http" for standard non-TLS connections
	return "http"
}

func GetBaseURL(r *http.Request) string {
	scheme := GetScheme(r)
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}
