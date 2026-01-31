package urlhelper

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestGetBaseURL(t *testing.T) {
	tests := []struct {
		name           string
		host           string
		forwardedProto string
		forwardedHost  string
		tls            bool
		want           string
	}{
		{
			name: "host only, no TLS",
			host: "localhost:4443",
			tls:  false,
			want: "http://localhost:4443",
		},
		{
			name: "host only, with TLS",
			host: "storage.example.com:443",
			tls:  true,
			want: "https://storage.example.com:443",
		},
		{
			name:           "X-Forwarded-Proto and X-Forwarded-Host",
			host:           "internal:8080",
			forwardedProto: "https",
			forwardedHost:  "gcs.example.com:4443",
			tls:            false,
			want:           "https://gcs.example.com:4443",
		},
		{
			name:           "X-Forwarded-Proto only",
			host:           "proxy.local:8080",
			forwardedProto: "http",
			tls:            true,
			want:           "http://proxy.local:8080",
		},
		{
			name:           "X-Forwarded-Host only",
			host:           "public.example.com",
			forwardedHost:  "public.example.com",
			tls:            false,
			want:           "http://public.example.com",
		},
		{
			name:           "forwarded proto trimmed and lowercased",
			host:           "host",
			forwardedProto: "  HTTPS  ",
			tls:            false,
			want:           "https://host",
		},
		{
			name: "empty host returns empty",
			host: "",
			tls:  false,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
			r.Host = tt.host
			if tt.forwardedProto != "" {
				r.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}
			if tt.forwardedHost != "" {
				r.Header.Set("X-Forwarded-Host", tt.forwardedHost)
			}
			if tt.tls {
				r.TLS = &tls.ConnectionState{}
			}
			got := GetBaseURL(r)
			if got != tt.want {
				t.Errorf("GetBaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
