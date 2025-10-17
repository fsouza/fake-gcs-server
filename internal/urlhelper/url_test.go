package urlhelper

import (
	"net/http"
	"net/url"
	"testing"
)

func TestGetScheme(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "default",
			args: args{
				r: &http.Request{
					URL:    &url.URL{},
					Header: http.Header{},
				},
			},
			want: "http",
		},
		{
			name: "http_via_url",
			args: args{
				r: &http.Request{
					URL: &url.URL{
						Scheme: "http",
					},
					Header: http.Header{},
				},
			},
			want: "http",
		},
		{
			name: "https_via_url",
			args: args{
				r: &http.Request{
					URL: &url.URL{
						Scheme: "https",
					},
					Header: http.Header{},
				},
			},
			want: "https",
		},
		{
			name: "http_via_header",
			args: args{
				r: &http.Request{
					URL: &url.URL{},
					Header: func() http.Header {
						header := http.Header{}
						header.Set("X-Forwarded-Proto", "http")
						return header
					}(),
				},
			},
			want: "http",
		},
		{
			name: "https_via_header",
			args: args{
				r: &http.Request{
					URL: &url.URL{},
					Header: func() http.Header {
						header := http.Header{}
						header.Set("X-Forwarded-Proto", "https")
						return header
					}(),
				},
			},
			want: "https",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getScheme(tt.args.r); got != tt.want {
				t.Errorf("GetScheme() = %v, want %v", got, tt.want)
			}
		})
	}
}
