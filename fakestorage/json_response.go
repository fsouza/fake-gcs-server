package fakestorage

import (
	"encoding/json"
	"net/http"
)

type jsonResponse struct {
	status       int
	header       http.Header
	data         interface{}
	errorMessage string
}

type jsonHandler = func(r *http.Request) jsonResponse

func jsonToHTTPHandler(h jsonHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := h(r)
		w.Header().Set("Content-Type", "application/json")
		for name, values := range resp.header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}

		status := resp.getStatus()
		var data interface{}
		if status > 399 {
			data = newErrorResponse(status, resp.getErrorMessage(status), nil)
		} else {
			data = resp.data
		}

		w.WriteHeader(status)
		json.NewEncoder(w).Encode(data)
	}
}

func (r *jsonResponse) getStatus() int {
	if r.status > 0 {
		return r.status
	}
	if r.errorMessage != "" {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

func (r *jsonResponse) getErrorMessage(status int) string {
	if r.errorMessage != "" {
		return r.errorMessage
	}
	return http.StatusText(status)
}
