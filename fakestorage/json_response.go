package fakestorage

import (
	"encoding/json"
	"net/http"
)

type jsonResponse struct {
	status int
	header http.Header
	data   interface{}
	err    error
}

type jsonHandler = func(r *http.Request) jsonResponse

func jsonToHTTPHandler(h jsonHandler) http.HandlerFunc {
	const (
		defaultSuccessStatus = http.StatusOK
		defaultErrorStatus   = http.StatusInternalServerError
	)
	return func(w http.ResponseWriter, r *http.Request) {
		resp := h(r)
		w.Header().Set("Content-Type", "application/json")
		for name, values := range resp.header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		status := resp.status
		var data interface{}
		if resp.err != nil {
			if status == 0 {
				status = defaultErrorStatus
			}
			data = newErrorResponse(status, resp.err.Error(), nil)
		} else {
			if status == 0 {
				status = defaultSuccessStatus
			}
			data = resp.data
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(data)
	}
}
