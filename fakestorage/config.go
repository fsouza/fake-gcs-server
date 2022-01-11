package fakestorage

import (
	"io/ioutil"
	"net/http"
)

func (s *Server) updateServerConfig(w http.ResponseWriter, r *http.Request) {

	body := r.Body
	defer body.Close()

	if body != nil {
		bodyBytes, _ := ioutil.ReadAll(body)
		s.externalURL = string(bodyBytes[:])
	}

	return
}
