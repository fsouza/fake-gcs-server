package fakestorage

import (
	"io/ioutil"
	"net/http"
)

func (s *Server) updateServerConfig(_ http.ResponseWriter, r *http.Request) {

	body := r.Body
	defer body.Close()
	bodyBytes, _ := ioutil.ReadAll(body)

	s.externalURL = string(bodyBytes[:])
}
