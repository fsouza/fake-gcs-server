package fakestorage

import (
	"encoding/json"
	"net/http"
)

func (s *Server) updateServerConfig(r *http.Request) jsonResponse {

	var configOptions struct {
		ExternalUrl string `json:"externalUrl,omitempty"`
	}
	err := json.NewDecoder(r.Body).Decode(&configOptions)
	if err != nil {
		return jsonResponse{
			status:       http.StatusBadRequest,
			errorMessage: "Update server config payload can not be parsed.",
		}
	}

	if configOptions.ExternalUrl != "" {
		s.externalURL = configOptions.ExternalUrl
	}

	return jsonResponse{status: http.StatusOK}
}
