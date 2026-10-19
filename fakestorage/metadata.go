package fakestorage

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// customMetadata is the custom metadata of an object as it appears in request
// bodies. Cloud Storage accepts any JSON scalar for a value (clients commonly
// send numbers and booleans) and stores its string form, so decoding is lenient
// where a plain map[string]string would reject the request.
type customMetadata map[string]string

func (m *customMetadata) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == nil { // JSON null
		*m = nil
		return nil
	}
	result := make(customMetadata, len(raw))
	for key, value := range raw {
		value = bytes.TrimSpace(value)
		switch value[0] {
		case '"':
			var s string
			if err := json.Unmarshal(value, &s); err != nil {
				return err
			}
			result[key] = s
		case 'n': // null: same as an empty string, as with map[string]string
			result[key] = ""
		case 't', 'f': // true, false
			result[key] = string(value)
		case '{', '[':
			return fmt.Errorf("metadata value for %q must be a string, number or boolean", key)
		default: // number: keep the literal as sent, e.g. 612
			result[key] = string(value)
		}
	}
	*m = result
	return nil
}
