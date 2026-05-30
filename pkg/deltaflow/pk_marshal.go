package deltaflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

func (k ProjectionKey) MarshalJSON() ([]byte, error) {
	canonical := make(map[string]any, len(k))
	for key, raw := range k {
		if raw == nil {
			canonical[key] = nil
			continue
		}

		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()

		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			if err == nil {
				return nil, errors.New("invalid JSON: multiple top-level values")
			}
			return nil, err
		}
		canonical[key] = value
	}

	return json.Marshal(canonical)
}
