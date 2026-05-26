package deltaflow

import (
	"encoding/json"
	"testing"
)

func TestProjectionKeyMarshalJSONPreservesLargeNumericLiterals(t *testing.T) {
	key := ProjectionKey{
		"id":     json.RawMessage(`12345678901234567890`),
		"nested": json.RawMessage(`{"ids":[12345678901234567890,1.2300]}`),
	}

	got, err := json.Marshal(key)
	if err != nil {
		t.Fatalf("MarshalJSON returned error: %v", err)
	}

	want := `{"id":12345678901234567890,"nested":{"ids":[12345678901234567890,1.2300]}}`
	if string(got) != want {
		t.Fatalf("MarshalJSON mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}
