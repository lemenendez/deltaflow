package connectors

import "testing"

type stringAliasError string

func (e stringAliasError) Error() string { return string(e) }

func TestIsUniqueViolationHandlesNonStructErrorTypes(t *testing.T) {
	if IsUniqueViolation(stringAliasError("something else")) {
		t.Fatal("IsUniqueViolation returned true for a string alias error without a matching unique-violation signal")
	}
}
