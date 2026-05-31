package postgres

import "testing"

type fakeUniqueErr struct {
	msg string
}

func (e fakeUniqueErr) Error() string {
	return e.msg
}

func (e fakeUniqueErr) SQLState() string {
	return "23505"
}

func TestIsOutboxDeltaMappedViolationRequiresUniqueSignal(t *testing.T) {
	var err error = fakeUniqueErr{msg: "pq: duplicate key value violates unique constraint \"deltaflow_sync_jobs_outbox_delta_unique\""}
	if !isOutboxDeltaMappedViolation(err) {
		t.Fatal("isOutboxDeltaMappedViolation returned false for unique violation on outbox index")
	}

	err = fakeUniqueErr{msg: "pq: duplicate key value violates unique constraint \"some_other_unique_index\""}
	if isOutboxDeltaMappedViolation(err) {
		t.Fatal("isOutboxDeltaMappedViolation returned true for different unique index")
	}

	err = stringAliasError("deltaflow_sync_jobs_outbox_delta_unique")
	if isOutboxDeltaMappedViolation(err) {
		t.Fatal("isOutboxDeltaMappedViolation returned true for name-only message without unique violation signal")
	}
}

type stringAliasError string

func (e stringAliasError) Error() string { return string(e) }
