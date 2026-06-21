package sqlite

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func newID(prefix string, now time.Time) (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d-%s", prefix, now.UTC().UnixMicro(), hex.EncodeToString(buf)), nil
}

func microsFromTime(t time.Time) int64 {
	return t.UTC().UnixMicro()
}

func timeFromMicros(v int64) time.Time {
	return time.UnixMicro(v).UTC()
}
