package task

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// processTag keeps ids minted by different processes apart. Two API instances
// share one PostgreSQL table, so each of them used to format ids as
// "task-<nanosecond>-<sequence>" with its own counter: submitting on both
// instances inside the same clock tick produced the same id, and the second
// insert died on the primary key with a misleading conflict error. Four random
// bytes make that collision negligible while the timestamp prefix keeps ids
// sortable.
var processTag = newProcessTag()

// idCounter disambiguates ids created within the same clock tick. A nanosecond
// timestamp alone collides under concurrent submission: two callers read the
// same time.Now() and the second task is rejected as a duplicate.
var idCounter uint64

func newProcessTag() string {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand does not fail in practice. If it ever does, fall back to
		// the clock so ids stay unique within this process.
		return fmt.Sprintf("%08x", uint32(time.Now().UnixNano()))
	}
	return hex.EncodeToString(buf)
}

// NewID returns an identifier for a task the caller did not name. The
// zero-padded counter keeps ids created in the same nanosecond
// lexicographically ordered, so the timestamp prefix still sorts.
func NewID() string {
	return idFrom(processTag, time.Now().UnixNano(), atomic.AddUint64(&idCounter, 1))
}

func idFrom(tag string, nanos int64, sequence uint64) string {
	return fmt.Sprintf("task-%d-%s-%06d", nanos, tag, sequence%1_000_000)
}
