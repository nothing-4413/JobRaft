package task

import (
	"strings"
	"testing"
)

func TestNewIDStaysUniqueUnderRepeatedCalls(t *testing.T) {
	seen := make(map[string]bool, 2000)
	for i := 0; i < 2000; i++ {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate id %q after %d calls", id, i)
		}
		seen[id] = true
		if fields := strings.Split(id, "-"); len(fields) != 4 || fields[0] != "task" {
			t.Fatalf("unexpected id shape %q", id)
		}
	}
}

func TestIDsFromDifferentProcessesDoNotCollide(t *testing.T) {
	// Two instances sharing one store used to mint the same id inside the same
	// clock tick: each had its own counter but no process tag, so the second
	// insert failed on the primary key.
	first := idFrom("aaaaaaaa", 42, 1)
	second := idFrom("bbbbbbbb", 42, 1)
	if first == second {
		t.Fatalf("ids from different processes collided: %q", first)
	}
	if got := idFrom(processTag, 42, 1); !strings.HasPrefix(got, "task-42-"+processTag+"-") {
		t.Fatalf("process tag %q is missing from %q", processTag, got)
	}
}

func TestIDsInOneTickSort(t *testing.T) {
	// The zero-padded counter is what makes ids created inside a single clock
	// tick lexicographically ordered.
	if a, b := idFrom(processTag, 7, 9), idFrom(processTag, 7, 10); !(a < b) {
		t.Fatalf("expected %q to sort before %q", a, b)
	}
}

func TestProcessTagLooksRandom(t *testing.T) {
	if len(processTag) != 8 {
		t.Fatalf("process tag %q should be 8 hex characters", processTag)
	}
	if strings.Trim(processTag, "0123456789abcdef") != "" {
		t.Fatalf("process tag %q is not hexadecimal", processTag)
	}
}
