package store

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestFileStoreRecoversTasks(t *testing.T) {
	dir, err := ioutil.TempDir("", "jobraft-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "tasks.json")
	s, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	item := task.Task{ID: "persisted", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(item); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(item.ID)
	if err != nil || got.Name != item.Name {
		t.Fatalf("recovery failed: %+v %v", got, err)
	}
}
