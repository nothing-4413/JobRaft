package fileutil

import (
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestReplaceExistingFile(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "state.json")
	src := dst + ".tmp"
	if err := ioutil.WriteFile(dst, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(src, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Replace(src, dst); err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadFile(dst)
	if err != nil || string(b) != "new" {
		t.Fatalf("content=%q err=%v", b, err)
	}
}
