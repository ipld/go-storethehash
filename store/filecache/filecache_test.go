package filecache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	var (
		evictedCount int
		evictedName  string
		evictedRefs  int
	)

	fc := NewOpenFile(2, os.O_CREATE|os.O_RDWR, 0777)
	fc.SetOnEvicted(func(file *os.File, refs int) {
		t.Logf("Removed %q from cache", filepath.Base(file.Name()))
		evictedCount++
		evictedName = file.Name()
		evictedRefs = refs
	})

	tmp := t.TempDir()
	fooName := filepath.Join(tmp, "foo")
	barName := filepath.Join(tmp, "bar")
	bazName := filepath.Join(tmp, "baz")

	fooFile, err := fc.Open(fooName)
	if err != nil {
		t.Fatal(err)
	}

	barFile, err := fc.Open(barName)
	if err != nil {
		t.Fatal(err)
	}

	fooFile, err = fc.Open(fooName)
	if err != nil {
		t.Fatal(err)
	}

	if evictedCount != 0 {
		t.Fatal("should not have evicted file")
	}

	bazFile, err := fc.Open(bazName)
	if err != nil {
		t.Fatal(err)
	}

	if evictedCount != 1 {
		t.Fatal("should have evicted 1 file")
	}
	if evictedName != barName {
		t.Fatalf("expected %s to be evicted, got %s", barName, evictedName)
	}
	if evictedRefs != 1 {
		t.Fatalf("expected file to have ref count of 1, got %d", evictedRefs)
	}

	err = fc.Close(barFile)
	if err != nil {
		t.Fatal(err)
	}

	barFile, err = fc.Open(barName)
	if err != nil {
		t.Fatal(err)
	}

	if evictedCount != 2 {
		t.Fatal("should have evicted 1 file")
	}
	if evictedName != fooName {
		t.Fatalf("expected %s to be evicted, got %s", fooName, evictedName)
	}
	if evictedRefs != 2 {
		t.Fatalf("expected file to have ref count of 1, got %d", evictedRefs)
	}

	if err = fc.Close(fooFile); err != nil {
		t.Fatal(err)
	}
	if err = fc.Close(fooFile); err != nil {
		t.Fatal(err)
	}
	if err = fc.Close(fooFile); err == nil {
		t.Fatal("did not get expected err")
	}

	if err = fc.Remove(bazName); err != nil {
		t.Fatal(err)
	}
	if evictedCount != 3 {
		t.Fatal("should have evicted 1 file")
	}
	if evictedName != bazName {
		t.Fatalf("expected %s to be evicted, got %s", fooName, evictedName)
	}
	if evictedRefs != 1 {
		t.Fatalf("expected file to have ref count of 1, got %d", evictedRefs)
	}
	if err = fc.Close(bazFile); err != nil {
		t.Fatal(err)
	}
	err = fc.Close(bazFile)
	if err != ErrAlreadyClosed {
		t.Fatal("expected error:", ErrAlreadyClosed)
	}
}
