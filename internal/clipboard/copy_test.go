package clipboard

import (
	"errors"
	"testing"
)

func TestCopyWritesClipboardText(t *testing.T) {
	original := writeAll
	t.Cleanup(func() {
		writeAll = original
	})

	var got string
	writeAll = func(text string) error {
		got = text
		return nil
	}

	if err := Copy("hello"); err != nil {
		t.Fatalf("Copy returned error: %v", err)
	}
	if got != "hello" {
		t.Fatalf("clipboard text = %q, want %q", got, "hello")
	}
}

func TestCopyWrapsClipboardError(t *testing.T) {
	original := writeAll
	t.Cleanup(func() {
		writeAll = original
	})

	want := errors.New("clipboard unavailable")
	writeAll = func(string) error {
		return want
	}

	err := Copy("hello")
	if err == nil {
		t.Fatal("Copy returned nil error")
	}
	if !errors.Is(err, want) {
		t.Fatalf("Copy error = %v, want wrapped %v", err, want)
	}
}
