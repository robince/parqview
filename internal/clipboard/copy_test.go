package clipboard

import (
	"errors"
	"testing"
)

func TestCopyWritesClipboardText(t *testing.T) {
	var got string
	writeAll := func(text string) error {
		got = text
		return nil
	}

	if err := copy("hello", writeAll); err != nil {
		t.Fatalf("copy returned error: %v", err)
	}
	if got != "hello" {
		t.Fatalf("clipboard text = %q, want %q", got, "hello")
	}
}

func TestCopyWrapsClipboardError(t *testing.T) {
	want := errors.New("clipboard unavailable")
	writeAll := func(string) error {
		return want
	}

	err := copy("hello", writeAll)
	if err == nil {
		t.Fatal("copy returned nil error")
	}
	if !errors.Is(err, want) {
		t.Fatalf("copy error = %v, want wrapped %v", err, want)
	}
}

func TestCopyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping clipboard integration test in short mode")
	}

	if err := Copy("parqview clipboard integration test"); err != nil {
		t.Skipf("clipboard unavailable: %v", err)
	}
}
