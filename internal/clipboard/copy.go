package clipboard

import (
	"fmt"

	systemclipboard "github.com/atotto/clipboard"
)

// Copy copies text to the system clipboard.
func Copy(text string) error {
	return copy(text, systemclipboard.WriteAll)
}

func copy(text string, writeAll func(string) error) error {
	if err := writeAll(text); err != nil {
		return fmt.Errorf("copy to system clipboard: %w", err)
	}
	return nil
}
