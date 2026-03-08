package clipboard

import (
	"fmt"

	systemclipboard "github.com/atotto/clipboard"
)

var writeAll = systemclipboard.WriteAll

// Copy copies text to the system clipboard.
func Copy(text string) error {
	if err := writeAll(text); err != nil {
		return fmt.Errorf("copy to system clipboard: %w", err)
	}
	return nil
}
