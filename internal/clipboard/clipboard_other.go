//go:build !darwin

package clipboard

import "fmt"

// Copy is unsupported on non-darwin platforms in this initial implementation.
func Copy(text string) error {
	_ = text
	return fmt.Errorf("clipboard copy is only supported on darwin")
}
