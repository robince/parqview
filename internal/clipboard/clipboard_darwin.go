package clipboard

import (
	"os/exec"
	"strings"
)

// Copy copies text to the system clipboard using pbcopy.
func Copy(text string) error {
	cmd := exec.Command("pbcopy")
	cmd.Stdin = strings.NewReader(text)
	return cmd.Run()
}
