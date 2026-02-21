// Package version provides build-time version information.
//
// Variables are set via ldflags at build time:
//
//	go build -ldflags "-X github.com/robince/parqview/internal/version.Version=v1.0.0 ..."
package version

import (
	"fmt"
	"runtime/debug"
)

// These variables are set at build time via -ldflags.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// String returns a formatted version string.
func String() string {
	return fmt.Sprintf("parqview %s (commit %s, built %s)", Version, short(Commit), Date)
}

func short(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

func init() {
	// If not set via ldflags, try to get info from the Go module build info
	// (works for `go install` builds).
	if Version != "dev" {
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		Version = info.Main.Version
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			Commit = s.Value
		case "vcs.time":
			Date = s.Value
		}
	}
}
