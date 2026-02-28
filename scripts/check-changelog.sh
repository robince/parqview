#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 1.0.0"
  exit 1
fi

VERSION="$1"
CHANGELOG_FILE="CHANGELOG.md"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: version must match X.Y.Z"
  exit 1
fi

if [ ! -f "$CHANGELOG_FILE" ]; then
  echo "Error: $CHANGELOG_FILE does not exist"
  exit 1
fi

if ! grep -Eq "^## \[$VERSION\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" "$CHANGELOG_FILE"; then
  echo "Error: expected heading '## [$VERSION] - YYYY-MM-DD' in $CHANGELOG_FILE"
  exit 1
fi

SECTION="$(
  awk -v version="$VERSION" '
    $0 ~ "^## \\[" version "\\] - " { in_section=1; next }
    in_section && $0 ~ "^## \\[" { exit }
    in_section { print }
  ' "$CHANGELOG_FILE"
)"

if [ -z "$(printf '%s' "$SECTION" | tr -d '[:space:]')" ]; then
  echo "Error: changelog section for $VERSION is empty"
  exit 1
fi

echo "OK: changelog contains a non-empty section for $VERSION"
