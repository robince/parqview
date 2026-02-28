#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 1.0.0"
  exit 1
fi

VERSION="$1"
TAG="v${VERSION}"
CHANGELOG_FILE="CHANGELOG.md"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: version must match X.Y.Z"
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: must be run from a git repository"
  exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "Error: releases must be tagged from main (current: $CURRENT_BRANCH)"
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "Error: working tree is dirty (including untracked files). Commit or stash changes before release."
  exit 1
fi

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "Error: tag $TAG already exists"
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

CHANGELOG_SECTION="$(
  awk -v version="$VERSION" '
    $0 ~ "^## \\[" version "\\] - " { in_section=1; next }
    in_section && $0 ~ "^## \\[" { exit }
    in_section { print }
  ' "$CHANGELOG_FILE"
)"

if [ -z "$(printf '%s' "$CHANGELOG_SECTION" | tr -d '[:space:]')" ]; then
  echo "Error: changelog section for $VERSION is empty"
  exit 1
fi

TMP_MSG="$(mktemp)"
trap 'rm -f "$TMP_MSG"' EXIT

{
  echo "Release $VERSION"
  echo
  printf "%s\n" "$CHANGELOG_SECTION"
} >"$TMP_MSG"

echo "Creating annotated tag $TAG"
git tag -a "$TAG" -F "$TMP_MSG"

echo "Pushing tag $TAG to origin"
if ! git push origin "$TAG"; then
  git tag -d "$TAG" >/dev/null 2>&1 || true
  echo "Push failed; local tag $TAG was removed. Re-run the release command to retry."
  exit 1
fi

echo "Done. GitHub Actions will publish the release for $TAG."
