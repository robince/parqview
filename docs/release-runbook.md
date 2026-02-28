# Parqview Release Runbook

## One-time setup

1. Create GitHub repository `robince/homebrew-tap` (public).
2. In `robince/parqview`, add repository secret `HOMEBREW_TAP_TOKEN`:
   - Fine-grained personal access token
   - Repository access: `robince/homebrew-tap`
   - Permission: `Contents` read/write
3. Add branch protection on `main` requiring CI checks:
   - `test (ubuntu-latest)`
   - `test (macos-latest)`
   - `lint`

## Per-release steps

1. Ensure `main` is green in CI.
2. Update `CHANGELOG.md`:
   - Fill entries under `## [Unreleased]` during development.
   - Move release notes into `## [X.Y.Z] - YYYY-MM-DD`.
3. Validate changelog:

```bash
just changelog 1.0.0
```

4. Tag and push annotated release tag:

```bash
just release 1.0.0
```

5. Wait for `.github/workflows/release.yml` to complete.
6. Verify GitHub release assets include:
   - `parqview_X.Y.Z_darwin_amd64.tar.gz`
   - `parqview_X.Y.Z_darwin_arm64.tar.gz`
   - `parqview_X.Y.Z_linux_amd64.tar.gz`
   - `parqview_X.Y.Z_linux_arm64.tar.gz`
   - `SHA256SUMS`
7. Verify tap update landed in `robince/homebrew-tap`:
   - `Formula/parqview.rb` version and checksums match the release.
8. Smoke test install:

```bash
brew install robince/tap/parqview
parqview --version
```
