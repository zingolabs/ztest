# Development

```sh
cargo build --workspace
cargo test --workspace    # unit tests, no cluster
nix flake check           # rustfmt + clippy -D warnings
```

## Release

```sh
cargo release <version> --workspace              # dry run
cargo release <version> --workspace --execute    # commit + tag + publish in dep order
git push --follow-tags
```
