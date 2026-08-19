# Development

```sh
cargo build --workspace
cargo test --workspace    # unit tests, no cluster
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

## Cluster VM tests

One NixOS VM per container engine, each running a real `kind` cluster against
`ztest cluster add|check|setup`. Defined in `nix/vm-test.nix`.

`nix build` reads the git tree, so you need to `git add .` before `nix run`

```sh
# Non-interactive background tests
nix run .#test-docker
nix run .#test-podman

# Boot into VM to run setup/bootstrap commands & validate UX
nix run .#vm-docker
nix run .#vm-podman
```

## Release

```sh
cargo release <version> --workspace              # dry run
cargo release <version> --workspace --execute    # commit + tag + publish in dep order
git push --follow-tags
```
