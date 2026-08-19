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

Every nix build here reads the **git tree**. you MUST `git add` any changes before running these nix tests

```sh
nix build .#checks.x86_64-linux.ztest-cluster-docker -L

nix build .#checks.x86_64-linux.ztest-cluster-docker.driverInteractive
./result/bin/nixos-test-driver --keep-vm-state    # reuses $TMPDIR/vm-state-machine
>>> start_all()
>>> test_script()          # run the whole script, then drop back to the REPL


# From second-terminal:  SSH into the VM
ssh vsock/3 -o User=root
```

## Release

```sh
cargo release <version> --workspace              # dry run
cargo release <version> --workspace --execute    # commit + tag + publish in dep order
git push --follow-tags
```
