# ztest

Boot Zcash topologies (validators, indexers, wallets) on Kubernetes and
hand **typed RPC handles** back to test code.

### Quickstart: run against a local kind cluster

Create a kind cluster, register it as a ztest profile, and run — the profile
binds the kube-context and image distribution so every `ztest run` lands there:

```sh
kind create cluster --name ztest
ztest cluster add local --kind ztest --set-default
ztest run
```

## Usage

`TestEnv::builder()` collects components; each `add_*` call returns the
component's concrete, typed handle. The handles only become usable after
`build()` — calling one earlier returns `EnvError::NotBuilt`.

```rust
use ztest::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn zaino_indexes_to_validator_tip() {
    let mut t = TestEnv::builder();
    let zeb = t.add_validator(Validator::zebrad("1.9.1").regtest());
    let zai = t.add_indexer(Indexer::zaino("0.4.0").regtest());
    t.build().await.unwrap();

    // Typed RPC sugar on the validator handle:
    zeb.generate_blocks(10).await.unwrap();
    zai.poll_block_height(zeb.chain_height().await.unwrap())
        .await
        .unwrap();

    // Or dial an endpoint directly (named ports per backend):
    let grpc = zai.endpoint("grpc").await.unwrap();
    let _uri = grpc.url("http"); // e.g. "http://127.0.0.1:38291"
}
```

The handle types are backend-specific (`ZebraValidator`, `ZainoIndexer`,
`ZingoWallet`), so a backend-only RPC called on the wrong backend is a
**compile error** — no downcasts, no runtime panics.

### In-process wallet

The zingo wallet runs in the test binary (no pod) against the indexer's
gRPC. ztest ships well-known regtest seeds, so a funded faucet needs no
mnemonic wiring:

```rust
let w = t.add_wallet(Wallet::zingo());
t.build().await.unwrap();

let faucet = w.funded_faucet(&zeb, &zai).await.unwrap();
let recipient = w.recipient(&zeb, &zai).await.unwrap();
let to = recipient.address(Pool::Orchard).await.unwrap();
faucet.send(&to, 100_000).await.unwrap();
```

### Mount a custom config and a seeded data dir

```rust
let zebra = t.add_validator(
    Validator::zebrad("1.9.1")
        .mount(mount_config! ("tests/assets/zebrad.toml",              "/etc/zebrad/zebrad.toml"))
        .mount(mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data")),
);
```

### Dev images

When iterating on a component locally, swap the published constructor for
a `dev!(...)` pointed at a `Dockerfile` (resolved relative to the test
crate; compile fails if missing):

```rust
let zai = t.add_indexer(dev!(Indexer::Zainod, "../packages/zainod/Dockerfile"));
```

Accepted variants: `Validator::Zebrad`, `Validator::Zcashd`,
`Indexer::Zainod`, `Wallet::Zingo`.

### Rust-version matrix

Run one test across several toolchains (e.g. MSRV vs latest) by declaring the
build-set on `dev!` and selecting per case with [rstest](https://docs.rs/rstest):

```rust
const RUSTS: &[&str] = &["1.88", "1.91.0"];

#[rstest]
#[case(RUSTS[0])]
#[case(RUSTS[1])]
#[tokio::test(flavor = "multi_thread")]
async fn builds_on_rust(#[case] rust: &str) {
    let zeb = t.add_validator(
        dev!(Validator::Zebrad, git = "…", rev = "…",
             dockerfile = "docker/Dockerfile", rust_versions = RUSTS)
            .rust_version(rust));
    // ...
}
```

The preflight pipeline pre-builds `zebrad:dev-<hash>` per version (the toolchain
folds into the content-addressed tag); each case runs against its own image. To
pin a single toolchain without a matrix, use the singular `rust_version = "1.91.0"`
(no `.rust_version()` call needed). See [`docs/rust-version-matrix.md`](docs/rust-version-matrix.md)
for how and why the test is written this way.

## CLI

The `ztest` binary is the developer entry point for running ztest-managed
integration tests:

```sh
cargo run --bin ztest -- --help
```

## TODO

- [ ] Cleanup TestEnv::regtest_state_in() API and ::persistent_state_in() APIs? Not a fan
- [x] MSRV as test parameterization target for test-matrix (see [`docs/rust-version-matrix.md`](docs/rust-version-matrix.md))
- [ ] Cargo test compile fail UX
- [ ] Docker image build fail UX
- [ ] Test-config/manifest for enabling/disabling a set of cases? Ie, test mode without zcashd
- [ ] Replace GitLFS stuff with a StorageBackend trait/abstraction, and then create a git-lfs.rs
