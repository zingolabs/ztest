# ztest

Boot Zcash topologies (validators, indexers, wallets) on Kubernetes and
hand **typed RPC handles** back to test code.

### Quickstart

```sh
kind create cluster
ztest cluster add kind --kind kind --set-default
ztest cluster setup
ztest run
```

A stock kind cluster cannot snapshot, so seeded tests will fail — `ztest cluster check` says so. See [docs/ops-local-cluster.md](docs/ops-local-cluster.md) to fix
that; [docs/ops-clusters.md](docs/ops-clusters.md) covers remote clusters.

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
let zai = t.add_indexer(dev!(Indexer::Zainod, "../Dockerfile"));
```

Accepted variants: `Validator::Zebrad`, `Validator::Zcashd`,
`Indexer::Zainod`, `Wallet::Zingo`.

## CLI

The `ztest` binary is the developer entry point for running ztest-managed
integration tests:

```sh
cargo run --bin ztest -- --help
```

## TODO

- [ ] Review config rendering path (Use rust structs instead of toml? How to handle version matrix w/ config migrations?) -> Put this into a trait?

  - That way zaino developers can define the function to generate the config toml based on the version? Need a clear example of how this would look

- [ ] Migrate src/backends/zainod.rs to src/backends/zainod/{config,backend,sync}.rs
