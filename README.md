# ztest

## Usage

```rust
use ztest::*;

#[tokio::test]
async fn zaino_indexes_to_validator_tip() {
    let mut t = TestEnv::builder();
    let zeb = t.add(Validator::zebrad("1.9.1"));
    let zai = t.add(dev!(Indexer::zaino("../Dockerfile")));
    let env = t.build().await.unwrap();

    // Typed RPC sugar:
    env.handle(&zeb).mine_blocks(10).await.unwrap();

    // Or dial the endpoint directly:
    let rpc = env.handle(&zai).endpoint("grpc").await.unwrap();
    // ...
}
```

Mount a custom config and a seeded data dir:

```rust
let zebra = t.add(Validator::zebrad("1.9.1")
    .mount(mount_config! ("tests/assets/zebrad.toml",              "/etc/zebrad/zebrad.toml"))
    .mount(mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data")));
```

## CLI

The `ztest` binary is the developer entry point for running ztest-managed
integration tests:

```sh
cargo run --bin ztest -- --help
```

## TODO

- [ ] Cleanup TestEnv::regtest_state_in() API and ::persistent_state_in() APIs? Not a fan
- [ ] MSRV as test parameterization target for test-matrix
- [ ] Scroll terminal after starting tests to give rustc, docker, and nexttest full height
- [ ] Cargo test compile fail UX
- [ ] Docker image build fail UX
- [ ] Test-config/manifest for enabling/disabling a set of cases? Ie, test mode without zcashd
- [ ] Replace GitLFS stuff with a StorageBackend trait/abstraction, and then create a git-lfs.rs
