# Writing tests

Rust test-author API: build a topology with `TestEnv::builder`, dial components through handles, matrix a
`dev!` image across toolchains.

- Version selection lives in the test code — no `versions.toml`, no `ZAINO_*_VERSION`
- Assets (configs, seed tarballs) under the test crate's `tests/assets/`
- `mount_*` macros resolve against `CARGO_MANIFEST_DIR` and fail *compilation* on a missing file

## At a glance

```rust
use ztest::prelude::*;

#[tokio::test]
async fn zaino_indexes_to_validator_tip() {
    let mut t = TestEnv::builder();
    let zeb = t.add(Validator::zebrad("1.9.1"));
    let zai = t.add(Indexer::zaino("0.4.0"));
    let env = t.build().await.unwrap();

    env.handle(&zeb).mine_blocks(10).await.unwrap();       // typed RPC sugar

    let rpc = env.handle(&zai).endpoint("grpc").await.unwrap();   // or dial directly
    let tip = LightwalletdClient::connect(rpc.url("http")).await.unwrap()
                  .get_latest_block().await.unwrap();
    assert_eq!(tip.height, 10);
}
```

## Components

```rust
pub enum Validator { Zebrad(ZebradOpts), Zcashd(ZcashdOpts) }
pub enum Indexer   { Zaino(ZainoOpts) }
```

| Variant  | `image_repo`            | Constructor                  | Named ports                     |
| -------- | ----------------------- | ---------------------------- | ------------------------------- |
| `Zebrad` | `zfnd/zebra`            | `Validator::zebrad(version)` | `rpc` (28232), `metrics` (9999) |
| `Zcashd` | `electriccoinco/zcashd` | `Validator::zcashd(version)` | `rpc` (28232)                   |
| `Zaino`  | `zingolabs/zaino`       | `Indexer::zaino(version)`    | `grpc` (8137), `metrics` (9998) |

Published constructor pulls `<image_repo>:<version>`. Every variant chains the same builders:

```rust
.named(name)                    // for peering / lookup
.mount(mount)                   // mount a file or directory at startup
.resources(cpu, mem)            // k8s requests; per-kind default
.expose(name, container_port)   // extra named port beyond the variant defaults
```

## `dev!` — build a component from source

Replaces the published constructor, returns the same `Validator`/`Indexer`/`Wallet` value, so the rest of
the test is unchanged. Two source forms:

```rust
// Local Dockerfile, resolved relative to the test source file:
dev!(Validator::Zebrad, "../zebrad/Dockerfile")
dev!(Indexer::Zaino,   "../packages/zainod/Dockerfile", context = "../packages")

// Remote git checkout:
dev!(Validator::Zebrad,
     git        = "https://github.com/ZcashFoundation/zebra.git",
     rev        = "9a27f886a5bfb143f65d1712e912cef252426800",
     dockerfile = "docker/Dockerfile")
```

Both accept the builder chain + an optional Rust-version selector ([matrix](#multi-rust-version-matrix)):

```rust
let zai = t.add(dev!(Indexer::Zaino, "../packages/zainod/Dockerfile")
    .named("zaino-dev")
    .mount(mount_archive!("tests/assets/zaino-100blocks.tar.zst", "/state")));
```

Dockerfile/`git`/`rev`/`context` fold into a content-addressed `<repo>:dev-<hash>` tag → identical `dev!`
sites collapse to one build, distinct ones cache independently. Re-runs hit cache; force with
`docker image rm <tag>` or `ZTEST_REBUILD_IMAGES=1`.

### Constraints

- **Every dev image builds before any test runs**, from a static declaration scan. A test only *looks up*
  an already-built tag; a tag with no matching build fails `build()` with `DevImageMissing`
- Valid only inside a function body. Binary-wide declaration no test references yet:
  `const _: () = { dev!(...); };`
- At most one `dev!` image per component variant per test binary — a second `dev!(Indexer::Zaino, …)`
  with a different source panics at startup
- Dockerfile / `git` / `rev` / `context` must be string literals (compile-time resolved); no computed paths

## Mounts

```rust
pub struct Mount { pub source: MountSource, pub destination: PathBuf, pub kind: MountKind }

pub enum MountSource {
    ConfigAbs(PathBuf),          // mount_config!
    ConfigInline(String),        // generated config bytes (regtest_conf)
    Seed(ChainSnapshot),         // mount_file! and mount_archive!
    Empty,                       // Mount::scratch
    SharedClaim { claim: String },// TestEnv::shared_volume
}
pub enum MountKind { Config, File, DirArchive, Scratch, Shared }
```

| Macro                      | Materialized as                                        | Templated | Compile-time rules                  |
| -------------------------- | ------------------------------------------------------ | --------- | ----------------------------------- |
| `mount_config!(rel, dst)`  | `ConfigMap` at `dst`                                   | Yes       | Must exist, UTF-8, < 1 MiB          |
| `mount_file!(rel, dst)`    | Content-addressed single-file PVC                      | No        | Must exist                          |
| `mount_archive!(rel, dst)` | Content-addressed extracted-tar PVC; CoW clone per use | No        | Must exist (`.tar.zst` recommended) |

## Handles and endpoints

`env.handle(&h)` = the test's interface to a running component: an **endpoint** (raw `(host, port)`) plus
**typed RPC** sugar over it.

```rust
pub struct Endpoint { pub host: IpAddr, pub port: u16 }
impl Endpoint {
    pub fn socket_addr(&self) -> SocketAddr;
    pub fn url(&self, scheme: &str) -> String;   // "http://127.0.0.1:38291"
}

pub trait Handle {
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;
    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError>;
}
```

Routing resolves at `TestEnv::build()`, transparent to test code:

| Mode                    | Endpoint                                 | Transport                               |
| ----------------------- | ---------------------------------------- | --------------------------------------- |
| In-cluster (CI runner)  | `{ host: pod IP, port: container_port }` | direct TCP                              |
| Out-of-cluster (laptop) | `{ host: 127.0.0.1, port: ephemeral }`   | kube-rs port-forward → API server → pod |

- Port-forwards created lazily on first `endpoint(name)` per `(handle, name)`, cached, closed on handle drop
- Undeclared port → `EnvError::UnknownEndpoint`

Typed RPC builds on `endpoint("rpc")` (validators) / `endpoint("grpc")` (indexers, wallets):

```rust
impl ValidatorHandle {
    pub async fn mine_blocks(&self, n: u32) -> Result<(), RpcError>;
    pub async fn tip(&self) -> Result<BlockTip, RpcError>;
    pub async fn block_at(&self, height: u32) -> Result<Block, RpcError>;
    pub fn rpc(&self) -> &dyn ValidatorRpc;
}
```

No shipped client for a protocol → dial the endpoint yourself:

```rust
let ep = env.handle(&zaino).endpoint("grpc").await?;
let channel = tonic::transport::Channel::from_shared(ep.url("http"))?.connect().await?;
let mut client = LightwalletdClient::new(channel);
```

## Peering

```rust
let alice = t.add(Validator::zebrad("1.9.1").named("alice")
    .mount(mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data")));
let bob = t.add(Validator::zebrad("1.9.1").named("bob"));
t.peer(&alice, &bob);
```

## rstest

Standard [rstest]; each `#[case]` becomes its own nextest target.

```rust
#[rstest]
#[case::zebrad(Validator::zebrad("1.9.1"),
               mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data"))]
#[case::zcashd(Validator::zcashd("6.4.1"),
               mount_archive!("tests/assets/zcashd-100blocks.tar.zst", "/data"))]
#[tokio::test]
async fn rejects_height_past_tip(#[case] v: Validator, #[case] data: Mount) {
    let mut t = TestEnv::builder();
    t.add(v.mount(data));
    let zaino = t.add(Indexer::zaino("0.4.0"));
    let env = t.build().await.unwrap();

    let err = env.handle(&zaino).client().await.get_block_by_height(999).await
        .expect_err("must reject past tip");
    assert!(matches!(err, ZainoError::BlockNotFound { height: 999 }));
}
```

## Multi-Rust-version matrix

Build one `dev!` image per toolchain, let rstest pick the case. The version list appears in two roles:

- `rust_versions` on the `dev!` call — **what gets built** (a property of the image, read by the pre-build scan)
- `#[case]` + `.rust_version(rust)` — **what a run uses** (selects among pre-built images at runtime)

```rust
const RUSTS: &[&str] = &["1.88", "1.91.0"];

#[rstest]
#[case(RUSTS[0])]
#[case(RUSTS[1])]
#[ztest::qos::integration]
#[tokio::test(flavor = "multi_thread")]
async fn builds_on_rust(#[case] rust: &str) -> Result<()> {
    let mut t = TestEnv::builder();
    let zeb = t.add_validator(
        dev!(Validator::Zebrad,
             git = "https://github.com/ZcashFoundation/zebra.git",
             rev = "9a27f886a5bfb143f65d1712e912cef252426800",
             dockerfile = "docker/Dockerfile",
             rust_versions = RUSTS)      // build every version
            .rust_version(rust));        // select this case's version
    t.build().await?;
    Ok(())
}
```

Singular `rust_version = "…"` pins one toolchain (and stops ztest overriding a Dockerfile's own default);
no `.rust_version()` call needed then. Each version folds into the tag, so `zebrad@1.88` and
`zebrad@1.91.0` coexist.

### Rules

- **Keep `#[case]` count in sync with the `const`** — rstest cannot expand a const into cases; an extra
  entry builds an unused image (slow), a missing one indexes past the end
- **Always thread `.rust_version(rust)`** — not compile-enforced. Skipping it resolves the *default* tag,
  never built for a matrixed `dev!` → `build()` fails with `DevImageMissing`
- **Only `dev!` images vary** — `.rust_version()` on a published constructor is a no-op (pulled, not built)

### `RUST_VERSION` build-arg resolution

1. Pinned version (`rust_version` / `.rust_version()` / a `rust_versions` entry)
1. A *concrete* `channel` in a `rust-toolchain.toml` in the build context — a rustup channel name
   (`stable`/`beta`/`nightly`) is ignored, it isn't a docker tag
1. The Dockerfile's own `ARG RUST_VERSION` default

Only (1) folds into the image tag.

### Cost

Each version = a full serialized rebuild → preflight time and cache size × N. Zebra is ~10 min per build;
keep matrix sets small.

## Errors

```rust
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("{component} failed to become ready after {elapsed:?}")]
    NotReady { component: String, elapsed: Duration },
    #[error("{component} exited uncleanly (exit {exit_code}) after {elapsed:?}")]
    UncleanExit { component: String, elapsed: Duration, exit_code: i32 },
    #[error("{component} RPC '{op}' timed out after {elapsed:?}")]
    RpcTimeout { component: String, op: &'static str, elapsed: Duration },
    #[error("archive materialize failed for {source}: {reason}")]
    ArchiveMaterializeFailed { source: PathBuf, reason: String },
    #[error("{component} does not expose endpoint '{name}'")]
    UnknownEndpoint { component: String, name: String },
    #[error("port-forward to {component}:{port} failed: {reason}")]
    PortForwardFailed { component: String, port: u16, reason: String },
    #[error(transparent)]
    Transient(Box<dyn Error + Send + Sync>),
}
```

- `component` = `{kind}-{version}` (e.g. `zebrad-1.9.1`)
- No auto-retry on `Transient` — wrap your own policy when the operation is idempotent

## See also

- [guide-running-tests.md](guide-running-tests.md) — invoking the suite, slots, filtering
- [design-remote-execution.md](design-remote-execution.md) — on-cluster image build flow
- [ops-clusters.md](ops-clusters.md) — cluster profiles and `ZTEST_IMAGE_*`

[rstest]: https://docs.rs/rstest
