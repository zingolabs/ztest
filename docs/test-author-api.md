# Test-Author API

All version selection lives in the test code. There is no `versions.toml`, no `ZAINO_*_VERSION` env var, no version coordinate outside the Rust call site.

Assets (config files, seed tarballs) live in the test crate under `tests/assets/`. The `mount_config!` / `mount_file!` / `mount_archive!`
macros resolve paths relative to `CARGO_MANIFEST_DIR` and **fail compilation** if the referenced file doesn't exist.

## At a glance

```rust
use ztest::prelude::*;

#[tokio::test]
async fn zaino_indexes_to_validator_tip() {
    let mut t = TestEnv::builder();
    let zeb = t.add(Validator::zebrad("1.9.1"));
    let zai = t.add(Indexer::zaino("0.4.0"));
    let env = t.build().await.unwrap();

    // Typed RPC sugar:
    env.handle(&zeb).mine_blocks(10).await.unwrap();

    // Or dial the endpoint directly:
    let rpc = env.handle(&zai).endpoint("grpc").await.unwrap();
    let tip = LightwalletdClient::connect(rpc.url("http")).await.unwrap()
                  .get_latest_block().await.unwrap();
    assert_eq!(tip.height, 10);
}
```

With a custom config + a seeded data dir:

```rust
#[tokio::test]
async fn returns_genesis_block_by_hash() {
    let mut t = TestEnv::builder();
    let zebra = t.add(Validator::zebrad("1.9.1")
        .mount(mount_config! ("tests/assets/zebrad.toml",              "/etc/zebrad/zebrad.toml"))
        .mount(mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data")));
    let zaino = t.add(Indexer::zaino("0.4.0")
        .mount(mount_archive!("tests/assets/zaino-100blocks.tar.zst",  "/state")));
    let env = t.build().await.unwrap();

    let genesis = env.handle(&zebra).block_at(0).await.unwrap();
    let block   = env.handle(&zaino).client().await
                      .get_block_by_hash(&genesis.hash).await.unwrap();
    assert_eq!(block.height, 0);
}
```

## Components

```rust
pub enum Validator { Zebrad(ZebradOpts), Zcashd(ZcashdOpts) }
pub enum Indexer   { Zaino(ZainoOpts) }
pub enum Wallet    { Zingo(ZingoOpts) }
```

| Variant  | `image_repo`            |
| -------- | ----------------------- |
| `Zebrad` | `zfnd/zebra`            |
| `Zcashd` | `electriccoinco/zcashd` |
| `Zaino`  | `zingolabs/zaino`       |
| `Zingo`  | `zingolabs/zingolib`    |

Constructors:

```rust
impl Validator {
    pub fn zebrad(version: impl Into<String>) -> Self;
    pub fn zcashd(version: impl Into<String>) -> Self;
}
impl Indexer { pub fn zaino(version: impl Into<String>) -> Self; }
impl Wallet  { pub fn zingo(version: impl Into<String>) -> Self; }
```

## Dev images

Most tests use a published image — `Validator::zebrad("1.9.1")` pulls
`zfnd/zebra:1.9.1`. When you're iterating on a component locally, swap
the constructor for a `dev!(...)` call that points at a `Dockerfile`:

```rust
let zai = t.add(dev!(Indexer::Zaino, "../packages/zainod/Dockerfile"));
let zeb = t.add(dev!(Validator::Zebrad, "../zebrad/Dockerfile"));
```

`dev!` returns the same `Indexer` / `Validator` value the published
constructor would — every builder method (`.named`, `.mount`,
`.resources`, `.expose`) chains the same way:

```rust
let zai = t.add(dev!(Indexer::Zaino, "../packages/zainod/Dockerfile")
    .named("zaino-dev")
    .mount(mount_archive!("tests/assets/zaino-100blocks.tar.zst", "/state")));
```

### What `dev!` does

`dev!(Component::Variant, "path/to/Dockerfile" [, context = "path/to/context"])`:

1. **Resolves paths** relative to the test source file (same rule as
   `mount_*` macros — uses `CARGO_MANIFEST_DIR` + `file!()`). Compile
   fails if the Dockerfile doesn't exist.
2. **Derives a content-addressed tag** from the resolved Dockerfile +
   context path. Two `dev!` sites pointing at the same Dockerfile
   collapse to one image build automatically.
3. **Registers the image declaration into the global inventory** via
   `inventory::submit!` inside a hidden `const _: () = { ... }` block.
   Registration happens at link time; the surrounding function never
   has to be called for discovery to see it.
4. **Returns a component value** wired to that tag, so the test code
   reads exactly like the published-image path.

### How discovery works

When you run `ztest run`, the harness:

1. Runs `cargo nextest list` to get the selected `(binary, test)` set.
2. Spawns each binary with at least one selected test under
   `ZTEST_DUMP_INVENTORY=1`. A `#[ctor::ctor]` hook inside ztest
   intercepts startup, serializes every `DevImageDecl` linked into
   that binary to stdout as JSON, and exits before `main` (so libtest
   never runs).
3. Unions the per-binary manifests, dedupes by tag, and emits a
   sequenced `docker build` for each unique image — output streams
   into the preflight scroll region the same way `cargo nextest list`
   already does.
4. For kind clusters: `kind load docker-image <tag>` per built image,
   with upload progress in the pinned banner.

Cache behavior is delegated to Docker — re-runs are cache hits and
near-instant. To force a rebuild, `docker image rm <tag>` or set
`ZTEST_REBUILD_IMAGES=1`.

### Per-binary granularity

Inventory is scoped per test binary, not per test. If `binary_a` has
any selected test, every `dev!` site reachable from `binary_a`'s link
graph contributes — even if the specific selected test doesn't hit
that site. This is almost free in practice (Docker layer cache hits)
and keeps the macro from having to coordinate with `#[test]`. If you
need a binary-wide image declaration that no individual test uses
yet, put a `dev!` call in any module-scope `const _: () = { dev!(...); };`
block.

### Constraints

- `dev!` is **not** valid outside a function body. The macro relies on
  `const _: () = { ... }` blocks being legal in a statement position
  to inject the `inventory::submit!`. Module-scope use needs the
  explicit wrapper above.
- Each component variant supports at most one `dev!` image per binary.
  Two `dev!(Indexer::Zaino, ...)` calls in the same binary with
  different Dockerfiles panic at startup with a clear `multiple dev
  images declared for Indexer::Zaino: <tag-a>, <tag-b>` message.
- `dev!` requires the Dockerfile path to be a string literal — needed
  so the macro can resolve it at compile time. Computed paths aren't
  supported.

Every enum exposes the same builder methods:

```rust
.named(name)                                  // for peering / lookup
.mount(mount)                                  // mount a file or directory at container startup
.resources(cpu, mem)                           // k8s requests; default per-kind sane value
.expose(name: &str, container_port: u16)      // additional named port beyond the variant defaults
```

## Mounts

```rust
pub struct Mount {
    pub source:      MountSource,
    pub destination: PathBuf,
    pub kind:        MountKind,
}

pub enum MountSource {
    ConfigAbs(PathBuf),          // emitted by mount_config!
    FileAbs(PathBuf),            // emitted by mount_file!
    ArchiveAbs(PathBuf),         // emitted by mount_archive!
    Snapshot(SnapshotRef),       // mid-test, via Mount::from_snapshot
}

pub enum MountKind {
    Config,       // ConfigMap; templated; ≤1 MiB UTF-8
    File,         // single-file PVC; opaque blob, no templating
    DirArchive,   // content-addressed extracted-tar PVC
}
```

Three macros, three deterministic materializations:

| Macro                      | Materialized as                                        | Templated? | Compile-time rules                              |
| -------------------------- | ------------------------------------------------------ | ---------- | ----------------------------------------------- |
| `mount_config!(rel, dst)`  | `ConfigMap` mounted at `dst`                           | Yes        | File must exist, be UTF-8, and be < 1 MiB       |
| `mount_file!(rel, dst)`    | Content-addressed single-file PVC                      | No         | File must exist                                 |
| `mount_archive!(rel, dst)` | Content-addressed extracted-tar PVC; CoW clone per use | No         | File must exist (suffix `.tar.zst` recommended) |

Mid-test snapshot mounts via `Mount::from_snapshot`:

```rust
let snap   = env.handle(&zebra).snapshot().await?;
let cloned = env.spawn(Validator::zebrad("1.9.1")
    .mount(Mount::from_snapshot(&snap, "/data"))).await?;
```

Snapshots are crash-consistent — clones boot as if the source crashed
at snapshot time. Regtest validators handle this. A `SnapshotRef` is
owned by the orchestrator pod; it lives until namespace teardown.

## Handles

A handle is the test's interface to a running component. Two layers:

1. **Endpoints** — raw `(host, port)` you can dial from the test process. Set up transparently; same API regardless of where the test runs.
1. **Typed RPC** — convenience methods on top of endpoints (`mine_blocks`, `tip`, etc.).

### Endpoints

```rust
pub struct Endpoint {
    pub host: IpAddr,        // 127.0.0.1 when port-forwarded; pod IP when in-cluster
    pub port: u16,           // local port (forwarded) or container port (direct)
}

impl Endpoint {
    pub fn socket_addr(&self) -> SocketAddr;
    pub fn url(&self, scheme: &str) -> String;        // "http://127.0.0.1:38291"
}
```

Every handle implements:

```rust
pub trait Handle {
    /// Resolve a named port (declared by the component variant).
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;

    /// Escape hatch — resolve by container port number.
    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError>;
}
```

Each component variant declares the named ports it exposes:

| Variant  | Named ports                     |
| -------- | ------------------------------- |
| `Zebrad` | `rpc` (28232), `metrics` (9999) |
| `Zcashd` | `rpc` (28232)                   |
| `Zaino`  | `grpc` (8137), `metrics` (9998) |
| `Zingo`  | `grpc` (20000)                  |

Add more on the component builder with `.expose(name, container_port)`.

Asking for a port the variant doesn't declare returns
`EnvError::UnknownEndpoint { name }`.

### How endpoints route

```
                          Library detects at TestEnv::build():

  In-cluster (CI runner pod):    Endpoint { host: pod.status.podIP, port: container_port }
                                  → direct TCP, no proxy

  Out-of-cluster (dev laptop):   Endpoint { host: 127.0.0.1, port: ephemeral }
                                  → kube-rs portforward → API server → pod:container_port
```

Test code is identical in both modes — `endpoint("rpc")` returns
something dialable. Port-forwards are lazy: created on first
`endpoint(name)` call per `(handle, name)` pair, cached, closed when
the handle drops.

### Typed RPC Handles

```rust
impl ValidatorHandle {
    pub async fn mine_blocks(&self, n: u32) -> Result<(), RpcError>;
    pub async fn tip(&self) -> Result<BlockTip, RpcError>;
    pub async fn block_at(&self, height: u32) -> Result<Block, RpcError>;
    pub async fn snapshot(&self) -> Result<SnapshotRef, EnvError>;
    pub fn rpc(&self) -> &dyn ValidatorRpc;
}
```

These build on `self.endpoint("rpc")` internally. If you need a
protocol the lib doesn't ship a client for, dial the endpoint yourself:

```rust
let zaino_endpoint = env.handle(&zaino).endpoint("grpc").await?;
let channel = tonic::transport::Channel::from_shared(zaino_endpoint.url("http"))?.connect().await?;
let mut client = LightwalletdClient::new(channel);
```

`IndexerHandle` and `WalletHandle` expose analogous RPC sugar over
their primary endpoint (`grpc` for zaino/zingo).

## Peering

```rust
let mut t = TestEnv::builder();
let alice = t.add(Validator::zebrad("1.9.1").named("alice")
    .mount(mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data")));
let bob = t.add(Validator::zebrad("1.9.1").named("bob"));
t.peer(&alice, &bob);
t.add(Indexer::zaino("0.4.0"));
let env = t.build().await?;
```

## rstest

Standard rstest. Each `#[case]` becomes its own nextest target.

```rust
#[rstest]
#[case::zebrad(Validator::zebrad("1.9.1"),
               mount_archive!("tests/assets/zebrad-100blocks.tar.zst", "/data"))]
#[case::zcashd(Validator::zcashd("6.4.1"),
               mount_archive!("tests/assets/zcashd-100blocks.tar.zst", "/data"))]
#[tokio::test]
async fn rejects_height_past_tip(#[case] v: Validator, #[case] data: Mount) {
    let mut t = TestEnv::builder();
    let _validator  = t.add(v.mount(data));
    let zaino  = t.add(Indexer::zaino("0.4.0"));
    let env  = t.build().await.unwrap();

    let err = env.handle(&zaino).client().await.get_block_by_height(999).await
        .expect_err("must reject past tip");
    assert!(matches!(err, ZainoError::BlockNotFound { height: 999 }));
}
```

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

`component` is `{kind}-{version}` (e.g. `zebrad-1.9.1`, `zaino-0.4.0`).
Client does not auto-retry on `Transient` — tests wrap their own policy
if idempotent.
