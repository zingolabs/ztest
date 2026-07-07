# Rust-version matrix for dev images

Run one test across several rust toolchains — e.g. MSRV vs latest — by building
its `dev!` image once per version and letting [rstest] pick the version per case.

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
             rust_versions = RUSTS)   // build-set: every version is pre-built
            .rust_version(rust));     // per-case select: pick the pre-built one
    t.build().await?;
    // ...
    Ok(())
}
```

To pin a single toolchain (no matrix — also the way to stop ztest from clobbering
a Dockerfile's own default), use the singular form; no `.rust_version()` is then
needed, the returned spec is already set:

```rust
dev!(Validator::Zebrad, git = "…", rev = "…",
     dockerfile = "docker/Dockerfile", rust_version = "1.91.0")
```

## Why it's shaped this way

rstest turns each `#[case]` into a distinct test at compile time, so nextest and
the ztest engine already run and report the cases separately — no bespoke
matrix attribute, no engine changes. rstest is the whole multiplication.

The one constraint it can't satisfy is **discovery**. ztest builds every dev image
*before any test runs* (a static inventory dump drives pre-provisioning), and a
test at runtime only *looks up* an already-built tag — it never builds (a
`~10 min` zebra build would blow the test's time cap). An rstest `#[case]` value is
a runtime function argument the dump can't see. So the *set* of toolchains has to
be declared where the dump can read it: `rust_versions` on the `dev!` call.

That splits cleanly into two roles, which is why the version list appears twice:

- **`rust_versions`** — *what gets built.* A property of the image; drives
  pre-provisioning. Point both at one shared `const` so the values don't drift.
- **`#[case]` + `.rust_version(rust)`** — *what this run uses.* A property of the
  test; selects among the pre-built images at runtime.

Each version folds into the content-addressed tag, so `zebrad@1.88` and
`zebrad@1.91.0` are distinct `<repo>:dev-<hash>` images that coexist and cache
independently — the same simple hash-based tagging as every other dev image.

## Rules that keep you out of trouble

- **Keep the `#[case]` count in sync with the `const`.** rstest can't expand a
  const into cases, so the case lines are hand-written. Add a version to the const
  and forget a `#[case]` and you build an image no test uses (a wasted, slow zebra
  build); the reverse indexes past the end of the const.
- **Always thread `.rust_version(rust)`.** It's not enforced at compile time. Skip
  it and the test resolves the *default* tag, which — for a matrixed `dev!` — was
  never built, so `build()` fails loud with `DevImageMissing` rather than quietly
  running on the wrong toolchain. That failure is the intended safety net.
- **Only `dev!` images vary.** Published constructors (`Validator::zebrad("1.9.1")`)
  are pulled, not built; `.rust_version()` on them is a no-op.

## Cost

Every version is a full rebuild of that image, and image builds are serialized, so
an N-version matrix multiplies preflight build time and image-cache size by N.
Zebra is the expensive one (~10 min each) — keep matrix sets small.

## Resolution of the `RUST_VERSION` build-arg

For any dev image build, ztest picks the `RUST_VERSION` build-arg in this order:

1. the pinned version (`rust_version` / `.rust_version()` / a `rust_versions` entry),
2. a *concrete* `channel` version in a `rust-toolchain.toml` in the build context
   (a rustup channel name like `stable`/`beta`/`nightly` is ignored — it's not a
   docker image tag, so `rust:stable` would 404),
3. nothing — the Dockerfile's own `ARG RUST_VERSION` default stands.

Only (1) folds into the tag. (2) is a build-arg convenience and is deliberately
*not* part of image identity (folding it in would churn every existing tag).

[rstest]: https://docs.rs/rstest
