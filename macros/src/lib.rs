//! Compile-time macros for `ztest`.
//!
//! `mount_config!` takes `(relative_source, container_destination)` and:
//! - resolves the source against `CARGO_MANIFEST_DIR` of the *invoking* crate,
//! - asserts the file exists, is UTF-8 and `< 1 MiB` at compile time,
//! - expands to a `::ztest::Mount` value carrying the absolute path.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{ItemFn, LitStr, Token, parse::Parse, parse::ParseStream, parse_macro_input};

const ONE_MIB: u64 = 1024 * 1024;

struct MountArgs {
    source: LitStr,
    destination: LitStr,
}

impl Parse for MountArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let source: LitStr = input.parse()?;
        let _: Token![,] = input.parse()?;
        let destination: LitStr = input.parse()?;
        // Allow trailing comma.
        let _ = input.parse::<Option<Token![,]>>();
        Ok(MountArgs { source, destination })
    }
}

fn resolve_source(rel: &LitStr) -> Result<std::path::PathBuf, syn::Error> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(rel.span(), "CARGO_MANIFEST_DIR not set; cannot resolve mount source")
    })?;
    let value = rel.value();
    let p = std::path::Path::new(&manifest).join(&value);
    if !p.exists() {
        return Err(syn::Error::new(
            rel.span(),
            format!("mount source does not exist: {}", p.display()),
        ));
    }
    if !p.is_file() {
        return Err(syn::Error::new(
            rel.span(),
            format!("mount source is not a regular file: {}", p.display()),
        ));
    }
    Ok(p)
}

/// A `ConfigMap`-backed mount: templated from a path at build time, with no
/// seed and no content addressing.
fn emit_config_mount(
    source_abs: &std::path::Path,
    destination: &LitStr,
) -> proc_macro2::TokenStream {
    let abs = source_abs.to_string_lossy().into_owned();
    let dst = destination.value();
    quote! {
        ::ztest::Mount {
            source: ::ztest::MountSource::ConfigAbs(
                ::std::path::PathBuf::from(#abs),
            ),
            destination: ::std::path::PathBuf::from(#dst),
            kind: ::ztest::MountKind::Config,
        }
    }
}

/// `mount_config!("rel/path.toml", "/etc/foo/foo.toml")`
///
/// Becomes a `ConfigMap`-backed mount. Compile-time checks: file exists,
/// is valid UTF-8, and is `< 1 MiB`.
#[proc_macro]
pub fn mount_config(input: TokenStream) -> TokenStream {
    let MountArgs { source, destination } = parse_macro_input!(input as MountArgs);
    let abs = match resolve_source(&source) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    match std::fs::metadata(&abs) {
        Ok(md) if md.len() >= ONE_MIB => {
            return syn::Error::new(
                source.span(),
                format!(
                    "mount_config! requires source < 1 MiB; {} is {} bytes",
                    abs.display(),
                    md.len()
                ),
            )
            .to_compile_error()
            .into();
        }
        Ok(_) => {}
        Err(e) => {
            return syn::Error::new(source.span(), format!("stat failed: {e}"))
                .to_compile_error()
                .into();
        }
    }
    if let Ok(bytes) = std::fs::read(&abs)
        && std::str::from_utf8(&bytes).is_err()
    {
        return syn::Error::new(
            source.span(),
            format!("mount_config! requires UTF-8 source; {} is not UTF-8", abs.display()),
        )
        .to_compile_error()
        .into();
    }
    emit_config_mount(&abs, &destination).into()
}

// ───────────────────────────── dev! macro ─────────────────────────────

/// Two forms:
///
/// - Local: `dev!(Indexer::Zainod, "rel/Dockerfile" [, context = "rel/ctx", version = "…", features = ["…"]])`
///   — a Dockerfile in the local checkout, path resolved against the caller's
///   `CARGO_MANIFEST_DIR` (same rule as the `mount_*` macros). Compile fails if
///   the Dockerfile doesn't exist or the context isn't a directory.
/// - Git: `dev!(Validator::Zebrad, git = "<url>", rev = "<sha>", dockerfile = "in/tree" [, context = "in/tree", version = "…", features = ["…"]])`
///   — built from `<url>` checked out at `<rev>`, using an in-tree Dockerfile
///   and context (paths relative to the repo root; context defaults to `"."`).
///   The rev is the tag suffix (`<repo>:dev-<rev>`), so no fetch is needed to
///   name the image.
///
/// Block expression returning a `Validator` / `Indexer` / `Wallet` value whose
/// container image was declared as a dev image. At the same call site the macro
/// injects an `inventory::submit!` for the corresponding `DevImageDecl`, so
/// the preflight image pipeline can discover and build the image before any
/// test runs. `version` names the release a build corresponds to for backends
/// (zebra) that render config / derive a ceiling from it; it defaults to `"dev"`.
///
/// Supported component variants: `Validator::Zebrad`, `Validator::Zcashd`,
/// `Indexer::Zainod`. Any other path yields a compile error — keeps the
/// matrix grep-able and the test-author surface small.
#[proc_macro]
pub fn dev(input: TokenStream) -> TokenStream {
    let DevArgs { variant, source, version, features, rust_version, rust_versions } =
        parse_macro_input!(input as DevArgs);

    if rust_version.is_some() && rust_versions.is_some() {
        return syn::Error::new(
            variant.span(),
            "dev!: use either `rust_version = \"x\"` (pin one) or \
             `rust_versions = [...]` (a matrix set), not both",
        )
        .to_compile_error()
        .into();
    }

    // Derive the kind label from the variant name itself — lowercased.
    // `Indexer::Zainod` → `"zainod"`, `Validator::Zebrad` → `"zebrad"`, etc.
    // The lowercased form is used two ways:
    //   - as the inventory `repo:` field (becomes the local image
    //     repo name in the resolved `<repo>:dev-<suffix>` tag),
    //   - as the constructor ident (`Indexer::zainod_dev(...)`).
    let kind_str: String =
        match (variant.category.to_string().as_str(), variant.variant.to_string().as_str()) {
            ("Validator", "Zebrad") => "zebrad".to_string(),
            ("Validator", "Zcashd") => "zcashd".to_string(),
            ("Indexer", "Zainod") => "zainod".to_string(),
            (cat, var) => {
                return syn::Error::new(
                    variant.span(),
                    format!(
                        "dev!: unsupported component variant `{cat}::{var}`; \
                     expected one of `Validator::Zebrad`, `Validator::Zcashd`, \
                     `Indexer::Zainod`"
                    ),
                )
                .to_compile_error()
                .into();
            }
        };

    let feat_lits: Vec<String> =
        features.map(|fs| fs.iter().map(LitStr::value).collect()).unwrap_or_default();
    let repo_lit = kind_str.clone();
    // The release this build corresponds to (validators render config / derive
    // a ceiling from it); `"dev"` when the caller doesn't say.
    let version_lit = version.map(|v| v.value()).unwrap_or_else(|| "dev".to_string());

    // Per source form, build the static `DevSourceDecl` (inventory) and the
    // owned `DevSource` (constructor arg). Local paths resolve against
    // `CARGO_MANIFEST_DIR` at compile time; git paths stay repo-relative (the
    // pipeline resolves them against the fetched checkout).
    let (decl_source, ctor_source) = match source {
        DevSourceArg::Local { dockerfile, context } => {
            let df_abs = match dev_local(&dockerfile, resolve_source) {
                Ok(p) => p,
                Err(e) => return e.to_compile_error().into(),
            };
            let ctx_abs = match context {
                Some(c) => match dev_local(&c, resolve_dir) {
                    Ok(p) => p,
                    Err(e) => return e.to_compile_error().into(),
                },
                None => df_abs
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from(".")),
            };
            let df_lit = df_abs.to_string_lossy().into_owned();
            let ctx_lit = ctx_abs.to_string_lossy().into_owned();
            (
                quote! {
                    ::ztest::macro_support::DevSourceDecl::Local {
                        dockerfile: #df_lit,
                        context: #ctx_lit,
                    }
                },
                quote! {
                    ::ztest::DevSource::Local {
                        dockerfile: ::std::path::PathBuf::from(#df_lit),
                        context: ::std::path::PathBuf::from(#ctx_lit),
                    }
                },
            )
        }
        DevSourceArg::Git { url, rev, dockerfile, context } => {
            let url_s = url.value();
            let rev_s = rev.value();
            let df_s = dockerfile.value();
            let ctx_s = context.map(|c| c.value()).unwrap_or_else(|| ".".to_string());
            (
                quote! {
                    ::ztest::macro_support::DevSourceDecl::Git {
                        url: #url_s,
                        rev: #rev_s,
                        dockerfile: #df_s,
                        context: #ctx_s,
                    }
                },
                quote! {
                    ::ztest::DevSource::Git {
                        url: #url_s.to_string(),
                        rev: #rev_s.to_string(),
                        dockerfile: #df_s.to_string(),
                        context: #ctx_s.to_string(),
                    }
                },
            )
        }
    };

    let category_ident = &variant.category;
    let ctor_ident = syn::Ident::new(&format!("{kind_str}_dev"), variant.variant.span());

    // The build-set for the inventory decl: an explicit plural set, or the
    // singular pin as a one-element set, or empty (Dockerfile default).
    let rust_versions_tokens = match (&rust_versions, &rust_version) {
        (Some(set), _) => set.clone(),
        (None, Some(v)) => quote! { &[ #v ] },
        (None, None) => quote! { &[] },
    };
    // A singular pin also selects the version on the returned spec, so the test
    // needs no `.rust_version()` call; a plural set leaves selection to the test.
    let rust_version_chain = match &rust_version {
        Some(v) => quote! { .rust_version(#v) },
        None => quote! {},
    };

    quote! {
        {
            ::ztest::__private::inventory::submit! {
                ::ztest::macro_support::DevImageDecl {
                    repo: #repo_lit,
                    source: #decl_source,
                    features: &[ #( #feat_lits ),* ],
                    rust_versions: #rust_versions_tokens,
                }
            }
            ::ztest::#category_ident::#ctor_ident(
                #ctor_source,
                #version_lit,
                ::std::vec![ #( ::std::string::String::from(#feat_lits) ),* ],
            ) #rust_version_chain
        }
    }
    .into()
}

struct DevVariant {
    category: syn::Ident,
    variant: syn::Ident,
}

impl DevVariant {
    fn span(&self) -> Span {
        self.category.span()
    }
}

/// Where the dev image is built from, as parsed from the macro input.
enum DevSourceArg {
    /// Positional local form: `"rel/Dockerfile" [, context = "rel/ctx"]`.
    /// Paths are caller-relative (resolved against `CARGO_MANIFEST_DIR`).
    Local { dockerfile: LitStr, context: Option<LitStr> },
    /// Keyword git form: `git = "…", rev = "…", dockerfile = "in/tree" [, context = "in/tree"]`.
    /// Paths are relative to the fetched repo root (default context `"."`).
    Git { url: LitStr, rev: LitStr, dockerfile: LitStr, context: Option<LitStr> },
}

struct DevArgs {
    variant: DevVariant,
    source: DevSourceArg,
    /// The release this build corresponds to; threaded to the `_dev`
    /// constructor for backends (zebra) that render config / derive a ceiling
    /// from a version. Defaults to `"dev"`.
    version: Option<LitStr>,
    /// Cargo features; `None` = none.
    features: Option<Vec<LitStr>>,
    /// Singular `rust_version = "x"`: pins the built + selected toolchain.
    rust_version: Option<LitStr>,
    /// Plural `rust_versions = <expr>`: the pre-build set, lowered to decl-field
    /// tokens. Mutually exclusive with `rust_version`.
    rust_versions: Option<proc_macro2::TokenStream>,
}

impl Parse for DevArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let category: syn::Ident = input.parse()?;
        let _: Token![::] = input.parse()?;
        let variant: syn::Ident = input.parse()?;
        let _: Token![,] = input.parse()?;

        // A string literal here → the positional local form. An ident (e.g.
        // `git`) → the keyword form. This is the one-token lookahead that
        // disambiguates the two shapes.
        if input.peek(LitStr) {
            let dockerfile: LitStr = input.parse()?;
            let mut kw = KwArgs::default();
            kw.parse_trailing(
                input,
                &["context", "version", "features", "rust_version", "rust_versions"],
            )?;
            return Ok(DevArgs {
                variant: DevVariant { category, variant },
                source: DevSourceArg::Local { dockerfile, context: kw.context },
                version: kw.version,
                features: kw.features,
                rust_version: kw.rust_version,
                rust_versions: kw.rust_versions,
            });
        }

        let mut kw = KwArgs::default();
        kw.parse_all(
            input,
            &[
                "git",
                "rev",
                "dockerfile",
                "context",
                "version",
                "features",
                "rust_version",
                "rust_versions",
            ],
        )?;
        let url = kw.git.ok_or_else(|| {
            syn::Error::new(variant.span(), "dev!: git form requires `git = \"<url>\"`")
        })?;
        let rev = kw.rev.ok_or_else(|| {
            syn::Error::new(variant.span(), "dev!: git form requires `rev = \"<sha>\"`")
        })?;
        let dockerfile = kw.dockerfile.ok_or_else(|| {
            syn::Error::new(variant.span(), "dev!: git form requires `dockerfile = \"<path>\"`")
        })?;
        Ok(DevArgs {
            variant: DevVariant { category, variant },
            source: DevSourceArg::Git { url, rev, dockerfile, context: kw.context },
            version: kw.version,
            features: kw.features,
            rust_version: kw.rust_version,
            rust_versions: kw.rust_versions,
        })
    }
}

/// Accumulates `key = value` arguments for the `dev!` forms. Each key is
/// optional and recognized against an allow-list; `features` takes a `[...]`
/// array, the rest take a string literal.
#[derive(Default)]
struct KwArgs {
    git: Option<LitStr>,
    rev: Option<LitStr>,
    dockerfile: Option<LitStr>,
    context: Option<LitStr>,
    version: Option<LitStr>,
    features: Option<Vec<LitStr>>,
    /// Singular `rust_version = "x"`: pin one toolchain.
    rust_version: Option<LitStr>,
    /// Plural `rust_versions = <expr>`: the build-set, already lowered to the
    /// tokens for the `&'static [&'static str]` decl field (a bracket list is
    /// wrapped `&[…]`; a bare path/const is passed through).
    rust_versions: Option<proc_macro2::TokenStream>,
}

impl KwArgs {
    /// Parse `, key = value` pairs that follow a positional argument, until the
    /// input is exhausted. A leading comma is expected before each pair.
    fn parse_trailing(&mut self, input: ParseStream, allowed: &[&str]) -> syn::Result<()> {
        while input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }
            self.parse_one(input, allowed)?;
        }
        Ok(())
    }

    /// Parse a comma-separated list of `key = value` pairs (no leading comma).
    fn parse_all(&mut self, input: ParseStream, allowed: &[&str]) -> syn::Result<()> {
        loop {
            self.parse_one(input, allowed)?;
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
                if input.is_empty() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn parse_one(&mut self, input: ParseStream, allowed: &[&str]) -> syn::Result<()> {
        let key: syn::Ident = input.parse()?;
        let key_s = key.to_string();
        if !allowed.contains(&key_s.as_str()) {
            return Err(syn::Error::new(
                key.span(),
                format!("dev!: unexpected key `{key_s}`; allowed here: {}", allowed.join(", ")),
            ));
        }
        let _: Token![=] = input.parse()?;
        if key_s == "features" {
            let content;
            syn::bracketed!(content in input);
            let list = content.parse_terminated(<LitStr as Parse>::parse, Token![,])?;
            self.features = Some(list.into_iter().collect());
            return Ok(());
        }
        if key_s == "rust_versions" {
            // Either a literal `["1.88", …]` (lower to a `&[…]` slice) or a bare
            // path/const like `RUSTS` (a `&[&str]`), passed through as-is.
            if input.peek(syn::token::Bracket) {
                let content;
                syn::bracketed!(content in input);
                let list = content.parse_terminated(<LitStr as Parse>::parse, Token![,])?;
                let lits: Vec<LitStr> = list.into_iter().collect();
                self.rust_versions = Some(quote! { &[ #( #lits ),* ] });
            } else {
                let expr: syn::Expr = input.parse()?;
                self.rust_versions = Some(quote! { #expr });
            }
            return Ok(());
        }
        let val: LitStr = input.parse()?;
        match key_s.as_str() {
            "git" => self.git = Some(val),
            "rev" => self.rev = Some(val),
            "dockerfile" => self.dockerfile = Some(val),
            "context" => self.context = Some(val),
            "version" => self.version = Some(val),
            "rust_version" => self.rust_version = Some(val),
            _ => unreachable!("checked against allow-list above"),
        }
        Ok(())
    }
}

/// Set by `docker/runner.Dockerfile` for the pod-side compile
const RUNNER_BUILD: &str = "ZTEST_RUNNER_BUILD";

/// `dev!` local path: `checked` on the laptop, bare join in the runner build
///
/// - consumed laptop-side only (re-homed + built there), so a sibling repo absent from the
///   shipped source must not fail the pod compile
fn dev_local(
    rel: &LitStr,
    checked: fn(&LitStr) -> Result<std::path::PathBuf, syn::Error>,
) -> Result<std::path::PathBuf, syn::Error> {
    if std::env::var_os(RUNNER_BUILD).is_none() {
        return checked(rel);
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(rel.span(), "CARGO_MANIFEST_DIR not set; cannot resolve dev! path")
    })?;
    Ok(std::path::Path::new(&manifest).join(rel.value()))
}

fn resolve_dir(rel: &LitStr) -> Result<std::path::PathBuf, syn::Error> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(rel.span(), "CARGO_MANIFEST_DIR not set; cannot resolve dev! context")
    })?;
    let p = std::path::Path::new(&manifest).join(rel.value());
    if !p.exists() {
        return Err(syn::Error::new(
            rel.span(),
            format!("dev! context does not exist: {}", p.display()),
        ));
    }
    if !p.is_dir() {
        return Err(syn::Error::new(
            rel.span(),
            format!("dev! context is not a directory: {}", p.display()),
        ));
    }
    Ok(p)
}

// ─────────────────────── typed resource handles ───────────────────────
//
// - `artifact!("snapshots/….toml")` = declaration only (`const` → undeclared = compile error)
// - `#[ztest::needs(CONST)]` = `SeedDecl` (provisionable) + `TestDepDecl` (bound to this test)

/// The `inventory::submit!` for one test→resource edge. `resource` is a const
/// expression yielding the archive's OID — the same identity the paired
/// `SeedDecl` carries, so both resolve to one node in the resource graph.
fn test_dep_submit(
    fn_ident: &syn::Ident,
    resource: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::macro_support::TestDepDecl {
                test_id: concat!(module_path!(), "::", stringify!(#fn_ident)),
                resource: #resource,
            }
        }
    }
}

// ───────────────────────── qos tier attributes ────────────────────────

/// `#[ztest::qos::integration]` — declare a test's quality-of-service tier.
///
/// The tier attributes (`integration`, `wallet`, `testnet`, `sync`)
/// wrap a test, re-emit it intact (preserving any inner `#[tokio::test]` etc.),
/// and inject two things — mirroring the `dev!` → inventory pattern:
///   1. an `inventory::submit!` of a `ztest::inventory::QosDecl` so
///      `ztest run` can group selected tests by tier (the out-of-process
///      bridge, dumped via `ZTEST_DUMP_INVENTORY`);
///   2. a `::ztest::macro_support::__enter(class)` first statement so the runtime can
///      read the tier in `TestEnv::build()` (the in-process bridge).
///
/// One optional argument, `footprint = "15c/29Gi[/400Gi]"`: this test's exact component
/// reserve (= Σ pod requests; tier still supplies pool/hard cap). Omitted → tier default, and
/// pods size themselves from `qos::pod` unless they call `.resources(..)`.
///
/// `sync` has no default ceiling, so there it is required rather than optional.
#[proc_macro_attribute]
pub fn integration(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Integration", attr, item)
}

/// `#[ztest::qos::wallet]` — see [`integration`].
#[proc_macro_attribute]
pub fn wallet(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Wallet", attr, item)
}

/// `#[ztest::qos::testnet]` — see [`integration`].
#[proc_macro_attribute]
pub fn testnet(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Testnet", attr, item)
}

/// `#[ztest::qos::sync]` — see [`integration`]. `footprint = ".."` is required here.
#[proc_macro_attribute]
pub fn sync(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Sync", attr, item)
}

/// Parsed `footprint = ".."` → the const the inventory decl stores *and* the
/// `__enter` override; one emitter so the two can never disagree
///
/// - Integers, not the string (CLI never re-parses what the macro validated)
fn footprint_resources_tokens(f: Option<ztest_attr::Footprint>) -> proc_macro2::TokenStream {
    match f {
        Some(f) => {
            // Unwrapped here: `quote!` renders `Option<u64>` by its `ToTokens`, which emits
            // *nothing* for `None` — `.with_disk()` with no argument, at the call site
            let (cpu, mem) = (f.cpu_milli, f.mem_bytes);
            let disk = f.disk_bytes.unwrap_or(0);
            quote! {
                ::core::option::Option::Some(
                    ::ztest::qos::Resources::new(#cpu, #mem, 0, 0).with_disk(#disk)
                )
            }
        }
        None => quote! { ::core::option::Option::None },
    }
}

/// Shared body of the four tier attributes. `variant` is the [`QosClass`]
/// variant ident (`"Integration"` …).
fn qos_attr(variant: &str, attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<ztest_attr::QosAttrArgs>(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let mut func = match syn::parse::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    // `sync` alone carries no tier default, so the runtime would have nothing to reserve.
    // Caught here rather than at `profile_with`, which fires mid-run on the cluster
    if variant == "Sync" && args.footprint.is_none() {
        return syn::Error::new(
            Span::call_site(),
            "the `sync` tier has no default reserve — declare one, e.g. \
             `#[ztest::qos::sync(footprint = \"15c/29Gi\")]`",
        )
        .to_compile_error()
        .into();
    }

    let variant = syn::Ident::new(variant, Span::call_site());
    let ident = &func.sig.ident;
    let footprint_res = footprint_resources_tokens(args.footprint);

    // (b) in-process bridge: set the task-local tier + override as the first
    // statement, before any `.await` can migrate the test future across threads.
    let enter: syn::Stmt = syn::parse_quote! {
        ::ztest::macro_support::__enter(::ztest::qos::QosClass::#variant, #footprint_res);
    };
    func.block.stmts.insert(0, enter);

    // (a) out-of-process bridge: register the tier in the link-time inventory.
    // `concat!(module_path!(), "::", stringify!(name))` is const-evaluable, so
    // it satisfies `submit!`'s static initializer.
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::macro_support::QosDecl {
                test_id: concat!(module_path!(), "::", stringify!(#ident)),
                class: ::ztest::qos::QosClass::#variant,
                footprint: #footprint_res,
            }
        }
        #func
    }
    .into()
}

// ─────────────────────────── sync_test attribute ──────────────────────

/// Static metadata parsed from `#[ztest::sync_test(...)]`. `name`/`subject`/
/// `qos` are required; `description` defaults to empty, `timeout` to `"48h"`,
/// `tags` to none.
///
/// Grammar in `ztest_attr`: second reader = `ztest sync`'s source scan (two parsers would drift,
/// and a drifting scanner rejects valid profiles).
use ztest_attr::SyncTestArgs;

/// `#[ztest::sync_test(name = "..", subject = wallet, qos = sync, ..)]` on
/// `async fn(mut run: SyncRunner) -> SyncOutcome`.
///
/// Emits two things (mirroring the QoS attribute's out-of-process bridge):
/// 1. an `inventory::submit!` of a `ztest::inventory::SyncTestDecl`
///    carrying the static metadata, so `ztest sync list`/`describe` and QoS
///    admission can see the profile without running its body;
/// 2. a `#[tokio::test]` wrapper that constructs a `SyncRunner`, runs the body,
///    and asserts the outcome passed.
///
/// The wrapper is a real libtest test only because that is how `ztest sync
/// start` invokes it in the detached pod (`<bin> --exact <test>`), and it is the
/// *only* thing that runs it: `ztest run` subtracts every profile from its
/// selection using the declaration in (1). A sync profile has a 48 h cap and a
/// PVC-backed datadir, so the engine is not its lifecycle owner.
#[proc_macro_attribute]
pub fn sync_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as SyncTestArgs);
    let mut body_fn = match syn::parse::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    let test_ident = body_fn.sig.ident.clone();
    // Nest the author's body under a fixed name inside the generated wrapper.
    body_fn.sig.ident = syn::Ident::new("__ztest_sync_body", test_ident.span());

    let SyncTestArgs { name, description, subject, timeout, qos, footprint, tags } = args;
    // Same rule as the bare `#[ztest::qos::sync]` attribute: the tier reserves nothing by
    // default, so a profile riding it must price its own topology
    if qos == "sync" && footprint.is_none() {
        return syn::Error::new(
            qos.span(),
            "`qos = sync` has no default reserve — declare one, e.g. \
             `footprint = \"15c/29Gi\"`",
        )
        .to_compile_error()
        .into();
    }
    let subject_str = LitStr::new(&subject.to_string(), subject.span());
    let qos_str = LitStr::new(&qos.to_string(), qos.span());
    let footprint_res = footprint_resources_tokens(footprint);
    // The tier ident (`sync`) → the `QosClass` variant (`Sync`), so the wrapper
    // can enter the tier at runtime exactly as `#[ztest::qos::*]` does. Without
    // this the in-pod `TestEnv` would size the topology's component pods at the
    // default `Basic` tier — not the sync tier the profile declared.
    let qos_variant = {
        let s = qos.to_string();
        let mut chars = s.chars();
        let capitalized = match chars.next() {
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            None => s,
        };
        syn::Ident::new(&capitalized, qos.span())
    };

    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::macro_support::SyncTestDecl {
                test_id: concat!(module_path!(), "::", stringify!(#test_ident)),
                name: #name,
                description: #description,
                subject: #subject_str,
                timeout: #timeout,
                qos: #qos_str,
                footprint: #footprint_res,
                tags: &[#(#tags),*],
            }
        }

        #[::tokio::test(flavor = "multi_thread")]
        async fn #test_ident() {
            // Enter the declared tier before any `.await` (mirrors the
            // `#[ztest::qos::*]` attribute) so `TestEnv::build()` sizes the
            // topology at this profile's tier, whether run by the CI engine or
            // detached via `ztest sync start`.
            ::ztest::macro_support::__enter(::ztest::qos::QosClass::#qos_variant, #footprint_res);
            #body_fn
            let __outcome = __ztest_sync_body(::ztest::sync::SyncRunner::new()).await;
            assert!(
                __outcome.verdict.is_pass(),
                "sync_test failed: {:?}",
                __outcome
            );
        }
    }
    .into()
}

// ───────────────────── snapshot manifests ─────────────────────
//
// - `snapshots/<network>/*.toml`, written by `ztest snapshot push`, committed (bytes never are)
// - Keys: `name`, `sha256` (= oid of SHA256SUMS), `size_bytes`, `base_uri`, `key_prefix`

/// Missing field = producer bug → name the file and the field, never default
fn manifest_int(
    table: &toml::Value,
    key: &str,
    manifest: &std::path::Path,
    span: Span,
) -> Result<u64, syn::Error> {
    table.get(key).and_then(toml::Value::as_integer).and_then(|v| u64::try_from(v).ok()).ok_or_else(
        || {
            syn::Error::new(
                span,
                format!(
                    "manifest {} is missing a non-negative integer `{key}`",
                    manifest.display()
                ),
            )
        },
    )
}

/// Read `key` from `table` as a string, with the same contract as
/// [`manifest_int`].
fn manifest_str(
    table: &toml::Value,
    key: &str,
    manifest: &std::path::Path,
    span: Span,
) -> Result<String, syn::Error> {
    table.get(key).and_then(toml::Value::as_str).map(str::to_owned).ok_or_else(|| {
        syn::Error::new(
            span,
            format!("manifest {} is missing a string `{key}`", manifest.display()),
        )
    })
}

/// `artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml")` — bake a manifest into an
/// [`Artifact`](ztest::Artifact) expression.
///
/// Reads the manifest at expansion time, so a checkout holding none of the snapshot bytes
/// still compiles.
#[proc_macro]
pub fn artifact(input: TokenStream) -> TokenStream {
    let source = parse_macro_input!(input as LitStr);
    let abs = match resolve_source(&source) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    match bake_artifact(&abs, source.span()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// The keys a manifest carries, as an `Artifact` literal.
fn bake_artifact(
    manifest_abs: &std::path::Path,
    span: Span,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let text = std::fs::read_to_string(manifest_abs).map_err(|e| {
        syn::Error::new(span, format!("reading manifest {}: {e}", manifest_abs.display()))
    })?;
    let doc: toml::Value = toml::from_str(&text).map_err(|e| {
        syn::Error::new(span, format!("manifest {} is not valid TOML: {e}", manifest_abs.display()))
    })?;
    let name = manifest_str(&doc, "name", manifest_abs, span)?;
    if doc.get("uncompressed_bytes").is_some() || name.ends_with(".tar.zst") {
        return Err(syn::Error::new(
            span,
            format!(
                "manifest {} predates the rclone format; republish with `ztest snapshot push`",
                manifest_abs.display()
            ),
        ));
    }
    let oid = manifest_str(&doc, "sha256", manifest_abs, span)?;
    if oid.len() != 64 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(syn::Error::new(
            span,
            format!(
                "manifest {} records sha256 = {oid:?}, which is not a 64-character hex digest",
                manifest_abs.display()
            ),
        ));
    }
    let oid = oid.to_ascii_lowercase();
    let size = manifest_int(&doc, "size_bytes", manifest_abs, span)?;
    let base_uri = manifest_str(&doc, "base_uri", manifest_abs, span)?;
    let key_prefix = manifest_str(&doc, "key_prefix", manifest_abs, span)?;
    Ok(quote! {
        ::ztest::Artifact {
            name: #name,
            oid: #oid,
            size: #size,
            base_uri: #base_uri,
            key_prefix: #key_prefix,
        }
    })
}

/// `#[ztest::needs(SNAPSHOT)]` — depend on a snapshot declared out of line.
///
/// Submits the `SeedDecl` (preflight pre-provisions the seed) and the `TestDepDecl` binding it
/// to *this* test (`ztest run` SKIPs only the tests whose seed failed). All const expressions.
#[proc_macro_attribute]
pub fn needs(attr: TokenStream, item: TokenStream) -> TokenStream {
    let handle = parse_macro_input!(attr as syn::Path);
    let func = match syn::parse::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let ident = func.sig.ident.clone();
    let dep = test_dep_submit(&ident, &quote! { #handle.artifact.oid });
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::macro_support::SeedDecl {
                name: #handle.artifact.name,
                oid: #handle.artifact.oid,
                size: #handle.artifact.size,
                base_uri: #handle.artifact.base_uri,
                key_prefix: #handle.artifact.key_prefix,
            }
        }
        #dep
        #func
    }
    .into()
}
