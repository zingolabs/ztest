//! Compile-time mount macros for `ztest`.
//!
//! Each macro takes `(relative_source, container_destination)` and:
//! - resolves the source against `CARGO_MANIFEST_DIR` of the *invoking* crate,
//! - asserts the file exists at compile time (`compile_error!` otherwise),
//! - for `mount_config!`, additionally asserts UTF-8 and size `< 1 MiB`,
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
        Ok(MountArgs {
            source,
            destination,
        })
    }
}

fn resolve_source(rel: &LitStr) -> Result<std::path::PathBuf, syn::Error> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            rel.span(),
            "CARGO_MANIFEST_DIR not set; cannot resolve mount source",
        )
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

fn emit(
    source_abs: &std::path::Path,
    destination: &LitStr,
    kind_ident: &str,
    source_variant: &str,
    seed_payload: Option<&str>,
) -> proc_macro2::TokenStream {
    let abs = source_abs.to_string_lossy().into_owned();
    let dst = destination.value();
    let kind = syn::Ident::new(kind_ident, Span::call_site());
    let src_variant = syn::Ident::new(source_variant, Span::call_site());
    let mount = quote! {
        ::ztest::Mount {
            source: ::ztest::MountSource::#src_variant(
                ::std::path::PathBuf::from(#abs),
            ),
            destination: ::std::path::PathBuf::from(#dst),
            kind: ::ztest::MountKind::#kind,
        }
    };
    // For PVC-backed mounts (archive/file), also register a static `SeedDecl` in
    // the link-time inventory — same pattern as `dev!`. This lets the preflight
    // resource graph pre-provision the content-addressed seed before any test
    // runs (`materialize::ensure_seed`), instead of the first test materializing
    // it lazily at `build()`. The author writes `mount_archive!` exactly as
    // before; the declaration is invisible. `ConfigMap` mounts have no seed, so
    // they pass `None` and emit the bare value.
    match seed_payload {
        Some(payload) => {
            let payload = syn::Ident::new(payload, Span::call_site());
            quote! {
                {
                    ::ztest::__private::inventory::submit! {
                        ::ztest::inventory::SeedDecl {
                            source: #abs,
                            payload: ::ztest::inventory::SeedPayload::#payload,
                        }
                    }
                    #mount
                }
            }
        }
        None => mount,
    }
}

/// `mount_config!("rel/path.toml", "/etc/foo/foo.toml")`
///
/// Becomes a `ConfigMap`-backed mount. Compile-time checks: file exists,
/// is valid UTF-8, and is `< 1 MiB`.
#[proc_macro]
pub fn mount_config(input: TokenStream) -> TokenStream {
    let MountArgs {
        source,
        destination,
    } = parse_macro_input!(input as MountArgs);
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
            format!(
                "mount_config! requires UTF-8 source; {} is not UTF-8",
                abs.display()
            ),
        )
        .to_compile_error()
        .into();
    }
    emit(&abs, &destination, "Config", "ConfigAbs", None).into()
}

/// `mount_file!("rel/blob.bin", "/path/in/container")`
///
/// Materializes as a content-addressed single-file PVC. Compile-time check:
/// file exists.
#[proc_macro]
pub fn mount_file(input: TokenStream) -> TokenStream {
    let MountArgs {
        source,
        destination,
    } = parse_macro_input!(input as MountArgs);
    let abs = match resolve_source(&source) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    emit(&abs, &destination, "File", "FileAbs", Some("File")).into()
}

/// `mount_archive!("rel/data.tar.zst", "/data")`
///
/// Materializes as a content-addressed extracted-tar PVC (CoW clone per use).
/// Compile-time check: file exists. `.tar.zst` suffix recommended.
#[proc_macro]
pub fn mount_archive(input: TokenStream) -> TokenStream {
    let MountArgs {
        source,
        destination,
    } = parse_macro_input!(input as MountArgs);
    let abs = match resolve_source(&source) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    emit(
        &abs,
        &destination,
        "DirArchive",
        "ArchiveAbs",
        Some("Archive"),
    )
    .into()
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
/// injects an `inventory::submit!` for the corresponding [`DevImageDecl`], so
/// the preflight image pipeline can discover and build the image before any
/// test runs. `version` names the release a build corresponds to for backends
/// (zebra) that render config / derive a ceiling from it; it defaults to `"dev"`.
///
/// Supported component variants: `Validator::Zebrad`, `Validator::Zcashd`,
/// `Indexer::Zainod`, `Wallet::Zingo`. Any other path yields a compile
/// error — keeps the matrix grep-able and the test-author surface
/// small.
#[proc_macro]
pub fn dev(input: TokenStream) -> TokenStream {
    let DevArgs {
        variant,
        source,
        version,
        features,
        rust_version,
        rust_versions,
    } = parse_macro_input!(input as DevArgs);

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
    // The lowercased form is used three ways:
    //   - as the inventory `repo:` field (becomes the local image
    //     repo name in the resolved `<repo>:dev-<suffix>` tag),
    //   - as the constructor ident (`Indexer::zainod_dev(...)`),
    //   - keyed lookup of default cargo features below.
    let (kind_str, default_features): (String, Vec<&'static str>) = match (
        variant.category.to_string().as_str(),
        variant.variant.to_string().as_str(),
    ) {
        ("Validator", "Zebrad") => ("zebrad".to_string(), vec![]),
        ("Validator", "Zcashd") => ("zcashd".to_string(), vec![]),
        ("Indexer", "Zainod") => ("zainod".to_string(), vec!["no_tls_use_unencrypted_traffic"]),
        ("Wallet", "Zingo") => ("zingo".to_string(), vec![]),
        (cat, var) => {
            return syn::Error::new(
                variant.span(),
                format!(
                    "dev!: unsupported component variant `{cat}::{var}`; \
                     expected one of `Validator::Zebrad`, `Validator::Zcashd`, \
                     `Indexer::Zainod`, `Wallet::Zingo`"
                ),
            )
            .to_compile_error()
            .into();
        }
    };

    // Feature list: explicit override, else the per-kind default.
    let feat_lits: Vec<String> = match features {
        Some(fs) => fs.iter().map(LitStr::value).collect(),
        None => default_features.into_iter().map(String::from).collect(),
    };
    let repo_lit = kind_str.clone();
    // The release this build corresponds to (validators render config / derive
    // a ceiling from it); `"dev"` when the caller doesn't say.
    let version_lit = version
        .map(|v| v.value())
        .unwrap_or_else(|| "dev".to_string());

    // Per source form, build the static `DevSourceDecl` (inventory) and the
    // owned `DevSource` (constructor arg). Local paths resolve against
    // `CARGO_MANIFEST_DIR` at compile time; git paths stay repo-relative (the
    // pipeline resolves them against the fetched checkout).
    let (decl_source, ctor_source) = match source {
        DevSourceArg::Local {
            dockerfile,
            context,
        } => {
            let df_abs = match resolve_source(&dockerfile) {
                Ok(p) => p,
                Err(e) => return e.to_compile_error().into(),
            };
            let ctx_abs = match context {
                Some(c) => match resolve_dir(&c) {
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
                    ::ztest::inventory::DevSourceDecl::Local {
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
        DevSourceArg::Git {
            url,
            rev,
            dockerfile,
            context,
        } => {
            let url_s = url.value();
            let rev_s = rev.value();
            let df_s = dockerfile.value();
            let ctx_s = context.map(|c| c.value()).unwrap_or_else(|| ".".to_string());
            (
                quote! {
                    ::ztest::inventory::DevSourceDecl::Git {
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
                ::ztest::inventory::DevImageDecl {
                    repo: #repo_lit,
                    source: #decl_source,
                    features: &[ #( #feat_lits ),* ],
                    rust_versions: #rust_versions_tokens,
                }
            }
            ::ztest::#category_ident::#ctor_ident(#ctor_source, #version_lit) #rust_version_chain
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
    Local {
        dockerfile: LitStr,
        context: Option<LitStr>,
    },
    /// Keyword git form: `git = "…", rev = "…", dockerfile = "in/tree" [, context = "in/tree"]`.
    /// Paths are relative to the fetched repo root (default context `"."`).
    Git {
        url: LitStr,
        rev: LitStr,
        dockerfile: LitStr,
        context: Option<LitStr>,
    },
}

struct DevArgs {
    variant: DevVariant,
    source: DevSourceArg,
    /// The release this build corresponds to; threaded to the `_dev`
    /// constructor for backends (zebra) that render config / derive a ceiling
    /// from a version. Defaults to `"dev"`.
    version: Option<LitStr>,
    /// Cargo features override; `None` uses the per-kind default.
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
                source: DevSourceArg::Local {
                    dockerfile,
                    context: kw.context,
                },
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
        let rev = kw
            .rev
            .ok_or_else(|| syn::Error::new(variant.span(), "dev!: git form requires `rev = \"<sha>\"`"))?;
        let dockerfile = kw.dockerfile.ok_or_else(|| {
            syn::Error::new(variant.span(), "dev!: git form requires `dockerfile = \"<path>\"`")
        })?;
        Ok(DevArgs {
            variant: DevVariant { category, variant },
            source: DevSourceArg::Git {
                url,
                rev,
                dockerfile,
                context: kw.context,
            },
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

fn resolve_dir(rel: &LitStr) -> Result<std::path::PathBuf, syn::Error> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            rel.span(),
            "CARGO_MANIFEST_DIR not set; cannot resolve dev! context",
        )
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
// The sound, per-test resource-dependency surface. `#[ztest::archive(NAME =
// "path")]` on a test both (a) makes the archive provisionable (a `SeedDecl`,
// same as `mount_archive!`) and (b) records a per-test dependency edge (a
// `TestDepDecl`), so `ztest run` can pre-provision it and cleanly SKIP only the
// tests whose archive failed. It also binds a fn-local `const NAME:
// ArchiveHandle` the body passes to `Validator::with_regtest_cache` — a real
// `const`, so a typo is a compile error.
//
// When the archive is consumed through a helper (the helper can't see a fn-local
// const), the test still owns the declaration: pass the `NAME` handle into the
// helper as a value (e.g. carried by a backend enum variant), rather than
// declaring the handle out-of-line.

/// `NAME = "rel/path"` — the shared parse for the archive macros.
struct HandleDecl {
    name: syn::Ident,
    source: LitStr,
}

impl Parse for HandleDecl {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: syn::Ident = input.parse()?;
        let _: Token![=] = input.parse()?;
        let source: LitStr = input.parse()?;
        let _ = input.parse::<Option<Token![,]>>();
        Ok(HandleDecl { name, source })
    }
}

/// The `inventory::submit!` that makes an archive provisionable — a `SeedDecl`
/// with `Archive` payload, keyed by its absolute source path.
fn seed_decl_submit(abs: &str) -> proc_macro2::TokenStream {
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::inventory::SeedDecl {
                source: #abs,
                payload: ::ztest::inventory::SeedPayload::Archive,
            }
        }
    }
}

/// The `inventory::submit!` for one test→resource edge. `resource` is a const
/// expression yielding the absolute source path (a string literal for
/// `#[archive]`, or `HANDLE.source()` for `#[needs]`).
fn test_dep_submit(
    fn_ident: &syn::Ident,
    resource: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::inventory::TestDepDecl {
                test_id: concat!(module_path!(), "::", stringify!(#fn_ident)),
                resource: #resource,
            }
        }
    }
}

/// `#[ztest::archive(NAME = "rel/path.tar.gz")]` — declare + depend + bind, on one
/// test. Resolves the path against the caller's `CARGO_MANIFEST_DIR` at compile
/// time (same rule as `mount_archive!`), submits the provisionable `SeedDecl` and
/// the per-test `TestDepDecl`, and binds a fn-local `const NAME: ArchiveHandle` the
/// body passes to `with_regtest_cache`. Stacks with `#[ztest::qos::*]`.
#[proc_macro_attribute]
pub fn archive(attr: TokenStream, item: TokenStream) -> TokenStream {
    let HandleDecl { name, source } = parse_macro_input!(attr as HandleDecl);
    let abs = match resolve_source(&source) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    let abs = abs.to_string_lossy().into_owned();

    let mut func = match syn::parse::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let ident = func.sig.ident.clone();

    // fn-local typed handle, bound before the body runs. `dead_code` allowed so a
    // declared-but-unused handle is a (harmless) over-provision, not a build error.
    let bind: syn::Stmt = syn::parse_quote! {
        #[allow(dead_code)]
        const #name: ::ztest::ArchiveHandle = ::ztest::ArchiveHandle::__new(#abs);
    };
    func.block.stmts.insert(0, bind);

    let seed = seed_decl_submit(&abs);
    let dep = test_dep_submit(&ident, &quote! { #abs });
    quote! {
        #seed
        #dep
        #func
    }
    .into()
}

// ───────────────────────── qos tier attributes ────────────────────────

/// `#[ztest::qos::basic]` — declare a test's quality-of-service tier.
///
/// The four tier attributes (`basic`, `integration`, `testnet`, `sync`) wrap
/// a test, re-emit it intact (preserving any inner `#[tokio::test]` etc.), and
/// inject two things — mirroring the `dev!` → inventory pattern:
///   1. an `inventory::submit!` of a [`ztest::inventory::QosDecl`] so
///      `ztest run` can group selected tests by tier (the out-of-process
///      bridge, dumped via `ZTEST_DUMP_INVENTORY`);
///   2. a `::ztest::qos::__enter(class)` first statement so the runtime can
///      read the tier in `TestEnv::build()` (the in-process bridge).
///
/// The attribute takes no arguments.
#[proc_macro_attribute]
pub fn basic(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Basic", attr, item)
}

/// `#[ztest::qos::integration]` — see [`basic`].
#[proc_macro_attribute]
pub fn integration(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Integration", attr, item)
}

/// `#[ztest::qos::testnet]` — see [`basic`].
#[proc_macro_attribute]
pub fn testnet(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Testnet", attr, item)
}

/// `#[ztest::qos::sync]` — see [`basic`].
#[proc_macro_attribute]
pub fn sync(attr: TokenStream, item: TokenStream) -> TokenStream {
    qos_attr("Sync", attr, item)
}

/// Shared body of the four tier attributes. `variant` is the [`QosClass`]
/// variant ident (`"Basic"` …).
fn qos_attr(variant: &str, attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            Span::call_site(),
            "ztest qos tier attribute takes no arguments, e.g. `#[ztest::qos::sync]`",
        )
        .to_compile_error()
        .into();
    }
    let mut func = match syn::parse::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    let variant = syn::Ident::new(variant, Span::call_site());
    let ident = &func.sig.ident;

    // (b) in-process bridge: set the task-local tier as the first statement,
    // before any `.await` can migrate the test future across threads.
    let enter: syn::Stmt = syn::parse_quote! {
        ::ztest::qos::__enter(::ztest::qos::QosClass::#variant);
    };
    func.block.stmts.insert(0, enter);

    // (a) out-of-process bridge: register the tier in the link-time inventory.
    // `concat!(module_path!(), "::", stringify!(name))` is const-evaluable, so
    // it satisfies `submit!`'s static initializer.
    quote! {
        ::ztest::__private::inventory::submit! {
            ::ztest::inventory::QosDecl {
                test_id: concat!(module_path!(), "::", stringify!(#ident)),
                class: ::ztest::qos::QosClass::#variant,
            }
        }
        #func
    }
    .into()
}
