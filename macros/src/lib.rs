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

/// `dev!(Indexer::Zainod, "rel/Dockerfile" [, context = "rel/ctx"])`
///
/// Block expression returning a `Validator` / `Indexer` / `Wallet` value
/// whose container image was declared as a local-build dev image. At
/// the same call site the macro injects an `inventory::submit!` for
/// the corresponding [`DevImageDecl`], so the preflight image pipeline
/// can discover and build the image before any test runs.
///
/// The path is resolved against the caller's `CARGO_MANIFEST_DIR`
/// (same rule as the `mount_*` macros). Compile fails if the
/// Dockerfile doesn't exist or the context isn't a directory.
///
/// Supported component variants: `Validator::Zebrad`, `Validator::Zcashd`,
/// `Indexer::Zainod`, `Wallet::Zingo`. Any other path yields a compile
/// error — keeps the matrix grep-able and the test-author surface
/// small.
#[proc_macro]
pub fn dev(input: TokenStream) -> TokenStream {
    let DevArgs {
        variant,
        dockerfile,
        context,
    } = parse_macro_input!(input as DevArgs);

    // Derive the kind label from the variant name itself — lowercased.
    // `Indexer::Zainod` → `"zainod"`, `Validator::Zebrad` → `"zebrad"`, etc.
    // The lowercased form is used three ways:
    //   - as the inventory `repo:` field (becomes the local image
    //     repo name in the resolved `<repo>:dev-<hash>` tag),
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

    // Resolve the Dockerfile path relative to CARGO_MANIFEST_DIR.
    let df_abs = match resolve_source(&dockerfile) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };

    // Context: caller-provided or the Dockerfile's parent dir.
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
    let repo_lit = kind_str.to_string();
    let feat_lits: Vec<String> = default_features.into_iter().map(String::from).collect();

    let category_ident = &variant.category;
    let ctor_ident = syn::Ident::new(&format!("{kind_str}_dev"), variant.variant.span());

    quote! {
        {
            ::ztest::__private::inventory::submit! {
                ::ztest::inventory::DevImageDecl {
                    repo: #repo_lit,
                    dockerfile: #df_lit,
                    context: #ctx_lit,
                    features: &[ #( #feat_lits ),* ],
                }
            }
            ::ztest::#category_ident::#ctor_ident(
                ::std::path::PathBuf::from(#df_lit),
                ::std::path::PathBuf::from(#ctx_lit),
            )
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

struct DevArgs {
    variant: DevVariant,
    dockerfile: LitStr,
    context: Option<LitStr>,
}

impl Parse for DevArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let category: syn::Ident = input.parse()?;
        let _: Token![::] = input.parse()?;
        let variant: syn::Ident = input.parse()?;
        let _: Token![,] = input.parse()?;
        let dockerfile: LitStr = input.parse()?;
        let context = if input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            if input.is_empty() {
                None
            } else {
                let key: syn::Ident = input.parse()?;
                if key != "context" {
                    return Err(syn::Error::new(
                        key.span(),
                        "dev!: only `context = \"…\"` is recognized after the dockerfile",
                    ));
                }
                let _: Token![=] = input.parse()?;
                let ctx: LitStr = input.parse()?;
                let _ = input.parse::<Option<Token![,]>>();
                Some(ctx)
            }
        } else {
            None
        };
        Ok(DevArgs {
            variant: DevVariant { category, variant },
            dockerfile,
            context,
        })
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
