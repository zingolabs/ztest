//! `#[ztest::sync_test(..)]` grammar.
//!
//! - One parser, two readers: `ztest_macros::sync_test` (expands) + `ztest::pipeline::profiles`
//!   (scans source pre-compile)
//! - Drift → scanner rejects valid profiles, so grammar lives here (drift = compile error)
//! - Plain lib, not `proc-macro = true` (proc-macro crate unlinkable from the CLI)

pub mod footprint;

pub use footprint::Footprint;

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::{LitStr, Token};

pub const SUBJECTS: [&str; 3] = ["wallet", "indexer", "validator"];

pub const DEFAULT_TIMEOUT: &str = "48h";

/// Parsed `#[ztest::sync_test(..)]` args (`syn` types kept — macro re-splices with original spans).
///
/// - `footprint` parsed here, not at expansion (malformed → compile error on the literal)
pub struct SyncTestArgs {
    pub name: LitStr,
    pub description: LitStr,
    pub subject: syn::Ident,
    pub timeout: LitStr,
    pub qos: syn::Ident,
    pub footprint: Option<Footprint>,
    pub tags: Vec<LitStr>,
}

impl Parse for SyncTestArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name: Option<LitStr> = None;
        let mut description: Option<LitStr> = None;
        let mut subject: Option<syn::Ident> = None;
        let mut timeout: Option<LitStr> = None;
        let mut qos: Option<syn::Ident> = None;
        let mut footprint: Option<Footprint> = None;
        let mut tags: Vec<LitStr> = Vec::new();

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "description" => description = Some(input.parse()?),
                "timeout" => timeout = Some(input.parse()?),
                "subject" => subject = Some(input.parse()?),
                "qos" => qos = Some(input.parse()?),
                "footprint" => {
                    let lit: LitStr = input.parse()?;
                    footprint = Some(
                        footprint::parse(&lit.value())
                            .map_err(|why| syn::Error::new(lit.span(), why))?,
                    );
                }
                "tags" => {
                    let content;
                    syn::bracketed!(content in input);
                    let items = content.parse_terminated(<LitStr as Parse>::parse, Token![,])?;
                    tags = items.into_iter().collect();
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown sync_test key `{other}` \
                             (expected name/description/subject/timeout/qos/footprint/tags)"
                        ),
                    ));
                }
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }

        let name = name.ok_or_else(|| {
            syn::Error::new(Span::call_site(), "sync_test requires `name = \"..\"`")
        })?;
        let subject = subject.ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "sync_test requires `subject = wallet|indexer|validator`",
            )
        })?;
        let qos = qos.ok_or_else(|| {
            syn::Error::new(Span::call_site(), "sync_test requires `qos = <tier>`")
        })?;
        let subj = subject.to_string();
        if !SUBJECTS.contains(&subj.as_str()) {
            return Err(syn::Error::new(
                subject.span(),
                "sync_test `subject` must be wallet, indexer, or validator",
            ));
        }
        Ok(SyncTestArgs {
            name,
            description: description.unwrap_or_else(|| LitStr::new("", Span::call_site())),
            subject,
            timeout: timeout.unwrap_or_else(|| LitStr::new(DEFAULT_TIMEOUT, Span::call_site())),
            qos,
            footprint,
            tags,
        })
    }
}

/// Parsed `#[ztest::qos::<tier>(..)]` args; sole key = `footprint`, same grammar as
/// [`SyncTestArgs`]
#[derive(Default)]
pub struct QosAttrArgs {
    pub footprint: Option<Footprint>,
}

impl Parse for QosAttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut footprint: Option<Footprint> = None;
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "footprint" => {
                    let lit: LitStr = input.parse()?;
                    footprint = Some(
                        footprint::parse(&lit.value())
                            .map_err(|why| syn::Error::new(lit.span(), why))?,
                    );
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown qos tier key `{other}` (expected `footprint = \"15c/29Gi\"`)"
                        ),
                    ));
                }
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }
        Ok(QosAttrArgs { footprint })
    }
}

/// - `#[sync_test]` / `#[ztest::sync_test]` / `#[::ztest::sync_test]` all reach the macro
/// - Qualified forms pinned to leading `ztest` (bare trailing match = another crate's `sync_test`)
pub fn is_sync_test_path(path: &syn::Path) -> bool {
    let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    match segs.as_slice() {
        [one] => one == "sync_test",
        [krate, .., last] => krate == "ztest" && last == "sync_test",
        [] => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(s: &str) -> syn::Path {
        syn::parse_str(s).expect("test path parses")
    }

    #[test]
    fn recognizes_every_spelling_that_reaches_the_macro() {
        assert!(is_sync_test_path(&path("sync_test")));
        assert!(is_sync_test_path(&path("ztest::sync_test")));
        assert!(is_sync_test_path(&path("::ztest::sync_test")));
    }

    #[test]
    fn rejects_another_crates_attribute_of_the_same_name() {
        assert!(!is_sync_test_path(&path("other::sync_test")));
        assert!(!is_sync_test_path(&path("ztest::qos::basic")));
        assert!(!is_sync_test_path(&path("tokio::test")));
    }

    #[test]
    fn parses_a_full_declaration() {
        let args: SyncTestArgs = syn::parse_str(
            r#"name = "p", description = "d", subject = indexer, timeout = "12h",
               qos = sync, tags = ["a", "b"]"#,
        )
        .expect("declaration parses in test");
        assert_eq!(args.name.value(), "p");
        assert_eq!(args.description.value(), "d");
        assert_eq!(args.subject.to_string(), "indexer");
        assert_eq!(args.timeout.value(), "12h");
        assert_eq!(args.qos.to_string(), "sync");
        let tags: Vec<String> = args.tags.iter().map(LitStr::value).collect();
        assert_eq!(tags, ["a", "b"]);
    }

    #[test]
    fn applies_the_documented_defaults() {
        let args: SyncTestArgs =
            syn::parse_str(r#"name = "p", subject = wallet, qos = sync"#).expect("parses");
        assert_eq!(args.timeout.value(), DEFAULT_TIMEOUT);
        assert_eq!(args.description.value(), "");
        assert!(args.tags.is_empty());
    }

    #[test]
    fn rejects_a_declaration_missing_a_required_key() {
        assert!(syn::parse_str::<SyncTestArgs>(r#"subject = wallet, qos = sync"#).is_err());
        assert!(syn::parse_str::<SyncTestArgs>(r#"name = "p", qos = sync"#).is_err());
        assert!(syn::parse_str::<SyncTestArgs>(r#"name = "p", subject = wallet"#).is_err());
    }

    // Malformed override must fail at the literal (else reaches inventory as a bad reserve)

    #[test]
    fn parses_an_optional_footprint_override() {
        let args: SyncTestArgs =
            syn::parse_str(r#"name = "p", subject = indexer, qos = sync, footprint = "15c/29Gi""#)
                .expect("parses");
        assert_eq!(
            args.footprint,
            Some(Footprint { cpu_milli: 15_000, mem_bytes: 29 * 1024 * 1024 * 1024 })
        );
    }

    #[test]
    fn omitting_the_footprint_leaves_the_tier_reserve_alone() {
        let args: SyncTestArgs =
            syn::parse_str(r#"name = "p", subject = wallet, qos = sync"#).expect("parses");
        assert!(args.footprint.is_none());
    }

    #[test]
    fn rejects_a_malformed_footprint() {
        for bad in ["29Gi", "15c/29", "0c/1Gi", "1500m/2Gi"] {
            let src = format!(r#"name = "p", subject = wallet, qos = sync, footprint = "{bad}""#);
            assert!(
                syn::parse_str::<SyncTestArgs>(&src).is_err(),
                "footprint `{bad}` must not parse"
            );
        }
    }

    #[test]
    fn rejects_an_unknown_subject() {
        assert!(
            syn::parse_str::<SyncTestArgs>(r#"name = "p", subject = router, qos = sync"#).is_err()
        );
    }
}
