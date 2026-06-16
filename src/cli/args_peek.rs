//! Peek-parse the verbatim `cargo nextest run` arg vector.
//!
//! `ztest run` forwards its trailing argv to `cargo nextest run`
//! untouched (see [`crate::cli::run`]). But to make preflight useful,
//! ztest also needs to *read* a few of those args without disturbing
//! the passthrough — e.g. the configured test-threads count, the
//! nextest profile, the filter expression.
//!
//! [`peek`] does exactly that read. It is deliberately *partial* —
//! it knows only the flags ztest itself cares about and ignores
//! everything else, including flags whose argument-consumption rules
//! we can't determine without a full nextest flag table. Best-effort,
//! ambiguity-resilient: a missed value never breaks the run, it just
//! results in a less-informative banner.
//!
//! The output is a borrowed view; no allocations beyond the captured
//! values themselves.

/// View into the args ztest cares about for preflight purposes.
#[derive(Debug, Default, Clone)]
pub struct NextestArgs {
    /// `--profile NAME` / `--profile=NAME`.
    pub profile: Option<String>,
    /// `--test-threads N` / `--test-threads=N` / `-j N` / `-j=N`.
    /// `num-cpus` and negative values (nextest extension) parse to
    /// `None`; only positive integers are returned.
    pub test_threads: Option<u32>,
    /// `-E EXPR` / `--filter-expr EXPR` / `--filter-expr=EXPR`.
    pub filter_expr: Option<String>,
}

/// Flags that take a value as the next argv element.
const VALUE_FLAGS: &[&str] = &[
    "--profile",
    "--test-threads",
    "-j",
    "-E",
    "--filter-expr",
];

/// Walk the arg vector, capturing known flag values.
///
/// Conservative: only [`VALUE_FLAGS`] are known to consume the next
/// element. Any other flag is treated as standalone; this is wrong
/// for nextest flags we don't list (e.g. `--skip`), but for those
/// cases we just lose the value, never misclassify it as a known
/// flag's value.
pub fn peek(args: &[String]) -> NextestArgs {
    let mut out = NextestArgs::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        // Stop scanning at `--`; remaining args are filter positionals,
        // none of which match our flag table.
        if arg == "--" {
            break;
        }

        // `--flag=value` short-circuit.
        if let Some((flag, value)) = arg.split_once('=') {
            apply(&mut out, flag, value);
            continue;
        }

        // `--flag value` lookahead — only consume the next element
        // when the flag is one we know takes a value.
        if VALUE_FLAGS.contains(&arg.as_str())
            && let Some(value) = iter.next()
        {
            apply(&mut out, arg, value);
        }
    }
    out
}

fn apply(out: &mut NextestArgs, flag: &str, value: &str) {
    match flag {
        "--profile" => out.profile = Some(value.to_string()),
        "--test-threads" | "-j" => {
            out.test_threads = value.parse::<u32>().ok();
        }
        "-E" | "--filter-expr" => out.filter_expr = Some(value.to_string()),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn captures_long_value_pair() {
        let p = peek(&v(&["--profile", "ci", "--test-threads", "6"]));
        assert_eq!(p.profile.as_deref(), Some("ci"));
        assert_eq!(p.test_threads, Some(6));
    }

    #[test]
    fn captures_long_equal_form() {
        let p = peek(&v(&["--profile=ci", "--test-threads=6"]));
        assert_eq!(p.profile.as_deref(), Some("ci"));
        assert_eq!(p.test_threads, Some(6));
    }

    #[test]
    fn captures_short_filter_and_threads() {
        let p = peek(&v(&["-E", "test(reorg)", "-j", "4"]));
        assert_eq!(p.filter_expr.as_deref(), Some("test(reorg)"));
        assert_eq!(p.test_threads, Some(4));
    }

    #[test]
    fn ignores_unknown_flags() {
        let p = peek(&v(&[
            "--no-fail-fast",
            "--profile",
            "ci",
            "--retries",
            "2",
        ]));
        assert_eq!(p.profile.as_deref(), Some("ci"));
        assert!(p.filter_expr.is_none());
        assert!(p.test_threads.is_none());
    }

    #[test]
    fn stops_at_double_dash() {
        let p = peek(&v(&[
            "--profile",
            "ci",
            "--",
            "--test-threads",
            "999",
            "-E",
            "ignored",
        ]));
        assert_eq!(p.profile.as_deref(), Some("ci"));
        // Args after `--` are positional filter substrings, not flags.
        assert!(p.test_threads.is_none());
        assert!(p.filter_expr.is_none());
    }

    #[test]
    fn non_numeric_test_threads_yields_none() {
        let p = peek(&v(&["--test-threads", "num-cpus"]));
        assert!(p.test_threads.is_none());
    }

    #[test]
    fn empty_args_yields_default() {
        let p = peek(&v(&[]));
        assert!(p.profile.is_none());
        assert!(p.test_threads.is_none());
        assert!(p.filter_expr.is_none());
    }
}
