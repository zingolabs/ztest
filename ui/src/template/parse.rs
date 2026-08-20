//! Template source -> [`Cell`]s.
//!
//! Grammar, whole thing:
//!
//! ```text
//!   row      := ( literal | group | placeholder )*
//!   group    := '[' row ']'                     omitted when a cell inside is absent
//!   place    := '{' key spec? '}'
//!   spec     := ( ':' align? width? kind? )? ( '|' opts )?
//!   align    := '<' | '>' | '^'
//!   width    := digits | '*'                    '*' = remaining row width
//!   prec     := '.' digits                       bare fixed-point, unit as a literal
//!   kind     := '#' bar | '~' spark
//!   opts     := name ( '.' name )*              unit and/or tone, order-free
//! ```
//!
//! `{{` / `}}` / `[[` / `]]` escape a literal brace or bracket.
//!
//! `{@name}` draws a themed glyph instead of binding data — `{@spin}` for the shared
//! frame clock, any role in [`glyph_of`](super::bind::glyph_of) otherwise. A punctuation
//! mark written as a literal renders as itself on a terminal that cannot draw it, so
//! separators (`{@dot}`, `{@dash}`) and marks (`{@ok}`, `{@arrow}`) go through a cell.

use super::{Align, Cell, Tone, Width};

pub(super) fn parse(src: &str) -> Result<Vec<Cell>, String> {
    let mut p = Parser { b: src.as_bytes(), i: 0 };
    let cells = p.row(false)?;
    if p.i < p.b.len() {
        return Err(format!("unbalanced `]` at byte {}", p.i));
    }
    Ok(cells)
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    /// `nested` = inside a group, so `]` closes rather than being a literal
    fn row(&mut self, nested: bool) -> Result<Vec<Cell>, String> {
        let mut cells: Vec<Cell> = Vec::new();
        let mut lit = String::new();
        while self.i < self.b.len() {
            match self.b[self.i] {
                b']' if nested => break,
                // Doubled delimiters are the escape, so a literal `[` needs no backslash
                b'{' | b'}' | b'[' | b']' if self.peek(1) == Some(self.b[self.i]) => {
                    lit.push(self.b[self.i] as char);
                    self.i += 2;
                }
                b'{' => {
                    flush(&mut lit, &mut cells);
                    cells.push(self.placeholder()?);
                }
                b'[' => {
                    flush(&mut lit, &mut cells);
                    self.i += 1;
                    let inner = self.row(true)?;
                    if self.i >= self.b.len() {
                        return Err("unclosed `[`".into());
                    }
                    self.i += 1;
                    cells.push(Cell::Group(inner));
                }
                b'}' => return Err(format!("stray `}}` at byte {}", self.i)),
                _ => {
                    let start = self.i;
                    while self.i < self.b.len()
                        && !matches!(self.b[self.i], b'{' | b'}' | b'[' | b']')
                    {
                        self.i += 1;
                    }
                    lit.push_str(
                        std::str::from_utf8(&self.b[start..self.i]).map_err(|e| e.to_string())?,
                    );
                }
            }
        }
        flush(&mut lit, &mut cells);
        Ok(cells)
    }

    fn placeholder(&mut self) -> Result<Cell, String> {
        self.i += 1;
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != b'}' {
            self.i += 1;
        }
        if self.i >= self.b.len() {
            return Err("unclosed `{`".into());
        }
        let body = std::str::from_utf8(&self.b[start..self.i]).map_err(|e| e.to_string())?;
        self.i += 1;
        build(body)
    }

    fn peek(&self, n: usize) -> Option<u8> {
        self.b.get(self.i + n).copied()
    }
}

fn flush(lit: &mut String, cells: &mut Vec<Cell>) {
    if !lit.is_empty() {
        cells.push(Cell::Lit(std::mem::take(lit)));
    }
}

fn build(body: &str) -> Result<Cell, String> {
    let (head, opts) = match body.split_once('|') {
        Some((h, o)) => (h, Some(o)),
        None => (body, None),
    };
    let (key, spec) = match head.split_once(':') {
        Some((k, s)) => (k, Some(s)),
        // Kind marker with no width/align needs no `:` — `{d%}` reads better than `{d:%}`
        None => match head.strip_suffix(['%', '#', '~']) {
            Some(k) => (k, Some(&head[k.len()..])),
            None => (head, None),
        },
    };

    let mut unit = None;
    let mut tone = Tone::default();
    let mut thousands = false;
    for opt in opts.into_iter().flat_map(|o| o.split('.')).filter(|o| !o.is_empty()) {
        match (opt, tone_of(opt)) {
            ("thousands", _) => thousands = true,
            (_, Some(t)) => tone = t,
            _ => unit = Some(unit_of(opt).ok_or_else(|| format!("unknown option `{opt}`"))?),
        }
    }

    if let Some(g) = key.strip_prefix('@') {
        // Validated against the one glyph table `bind` draws from, so the two cannot drift
        return match g {
            "spin" => Ok(Cell::Spinner { tone }),
            _ if super::bind::glyph_of(g, &crate::theme::ThemeChars::unicode()).is_some() => {
                Ok(Cell::Glyph { name: g.to_string(), tone })
            }
            other => Err(format!("unknown `@{other}`")),
        };
    }

    let (align, width, prec, kind) = spec_of(spec.unwrap_or(""))?;
    let key = key.to_string();
    Ok(match kind {
        Some(Kind::Bar) => Cell::Bar { key, width: fixed(width, 12) },
        Some(Kind::Spark) => Cell::Spark { key, width: fixed(width, 12) },
        Some(Kind::Pair) => Cell::Pair { key, tone },
        None if unit.is_some() || prec.is_some() || thousands => {
            Cell::Value { key, width, align, unit, prec, thousands, tone }
        }
        None => Cell::Text { key, width, align, tone },
    })
}

enum Kind {
    Bar,
    Spark,
    Pair,
}

type Spec = (Align, Option<Width>, Option<usize>, Option<Kind>);

fn spec_of(spec: &str) -> Result<Spec, String> {
    let mut chars = spec.chars().peekable();
    let align = match chars.peek() {
        Some('<') => {
            chars.next();
            Align::Left
        }
        Some('>') => {
            chars.next();
            Align::Right
        }
        Some('^') => {
            chars.next();
            Align::Center
        }
        _ => Align::Left,
    };
    let mut digits = String::new();
    while let Some(c) = chars.peek() {
        match c {
            '0'..='9' => {
                digits.push(*c);
                chars.next();
            }
            _ => break,
        }
    }
    let mut width = match digits.is_empty() {
        true => None,
        false => Some(Width::Fixed(digits.parse::<usize>().map_err(|e| e.to_string())?)),
    };
    if chars.peek() == Some(&'*') {
        chars.next();
        width = Some(Width::Star);
    }
    // `.N` before the kind marker: bare fixed-point, so `{blk:.1} blk/s` reads as one unit
    let mut prec = None;
    if chars.peek() == Some(&'.') {
        chars.next();
        let mut d = String::new();
        while let Some('0'..='9') = chars.peek() {
            d.push(chars.next().unwrap_or_default());
        }
        prec = Some(d.parse::<usize>().map_err(|e| e.to_string())?);
    }
    let kind = match chars.next() {
        Some('#') => Some(Kind::Bar),
        Some('~') => Some(Kind::Spark),
        Some('%') => Some(Kind::Pair),
        Some(c) => return Err(format!("unknown cell kind `{c}`")),
        None => None,
    };
    if let Some(c) = chars.next() {
        return Err(format!("trailing `{c}` in spec"));
    }
    Ok((align, width, prec, kind))
}

fn fixed(w: Option<Width>, default: usize) -> usize {
    match w {
        Some(Width::Fixed(n)) => n,
        _ => default,
    }
}

/// Tone names are deliberately disjoint from unit names — `count` is a [`Unit`], and a
/// tone sharing the word would silently turn a value cell into a text cell
fn tone_of(s: &str) -> Option<Tone> {
    Some(match s {
        "dim" => Tone::Dim,
        "bold" => Tone::Count,
        "pass" => Tone::Pass,
        "fail" => Tone::Fail,
        "skip" => Tone::Skip,
        "script_id" => Tone::ScriptId,
        _ => return None,
    })
}

/// Name -> [`Unit`](ztest::api::Unit). Names match the exposition vocabulary a metric
/// row already declares, so a template and a `metrics::Row` cannot disagree
fn unit_of(s: &str) -> Option<ztest::api::Unit> {
    // `thousands` is a spelling, not a metric unit: identity figures (`1,500 ticks`) where
    // `compact`'s `1.50k` names nothing a reader can point at
    if s == "thousands" {
        return None;
    }
    use ztest::api::Unit;
    Some(match s {
        "count" => Unit::Count,
        "per_sec" => Unit::PerSec,
        "fraction" | "percent" => Unit::Fraction,
        "bytes" => Unit::Bytes,
        "bytes_per_sec" => Unit::BytesPerSec,
        "millis" => Unit::Millis,
        "cores" => Unit::Cores,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_and_placeholders_alternate() {
        let cells = parse("a {b} c").expect("parses");
        assert_eq!(cells.len(), 3);
        assert!(matches!(cells[0], Cell::Lit(ref s) if s == "a "));
        assert!(matches!(cells[1], Cell::Text { ref key, .. } if key == "b"));
        assert!(matches!(cells[2], Cell::Lit(ref s) if s == " c"));
    }

    #[test]
    fn a_spec_carries_align_width_unit_and_tone() {
        let cells = parse("{rate:>8|per_sec.dim}").expect("parses");
        let Cell::Value { align, width, unit, tone, .. } = &cells[0] else {
            panic!("expected a value cell, got {:?}", cells[0]);
        };
        assert_eq!(*align, Align::Right);
        assert_eq!(*width, Some(Width::Fixed(8)));
        assert_eq!(*unit, Some(ztest::api::Unit::PerSec));
        assert_eq!(*tone, Tone::Dim);
    }

    /// Options are order-free so a template author never has to remember which comes
    /// first; unit and tone are told apart by name, not position
    #[test]
    fn unit_and_tone_may_appear_in_either_order() {
        let a = parse("{x|dim.bytes}").expect("parses");
        let b = parse("{x|bytes.dim}").expect("parses");
        assert_eq!(a, b);
    }

    #[test]
    fn groups_nest_and_capture_their_literals() {
        let cells = parse("{a}[ · {b}[ · {c}]]").expect("parses");
        let Cell::Group(outer) = &cells[1] else { panic!("expected a group") };
        assert!(matches!(outer[0], Cell::Lit(ref s) if s == " · "));
        assert!(matches!(outer[2], Cell::Group(_)));
    }

    #[test]
    fn doubled_delimiters_escape() {
        let cells = parse("{{literal}} [[not a group]]").expect("parses");
        assert_eq!(cells.len(), 1);
        assert!(matches!(cells[0], Cell::Lit(ref s) if s == "{literal} [not a group]"));
    }

    #[test]
    fn cell_kinds_parse() {
        assert!(matches!(parse("{p:12#}").expect("bar")[0], Cell::Bar { width: 12, .. }));
        assert!(matches!(parse("{s:20~}").expect("spark")[0], Cell::Spark { width: 20, .. }));
        assert!(matches!(parse("{d%}").expect("pair")[0], Cell::Pair { .. }));
        assert!(matches!(parse("{@spin}").expect("spinner")[0], Cell::Spinner { .. }));
    }

    #[test]
    fn malformed_templates_are_rejected_by_name() {
        assert!(parse("{unclosed").is_err());
        assert!(parse("stray }").is_err());
        assert!(parse("[unclosed").is_err());
        assert!(parse("{x|nonsense}").is_err());
        assert!(parse("{x:9q}").is_err());
    }
}
