# ztest AI Contributor Guidelines

## Comments: signal only, delete the narration

Comments earn their place by explaining *why* something non-obvious is
done. A comment that restates what the code already says, or that
explains a decision any competent reader would infer, is noise and MUST
NOT be written. This is a hard rule.

Delete these classes of comment on sight (never add them):

1. **Restating the code.** `// increment i`, `// return the client`,
   `// Storage stack, profile-dependent` above a `match` that is already
   obviously profile-dependent.
1. **Justifying an API's own spec.** e.g. a comment explaining that the
   field is `fstype` not `fsType`, or that an enum value is one of the
   documented set. The code is correct; the reader does not need the
   rejected alternative narrated. Just write `fstype: xfs`.
1. **Docstrings that echo the item name.** A `#[test]` named
   `render_is_valid_yaml_with_all_paths` needs no `/// The render must be valid YAML with all paths` above it. Same for functions whose name and
   signature already say it.
1. **Provenance trivia.** "verified against a live cluster", "reached
   state Ready", "the historical behaviour" and similar belong in the PR
   description or commit message, not the source.

Keep a comment only when removing it would make a senior reader ask "why
is it done *this* way?" and the answer is not in the code: a genuine
gotcha, a non-local invariant, a workaround for an external bug (link
it), an ordering constraint that isn't visible at the call site. When you
do comment, 1-2 lines is the norm; a longer block must justify every
line. Prefer a well-named function or constant over a comment that
labels a block.

## Comments are notes, not prose

**Prose sentences in comments are BANNED.** Not "shortened" — banned. A
comment is a note jotted in a margin: fragments, symbols, parentheticals.
The reader is mid-code and must absorb it without parsing grammar. This
is a hard rule, and it is the one most often violated.

The shape is a **bullet-point note**:

```rust
//! Driver→controller live event stream.
//!
//! - Controller stateless between commands (driver-pod log = only channel to live term)
//! - One EVENT_PREFIX-tagged line per event, lifted from otherwise-verbatim stream
//! - Data only, no formatting (renderers controller-side, so layout floats)
```

not

```rust
//! The controller is stateless between commands, so the driver pod's log is
//! the only channel to a watching terminal: one EVENT_PREFIX-tagged line per
//! event, lifted out of a stream otherwise passed through verbatim.
```

Mechanical rules, all mandatory:

1. **No finite verb where a fragment works.** Kill "is/are/was/has/does"
   as the main verb. `Held across await` not "This lock is held across
   the await". `Callers must drop before poll()` is fine — a real
   constraint keeps its modal.
1. **No leading article.** Never open with "A", "An", "The", "This",
   "One". `Bytes remaining` not "The number of bytes remaining".
1. **Parenthesise the why.** The rationale goes in `(...)` after the
   fact, not in a subordinate clause. `rate ships pre-computed (resumed stream drops events)` not "rate is published rather than differenced
   controller-side, because a resumed stream replays and drops events".
1. **Use symbols over words.** `=`, `→`, `!=`, `>=`, `&`, `/`. `driver → controller` not "from the driver to the controller". `x = only channel` not "x is the only channel that exists".
1. **Multiple facts = multiple bullets.** Never chain with em-dash,
   semicolon, "because", "so that", "which means", "rather than". One
   fact per line, `- ` prefixed.
1. **No hedges.** "deliberately", "genuinely", "merely", "actually",
   "simply", "note that", "of course". Zero information.
1. **No trailing period on a fragment.** It is not a sentence.

**Budget: one line. Two if the invariant needs it.** Three-plus bullets
means it belongs in `docs/`, in a better name, or nowhere. A 30-word
comment evicts two lines of code from the screen and costs more attention
than the code it sits on.

**Comments run to 100 columns, same as `max_width`.** Never wrap at 80.
Two 80-column lines that would fit on one 100-column line are a bug —
merge them. rustfmt does not enforce this (`wrap_comments` is nightly-only
and reflows bullets into prose, so it stays off); the width is yours to
hold.

Length tracks *surprise*, not importance. Hairy invariant → two lines.
Well-named function → zero. Nothing earns twelve.

## Type declarations stay dense

A type declaration is read as a *shape*. A reader scans it to learn what
the thing is made of, and every line that isn't a field costs them that.
A 5-field struct is 7 lines: the `struct` line, five fields, the closing
brace. This is a hard rule.

**Fields and enum variants get no doc comment by default.** Do not write
one, and delete one you find. The field's name and type are the
documentation; if they aren't, that is a naming or typing bug and the fix
is a better name or a type that constrains the value (`Duration` over
`u64`, a newtype over `String`, an enum over `bool`), never a `///` that
apologises for the weak one.

The narrow exception is a field carrying an invariant the type genuinely
cannot express — a unit the type erases, a cross-field constraint, a
range the compiler won't enforce. Then prefer one line at the *top* of
the struct naming the field, and only inline the comment when it would be
lost up there. "This field is the X" is never such an invariant.

**Type-level docs: 0-2 lines normally, 5 at the absolute ceiling.** Say
what the type is for and how it fits its neighbours — the thing a reader
cannot recover by reading the fields. Then stop. Reaching for the fourth
line means it belongs in `docs/`. Specifically, never write:

- Design history. "used to carry", "was previously", "stopped doing X
  because" — dead alternatives and the path to the current shape belong
  in the commit message. The code is the current shape.
- A field-by-field walkthrough in prose. That is field docs relocated.
- Restated names. `/// A validator component.` above
  `struct Validator` is zero information.

If a type is subtle enough to need more than five lines, that is a
signal to write it up in `docs/` and link it in one line, not to grow the
header.

**Exception — clap and serde types.** Doc comments on `#[derive(Parser)]`
/ `Args` / `Subcommand` fields and variants are the `--help` text the
user reads: they are user-facing strings that happen to use `///`
syntax. Same for any type whose docs feed a generated schema. These are
code, not commentary — write them well and never strip them.
