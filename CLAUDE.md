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
2. **Justifying an API's own spec.** e.g. a comment explaining that the
   field is `fstype` not `fsType`, or that an enum value is one of the
   documented set. The code is correct; the reader does not need the
   rejected alternative narrated. Just write `fstype: xfs`.
3. **Docstrings that echo the item name.** A `#[test]` named
   `render_is_valid_yaml_with_all_paths` needs no `/// The render must be
   valid YAML with all paths` above it. Same for functions whose name and
   signature already say it.
4. **Provenance trivia.** "verified against a live cluster", "reached
   state Ready", "the historical behaviour" and similar belong in the PR
   description or commit message, not the source.

Keep a comment only when removing it would make a senior reader ask "why
is it done *this* way?" and the answer is not in the code: a genuine
gotcha, a non-local invariant, a workaround for an external bug (link
it), an ordering constraint that isn't visible at the call site. When you
do comment, 1-2 lines is the norm; a longer block must justify every
line. Prefer a well-named function or constant over a comment that
labels a block.
