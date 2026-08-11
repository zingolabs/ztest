//! Terminal time-series plots: the drawing primitives behind `ztest sync
//! status` and the live dashboard.
//!
//! The idea taken from btop is not its glyphs, it is that **several series
//! share one time axis so a reader correlates them by eye** — throughput fell
//! *while* CPU stayed pinned and I/O collapsed. Everything here therefore draws
//! into a caller-fixed column count, and callers give every plot on a screen
//! the same one, so a vertical position means the same instant in all of them.
//! [`Timeline`](crate::sync::Timeline) supplies the shared bucketing that makes
//! that alignment real rather than coincidental.
//!
//! ## One geometry, three glyph tiers
//!
//! Every mode rasterises at **2 sub-columns × 4 sub-rows per character**, and a
//! cell is folded by looking up `table[left_level * 5 + right_level]` in a
//! 25-entry table chosen by mode. This is btop's `Draw::Graph::_create`
//! arrangement, and the reason to copy it is that the tiers then differ only in
//! *data*: `▗▄▟█` cannot express four vertical levels and `#+.` cannot express
//! two horizontal ones, but the table absorbs that loss so no code branches on
//! it. Sampling above what a tier can show costs nothing; branching on it costs
//! a `match` at every cell forever.
//!
//! ## What colour means
//!
//! Colour encodes **channel identity** — which pool a band belongs to — and
//! never magnitude. btop gradients by height because its y-axis is utilisation,
//! a real 0–100% ceiling; `ops/s` has no ceiling, so a vertical gradient over it
//! would encode nothing.
//!
//! A character carries exactly one colour whatever its sub-cell count, so a cell
//! straddling two channels is awarded to whichever owns the most of it.
//!
//! Because colour is spent on identity, the **variation zone** is carried by
//! glyph instead ([`Fill`]): every mode has a lighter companion table, so a
//! bucket that stalled reads as a stall on a `NO_COLOR` terminal and in a
//! captured CI log, neither of which can be asked to render an attribute.

use owo_colors::{OwoColorize as _, Style};

/// One bucket's observed range, or `None` where nothing was observed.
///
/// A gap is never interpolated across: during a partition the absence of data
/// *is* the observation, and joining the line over it would erase the event.
pub type Band = Option<(f64, f64)>;

/// Sub-columns every mode rasterises to, per character cell.
const SUB_COLS: usize = 2;
/// Sub-rows every mode rasterises to, per character cell.
const SUB_ROWS: usize = 4;
/// Fill levels a sub-column can take, `0..=SUB_ROWS`.
const LEVELS: usize = SUB_ROWS + 1;

/// Glyph tier a terminal can render.
///
/// Only a table selector: all three rasterise identically (see the module
/// docs), so this never reaches the geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphMode {
    /// 2×4 braille dots per character. Needs a braille-capable font.
    Braille,
    /// `#+.:` — needs nothing.
    Ascii,
}

/// What a filled sub-cell represents.
///
/// The Solid/Varying split is what lets one plot answer both questions a sync
/// raises. Solid mass up to the bucket minimum is throughput that never dropped
/// below that level; lighter mass above it is the range it swung through. After
/// coarsening to half-hour buckets a 90-second stall is a light column reaching
/// almost to the floor — which a mean, or a max-only area, cannot show at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fill {
    /// At or below the bucket minimum.
    Solid,
    /// Between the bucket minimum and maximum.
    Varying,
    /// A channel below this one went unobserved, so nothing here can be placed.
    Unknown,
}

impl Fill {
    /// Every fill, ordered by **increasing uncertainty**.
    ///
    /// The order is load-bearing twice over: it indexes the tally in [`fold`],
    /// and folding breaks a tie toward the later entry, so a cell split evenly
    /// between known and unplaceable reports the doubt. Both readings come from
    /// this one array rather than from the enum's discriminant, so reordering
    /// the variants cannot silently invert them.
    const ALL: [Fill; 3] = [Fill::Solid, Fill::Varying, Fill::Unknown];

    const fn index(self) -> usize {
        match self {
            Fill::Solid => 0,
            Fill::Varying => 1,
            Fill::Unknown => 2,
        }
    }
}

/// A filled sub-cell: which channel owns it and what it represents.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Sub {
    channel: usize,
    fill: Fill,
}

// ────────────────────────────── glyph tables ──────────────────────────────

/// Braille dot bits by fill level: the bottom `level` dots of the left cell
/// column. Dots run 1-2-3-7 downward, so the bottom dot is 7 (`0x40`) and these
/// are not sequential. `[solid, stippled]`.
const BRAILLE_LEFT: [[u8; LEVELS]; 2] = [
    [0x00, 0x40, 0x44, 0x46, 0x47],
    [0x00, 0x40, 0x40, 0x42, 0x42],
];
/// As [`BRAILLE_LEFT`], for the right cell column (dots 4-5-6-8).
const BRAILLE_RIGHT: [[u8; LEVELS]; 2] = [
    [0x00, 0x80, 0xA0, 0xB0, 0xB8],
    [0x00, 0x80, 0x80, 0x90, 0x90],
];

/// OR the two columns' dots into the braille block.
///
/// Derived rather than transcribed. btop lists its `braille_up` as 25 literal
/// glyphs, which is 25 chances to paste a neighbouring codepoint into something
/// no test would ever read — and the stippled companion would be 25 more. The
/// masks above are the whole specification, so building from them makes a
/// transcription error unrepresentable instead of merely unlikely.
const fn braille_table(variant: usize) -> [char; 25] {
    let mut out = [' '; 25];
    let (mut l, mut r) = (0, 0);
    while l < LEVELS {
        while r < LEVELS {
            let bits = BRAILLE_LEFT[variant][l] | BRAILLE_RIGHT[variant][r];
            out[l * LEVELS + r] = match char::from_u32(0x2800 + bits as u32) {
                Some(c) => c,
                None => ' ',
            };
            r += 1;
        }
        r = 0;
        l += 1;
    }
    out
}

/// Braille, solid. Equals btop's `braille_up`.
const BRAILLE_SOLID: [char; 25] = braille_table(0);

/// Braille, variation zone: the same silhouette stippled to alternate dots.
///
/// Halving the dots halves the vertical resolution of the band — levels 1/2 and
/// 3/4 collapse. That is the right thing to lose: the band *is* a range, so
/// reporting its edge to a quarter-cell was never meaningful, and the density
/// difference against [`BRAILLE_SOLID`] is what makes the two zones separable
/// without colour.
const BRAILLE_LIGHT: [char; 25] = braille_table(1);

/// ASCII, solid: btop's `tty_up` density ladder with ` .+#`.
const ASCII_SOLID: [char; 25] = [
    ' ', '.', '.', '+', '+', //
    '.', '.', '+', '+', '#', //
    '.', '+', '+', '+', '#', //
    '+', '+', '+', '#', '#', //
    '+', '#', '#', '#', '#',
];

/// ASCII, variation zone: the same ladder one step lighter, so `#` never means
/// "swung" and `:` never means "steady".
const ASCII_LIGHT: [char; 25] = [
    ' ', '.', '.', '.', '.', //
    '.', '.', '.', '.', ':', //
    '.', '.', '.', '.', ':', //
    '.', '.', '.', ':', ':', //
    '.', ':', ':', ':', ':',
];

/// Hatch for the unplaceable region above an unobserved channel.
///
/// Not level-indexed, and that is the point: the region has no magnitude, so
/// there is nothing for a level table to say about it. A run of these reads as
/// diagonal hatching — the long-standing convention for "no data", as against
/// blank for "zero".
const UNKNOWN_GLYPH: (char, char) = ('╱', '/');

fn table(mode: GraphMode, fill: Fill) -> &'static [char; 25] {
    match (mode, fill) {
        (GraphMode::Braille, Fill::Varying) => &BRAILLE_LIGHT,
        (GraphMode::Braille, _) => &BRAILLE_SOLID,
        (GraphMode::Ascii, Fill::Varying) => &ASCII_LIGHT,
        (GraphMode::Ascii, _) => &ASCII_SOLID,
    }
}

// ─────────────────────────────── palette ───────────────────────────────

/// Per-channel colours, looked up **by channel name**.
///
/// By name and not by stack position, because a caller drops channels it has no
/// data for — a chain with no sprout activity plots four pools, not five. Keyed
/// positionally, that shifts every pool above the gap into its neighbour's
/// colour, and shifts them back the moment the quiet pool reports. Colour is
/// supposed to say *which pool this is*; it cannot also depend on who else
/// happened to be measured.
#[derive(Clone, Debug)]
pub struct Palette {
    channels: Vec<(&'static str, Style)>,
    /// For a channel this palette does not name, and for single-series plots.
    default: Style,
    /// Axis, frame, the unknown hatch, and the empty field behind the plot.
    pub axis: Style,
    /// The optional comparison rule.
    pub baseline: Style,
    /// Whether an attribute may be emitted at all. An uncoloured terminal gets
    /// none — emitting one would put escape sequences into a rendering whose
    /// whole point is that it has none.
    colorize: bool,
}

impl Palette {
    /// The pool palette, oldest pool first so it matches
    /// [`Work::rows`](crate::sync::Work::rows) stacking order: transparent,
    /// sprout, sapling, orchard, ironwood.
    ///
    /// Hues are ordered to stay distinguishable when adjacent in a stack.
    pub fn pools(colorize: bool) -> Palette {
        match colorize {
            false => Palette::plain(),
            true => Palette {
                channels: vec![
                    ("transparent", Style::new().bright_black()),
                    ("sprout", Style::new().magenta()),
                    ("sapling", Style::new().yellow()),
                    ("orchard", Style::new().green()),
                    ("ironwood", Style::new().cyan()),
                ],
                default: Style::new().white(),
                axis: Style::new().bright_black(),
                baseline: Style::new().blue(),
                colorize: true,
            },
        }
    }

    /// An uncoloured palette: every channel renders alike.
    pub fn plain() -> Palette {
        Palette {
            channels: Vec::new(),
            default: Style::new(),
            axis: Style::new(),
            baseline: Style::new(),
            colorize: false,
        }
    }

    fn style_of(&self, channel: &str) -> Style {
        self.channels
            .iter()
            .find(|(name, _)| *name == channel)
            .map(|(_, style)| *style)
            .unwrap_or(self.default)
    }

    /// Dimming a varying cell is additive polish on top of the lighter glyph
    /// table, never the only signal.
    fn shade(&self, base: Style, fill: Fill) -> Style {
        match self.colorize && fill == Fill::Varying {
            true => base.dimmed(),
            false => base,
        }
    }
}

// ──────────────────────────────── geometry ────────────────────────────────

/// Plot geometry and decoration.
///
/// There is no ceiling knob: the y-axis always auto-scales to what the data and
/// the [`baseline`](PlotOpts::baseline) require. Every quantity the harness
/// plots is an unbounded rate, so a pinned ceiling had no honest use — and when
/// it existed it was also read as "is this plot bounded?", which is how the
/// baseline rule came to be silently dropped on every auto-scaled plot.
///
/// A live plot that rescales every frame jitters under the reader. The fix is
/// [`at_least`](PlotOpts::at_least): the caller keeps the largest ceiling it has
/// shown and feeds it back, so the axis ratchets instead of breathing. Keeping
/// that memory in the caller rather than here is what leaves this module a pure
/// function of its inputs.
#[derive(Clone, Debug)]
pub struct PlotOpts {
    /// Character columns the plot body occupies (excluding any axis gutter the
    /// caller draws).
    pub width: usize,
    /// Character rows.
    pub height: usize,
    pub mode: GraphMode,
    /// Draw a horizontal rule at this value: the comparison run's rate. Always
    /// on scale, because it participates in the ceiling.
    pub baseline: Option<f64>,
    /// A floor under the auto-scaled ceiling.
    pub at_least: Option<f64>,
}

impl PlotOpts {
    /// Geometry with no comparison rule and a free-running axis.
    pub fn new(width: usize, height: usize, mode: GraphMode) -> PlotOpts {
        PlotOpts {
            width,
            height,
            mode,
            baseline: None,
            at_least: None,
        }
    }

    /// Draw the comparison rule at `value`.
    #[cfg(test)]
    pub fn baseline(mut self, value: f64) -> PlotOpts {
        self.baseline = Some(value);
        self
    }

    /// Hold the axis at `value` or above. Pair with [`ceiling`] to ratchet.
    #[cfg(test)]
    pub fn at_least(mut self, value: f64) -> PlotOpts {
        self.at_least = Some(value);
        self
    }
}

// ──────────────────────────────── plotting ────────────────────────────────

/// One named series in a stack: `(name, bands)`.
///
/// The name is what the palette keys on, so it travels with the data rather
/// than being implied by position. See [`Palette`] for why that matters.
pub type Channel<'a> = (&'a str, Vec<Band>);

/// A stacked area plot: `channels[0]` on the bottom, each subsequent channel
/// above the last, envelope equal to their sum.
///
/// Stacking rather than overlaying is what makes a *composition* shift visible
/// as distinct from a *rate* shift — the failure mode where a transparent-heavy
/// range fakes a throughput win.
///
/// A channel the palette does not name still plots, in the fallback colour; a
/// renderer must not be the thing that takes a run down, so this reports nothing
/// and draws. Callers wanting to know can ask [`Palette::covers`] first.
pub fn plot_stacked(channels: &[Channel<'_>], opts: &PlotOpts, palette: &Palette) -> Vec<String> {
    let series: Vec<&[Band]> = channels.iter().map(|(_, b)| b.as_slice()).collect();
    // Resolved once per plot rather than per cell: the name lookup is a linear
    // scan, and folding runs width × height times a frame.
    let styles: Vec<Style> = channels
        .iter()
        .map(|(name, _)| palette.style_of(name))
        .collect();
    render(&rasterize(&series, opts), opts, palette, &styles)
}

/// The ceiling [`plot_stacked`] would auto-scale to for this input.
///
/// Exposed so a live caller can ratchet it across frames and hand it back via
/// [`PlotOpts::at_least`], and so an axis gutter can be labelled with the same
/// number the plot beside it was drawn against.
pub fn ceiling(channels: &[Channel<'_>], opts: &PlotOpts) -> f64 {
    let sub_w = opts.width * SUB_COLS;
    let resampled: Vec<Vec<Band>> = channels.iter().map(|(_, b)| resample(b, sub_w)).collect();
    let borrowed: Vec<&[Band]> = resampled.iter().map(Vec::as_slice).collect();
    ceiling_of(&borrowed, opts)
}

// ───────────────────────────── rasterisation ─────────────────────────────

/// A sub-cell grid, row 0 at the **top**.
struct Raster {
    sub_w: usize,
    sub_h: usize,
    /// The value the top of the grid stands for. Resolved once here so the
    /// baseline rule and the fill can never disagree about the scale.
    ceiling: f64,
    cells: Vec<Option<Sub>>,
}

impl Raster {
    fn at(&self, x: usize, y: usize) -> Option<Sub> {
        self.cells.get(y * self.sub_w + x).copied().flatten()
    }

    /// Fill level `l` counted from the bottom, as a grid row.
    fn row_of(&self, level: usize) -> usize {
        self.sub_h.saturating_sub(1).saturating_sub(level)
    }

    fn set(&mut self, x: usize, level: usize, sub: Sub) {
        let y = self.row_of(level);
        if x < self.sub_w && level < self.sub_h {
            self.cells[y * self.sub_w + x] = Some(sub);
        }
    }
}

fn rasterize(channels: &[&[Band]], opts: &PlotOpts) -> Raster {
    let sub_w = opts.width * SUB_COLS;
    let sub_h = opts.height * SUB_ROWS;
    let resampled: Vec<Vec<Band>> = channels.iter().map(|c| resample(c, sub_w)).collect();
    let borrowed: Vec<&[Band]> = resampled.iter().map(Vec::as_slice).collect();
    let mut raster = Raster {
        sub_w,
        sub_h,
        ceiling: ceiling_of(&borrowed, opts),
        cells: vec![None; sub_w * sub_h],
    };
    // A flat-zero or empty series has no scale to draw against. Returning the
    // empty raster renders an empty frame, which says "nothing observed" —
    // where dividing by zero would paint a full-height block and claim the
    // opposite.
    // NaN is tested explicitly rather than folded into the comparison: it
    // propagates out of an empty fold and would slip past `<= 0.0`, scaling
    // every level to NaN and painting a plausible graph of nothing.
    if sub_w == 0 || sub_h == 0 || raster.ceiling.is_nan() || raster.ceiling <= 0.0 {
        return raster;
    }

    for x in 0..sub_w {
        paint_column(&mut raster, x, &borrowed);
    }
    raster
}

/// Paint one sub-column of the stack.
///
/// A channel's floor depends only on the channels *below* it, so an unobserved
/// channel invalidates itself and everything above it and nothing below. That
/// gives the whole rule: draw the observed prefix where it belongs, then hatch
/// from its top to the ceiling to say the rest is unplaceable. Blanking the
/// column instead would discard real observations; drawing the stack anyway
/// would assert a total nobody measured.
fn paint_column(raster: &mut Raster, x: usize, channels: &[&[Band]]) {
    let column: Vec<Band> = channels
        .iter()
        .map(|c| c.get(x).copied().flatten())
        .collect();

    // Nothing observed at all is a gap in the plot, not an unplaceable stack.
    if column.iter().all(Option::is_none) {
        return;
    }

    let known = column
        .iter()
        .position(Option::is_none)
        .unwrap_or(column.len());
    let bands: Vec<(f64, f64)> = column[..known].iter().flatten().copied().collect();
    let rows = apportion(
        &bands.iter().map(|&(_, hi)| hi).collect::<Vec<_>>(),
        raster.ceiling,
        raster.sub_h,
    );

    let mut level = 0usize;
    for (channel, (&(lo, hi), &n)) in bands.iter().zip(rows.iter()).enumerate() {
        // The solid part is this channel's own [0, lo] scaled into the rows it
        // was actually given, so the split can never exceed the segment.
        // A zero-height channel receives no rows, so its split is vacuous.
        let solid = match hi > 0.0 {
            true => ((n as f64 * (lo / hi).clamp(0.0, 1.0)).round() as usize).min(n),
            false => 0,
        };
        for i in 0..n {
            let fill = match i < solid {
                true => Fill::Solid,
                false => Fill::Varying,
            };
            raster.set(x, level + i, Sub { channel, fill });
        }
        level += n;
    }

    if known < column.len() {
        for l in level..raster.sub_h {
            raster.set(
                x,
                l,
                Sub {
                    channel: known,
                    fill: Fill::Unknown,
                },
            );
        }
    }
}

/// Split `sub_h` sub-rows across `values` by largest remainder.
///
/// Rounding each channel independently cannot preserve the total — that is what
/// let small pools round to nothing, collide on one sub-cell, and overwrite each
/// other. Apportioning the column instead makes the allocation sum to the height
/// the total deserves *by construction*, so no channel can land on another's row
/// and no special case is needed to keep a small one visible.
fn apportion(values: &[f64], ceiling: f64, sub_h: usize) -> Vec<usize> {
    let exact: Vec<f64> = values
        .iter()
        .map(|v| (v / ceiling).clamp(0.0, 1.0) * sub_h as f64)
        .collect();
    // Any positive total claims a sub-row, however small: a rate far under the
    // ceiling — one dwarfed by a baseline it is being compared against, say —
    // must not round away into an axis that reads as "nothing happened".
    let sum = exact.iter().sum::<f64>();
    let total = match sum > 0.0 {
        true => (sum.round() as usize).clamp(1, sub_h),
        false => 0,
    };
    let mut rows: Vec<usize> = exact.iter().map(|e| e.floor() as usize).collect();

    let mut order: Vec<usize> = (0..exact.len()).collect();
    order.sort_by(|&a, &b| {
        let rem = |i: usize| exact[i] - exact[i].floor();
        rem(b).total_cmp(&rem(a)).then(a.cmp(&b))
    });
    let deficit = total.saturating_sub(rows.iter().sum::<usize>());
    for &i in order.iter().cycle().take(deficit) {
        rows[i] += 1;
    }

    // A pool that is genuinely active but tiny must not render identically to an
    // idle one, so lend it a row from the largest holder that can spare one.
    for i in 0..rows.len() {
        if rows[i] > 0 || values[i] <= 0.0 {
            continue;
        }
        let Some(donor) = (0..rows.len())
            .max_by_key(|&j| rows[j])
            .filter(|&j| rows[j] > 1)
        else {
            break;
        };
        rows[donor] -= 1;
        rows[i] += 1;
    }
    rows
}

/// Largest column total the plot must fit, never below the baseline.
///
/// Summed across channels, not the tallest single one: a stack is as tall as its
/// columns, and scaling to one channel would push the total off the top. The
/// baseline joins the scale rather than being clipped against it, because a
/// comparison run three times faster is the most important thing on the plot and
/// clipping it renders it invisible.
fn ceiling_of(channels: &[&[Band]], opts: &PlotOpts) -> f64 {
    (0..channels.iter().map(|c| c.len()).max().unwrap_or(0))
        .map(|x| {
            channels
                .iter()
                .filter_map(|c| c.get(x).copied().flatten().map(|(_, hi)| hi))
                .sum::<f64>()
        })
        .chain(opts.baseline)
        .chain(opts.at_least)
        .fold(0.0, f64::max)
}

/// Fit `bands` to `n` columns.
///
/// Downsampling unions the bands sharing a column, so an extreme survives being
/// squeezed — the same reason buckets keep bands in the first place. A gap
/// narrower than one column is therefore absorbed by its neighbours: union means
/// "something was observed in this window", and preferring the gap instead would
/// suppress real data. Callers needing every partition visible should size the
/// timeline to the plot. Upsampling repeats, which is honest about there being
/// no finer data.
fn resample(bands: &[Band], n: usize) -> Vec<Band> {
    if n == 0 {
        return Vec::new();
    }
    if bands.is_empty() {
        return vec![None; n];
    }
    (0..n)
        .map(|i| {
            let from = i * bands.len() / n;
            let to = (((i + 1) * bands.len()) / n).max(from + 1).min(bands.len());
            bands[from..to]
                .iter()
                .flatten()
                .copied()
                .reduce(|(a, b), (lo, hi)| (a.min(lo), b.max(hi)))
        })
        .collect()
}

// ────────────────────────────── glyph folding ──────────────────────────────

fn render(raster: &Raster, opts: &PlotOpts, palette: &Palette, styles: &[Style]) -> Vec<String> {
    let baseline = opts.baseline.and_then(|v| baseline_row(v, raster));
    (0..opts.height)
        .map(|row| render_row(raster, opts, palette, styles, row, baseline))
        .collect()
}

/// The character row the comparison rule falls on.
///
/// Always on the plot when there is a plot at all: [`ceiling_of`] folded the
/// baseline into the scale, so it cannot fall off the top.
fn baseline_row(value: f64, raster: &Raster) -> Option<usize> {
    (value > 0.0 && raster.ceiling > 0.0).then(|| {
        let level = ((value / raster.ceiling) * raster.sub_h as f64).round() as usize;
        raster.row_of(level.min(raster.sub_h.saturating_sub(1))) / SUB_ROWS
    })
}

fn render_row(
    raster: &Raster,
    opts: &PlotOpts,
    palette: &Palette,
    styles: &[Style],
    row: usize,
    baseline: Option<usize>,
) -> String {
    let mut out = String::with_capacity(opts.width * 8);
    for col in 0..opts.width {
        match fold(raster, palette, styles, opts.mode, row, col) {
            Some(cell) => out.push_str(&cell),
            // An empty cell on the baseline row carries the rule; elsewhere it
            // is the blank field behind the plot. The rule is drawn only where
            // nothing was observed, so a comparison line can never hide data.
            None if baseline == Some(row) => out.push_str(&'╌'.style(palette.baseline).to_string()),
            None => out.push(' '),
        }
    }
    out
}

/// Fold one character cell, or `None` if nothing in it is filled.
fn fold(
    raster: &Raster,
    palette: &Palette,
    styles: &[Style],
    mode: GraphMode,
    row: usize,
    col: usize,
) -> Option<String> {
    // A cell holds SUB_COLS × SUB_ROWS sub-cells, so at most that many distinct
    // channels can appear in one — whatever the stack's depth. Sizing the tally
    // from the cell's own geometry keeps this allocation-free on a path that
    // runs width × height times per frame *and* exact for any number of
    // channels, where a guessed bound would silently mis-colour past its edge.
    let mut owners: [(usize, usize); SUB_COLS * SUB_ROWS] = [(usize::MAX, 0); SUB_COLS * SUB_ROWS];
    let mut fills = [0usize; Fill::ALL.len()];
    let mut levels = [0usize; SUB_COLS];

    for (sx, level) in levels.iter_mut().enumerate() {
        for sy in 0..SUB_ROWS {
            let Some(sub) = raster.at(col * SUB_COLS + sx, row * SUB_ROWS + sy) else {
                continue;
            };
            *level += 1;
            fills[sub.fill.index()] += 1;
            let slot = owners
                .iter_mut()
                .find(|(id, n)| *id == sub.channel || *n == 0)
                .expect("a cell cannot hold more channels than it has sub-cells");
            *slot = (sub.channel, slot.1 + 1);
        }
    }
    if levels.iter().all(|&l| l == 0) {
        return None;
    }

    // Ties go to the more uncertain reading: at the boundary of an unplaceable
    // region, understating what we know beats asserting what we do not. That is
    // why the tie-break prefers the *later* `Fill::ALL` entry here, while the
    // owner vote below prefers the *lower* channel — there, the tie is between
    // two equally real pools and the stable answer is the one nearer the floor.
    let fill = fills
        .iter()
        .enumerate()
        .max_by_key(|&(i, n)| (*n, i))
        .map(|(i, _)| Fill::ALL[i])
        .unwrap_or(Fill::Solid);
    if fill == Fill::Unknown {
        let (unicode, ascii) = UNKNOWN_GLYPH;
        let glyph = match mode {
            GraphMode::Ascii => ascii,
            _ => unicode,
        };
        return Some(glyph.style(palette.axis).to_string());
    }

    let channel = owners
        .iter()
        .filter(|(_, n)| *n > 0)
        .max_by_key(|(id, n)| (*n, std::cmp::Reverse(*id)))
        .map(|(id, _)| *id)
        .unwrap_or(0);
    let base = styles.get(channel).copied().unwrap_or_default();
    let glyph = table(mode, fill)[levels[0] * LEVELS + levels[1]];
    Some(glyph.style(palette.shade(base, fill)).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bands(values: &[f64]) -> Vec<Band> {
        values.iter().map(|&v| Some((v, v))).collect()
    }

    fn opts(w: usize, h: usize) -> PlotOpts {
        PlotOpts::new(w, h, GraphMode::Ascii)
    }

    /// A single-channel plot. Most rasterisation properties are stated over one
    /// channel; production callers always stack, so this is a test affordance
    /// rather than API.
    fn plot_band(bands: &[Band], opts: &PlotOpts, palette: &Palette) -> Vec<String> {
        plot_stacked(&[("", bands.to_vec())], opts, palette)
    }

    /// Name a list of series positionally, for tests about geometry rather than
    /// about colour.
    fn stack(series: &[Vec<Band>]) -> Vec<Channel<'static>> {
        const NAMES: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
        series
            .iter()
            .enumerate()
            .map(|(i, b)| (NAMES[i], b.clone()))
            .collect()
    }

    /// Borrow a list of series for the rasteriser, which takes slices so the
    /// public entry point need not clone.
    fn borrow(series: &[Vec<Band>]) -> Vec<&[Band]> {
        series.iter().map(Vec::as_slice).collect()
    }

    fn plain() -> Palette {
        Palette::plain()
    }

    /// Any glyph from the solid ASCII ladder — the tests read "is there mass
    /// here", not which density step it landed on.
    fn solid(c: char) -> bool {
        ASCII_SOLID.contains(&c) && c != ' '
    }

    fn light(c: char) -> bool {
        c == ':'
    }

    fn row_has(rows: &[String], row: usize, f: impl Fn(char) -> bool) -> bool {
        rows[row].chars().any(f)
    }

    #[test]
    fn a_plot_has_the_requested_geometry() {
        let rows = plot_band(&bands(&[1.0, 2.0, 3.0]), &opts(10, 3), &plain());
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.chars().count() == 10), "{rows:?}");
    }

    /// The signal a reader takes from a plot's shape: bigger values reach higher
    /// rows, and every column is filled mass down to the axis rather than a
    /// floating line.
    #[test]
    fn taller_values_occupy_higher_rows() {
        let rows = plot_band(&bands(&[1.0, 4.0]), &opts(2, 4), &plain());
        let cell = |r: usize, c: usize| rows[r].chars().nth(c).unwrap();
        assert_eq!(cell(0, 0), ' ', "the small value must not reach the top");
        assert!(solid(cell(0, 1)), "the large value must reach the top");
        assert!(solid(cell(3, 0)), "every column is anchored at the axis");
        assert!(solid(cell(3, 1)));
    }

    /// The band's two halves are distinguishable **without colour**: mass below
    /// the minimum is throughput that never dropped, mass above it is the swing.
    /// Collapsing them would make a bucket containing a stall look steady.
    #[test]
    fn the_variation_zone_is_marked_apart_from_the_solid_floor() {
        let steady = plot_band(&[Some((100.0, 100.0))], &opts(1, 4), &plain());
        let stalled = plot_band(&[Some((0.0, 100.0))], &opts(1, 4), &plain());
        assert!(
            steady.iter().all(|r| !r.chars().any(light)),
            "steady throughput must carry no variation mark: {steady:?}"
        );
        assert!(
            stalled.iter().any(|r| r.chars().any(light)),
            "a stall must be marked: {stalled:?}"
        );
        assert!(
            stalled.iter().all(|r| !r.trim().is_empty()),
            "the swing still fills the column: {stalled:?}"
        );
    }

    /// Every mode must separate the zones by glyph, because the plain palette
    /// emits no attributes at all and a captured CI log has no colour to read.
    #[test]
    fn every_mode_separates_the_zones_without_colour() {
        for mode in [GraphMode::Braille, GraphMode::Ascii] {
            let o = PlotOpts::new(1, 4, mode);
            let steady = plot_band(&[Some((100.0, 100.0))], &o, &plain());
            let stalled = plot_band(&[Some((0.0, 100.0))], &o, &plain());
            assert_ne!(
                steady, stalled,
                "{mode:?} renders a stall identically to steady throughput"
            );
            assert!(
                steady.iter().chain(&stalled).all(|r| !r.contains('\u{1b}')),
                "{mode:?} emitted an escape under a plain palette"
            );
        }
    }

    /// A gap is data that does not exist. Painting through it would erase
    /// exactly the partition or stall the plot exists to show.
    #[test]
    fn a_gap_is_left_blank_rather_than_interpolated() {
        let series = vec![Some((5.0, 5.0)), None, Some((5.0, 5.0))];
        let rows = plot_band(&series, &opts(3, 2), &plain());
        assert!(
            rows.iter().all(|r| r.chars().nth(1) == Some(' ')),
            "column 1 must stay empty: {rows:?}"
        );
        assert!(rows.iter().any(|r| r.chars().next().is_some_and(solid)));
    }

    /// A stack whose lower channel is observed but whose upper one is not can be
    /// drawn up to the gap and no further: the observed prefix keeps its place,
    /// and everything above it is unplaceable rather than absent.
    #[test]
    fn a_partly_observed_column_keeps_its_prefix_and_hatches_the_rest() {
        let channels = vec![
            vec![Some((5.0, 5.0)), Some((5.0, 5.0))],
            vec![Some((5.0, 5.0)), None],
        ];
        let rows = plot_stacked(&stack(&channels), &opts(2, 8), &Palette::plain());
        let col1: Vec<char> = rows.iter().map(|r| r.chars().nth(1).unwrap()).collect();
        assert!(
            col1.contains(&'/'),
            "the unplaceable region must be hatched: {col1:?}"
        );
        assert!(
            col1.iter().any(|&c| solid(c)),
            "the observed channel must still be drawn: {col1:?}"
        );
        assert!(
            col1.last().is_some_and(|&c| solid(c)),
            "the observed channel keeps the floor, hatch sits above it: {col1:?}"
        );
    }

    /// Observing nothing at all is a gap, not an unplaceable stack — otherwise
    /// a run that has not started yet renders as solid hatching.
    #[test]
    fn a_wholly_unobserved_column_is_blank_not_hatched() {
        let channels = vec![vec![Some((5.0, 5.0)), None], vec![Some((5.0, 5.0)), None]];
        let rows = plot_stacked(&stack(&channels), &opts(2, 4), &Palette::plain());
        assert!(
            rows.iter().all(|r| r.chars().nth(1) == Some(' ')),
            "{rows:?}"
        );
    }

    /// A steady rate has zero band thickness; it must still draw, or "no
    /// variation" would render identically to "no data".
    #[test]
    fn a_zero_thickness_band_still_draws() {
        let rows = plot_band(&[Some((7.0, 7.0))], &opts(1, 4), &plain());
        assert!(rows.iter().any(|r| r.chars().any(solid)), "{rows:?}");
    }

    /// The band is the whole reason buckets keep min and max: a bucket holding
    /// both a peak and a stall must show a visibly larger swing than a steady
    /// one that reached the same peak.
    #[test]
    fn a_wide_band_shows_more_variation_than_a_narrow_one() {
        let swing = |b: Band| {
            plot_band(&[b], &opts(1, 8), &plain())
                .iter()
                .filter(|r| r.chars().any(light))
                .count()
        };
        assert!(
            swing(Some((0.0, 100.0))) > swing(Some((90.0, 100.0))),
            "a stall must not look like steady throughput"
        );
    }

    /// Empty input must render an empty frame, not divide by zero and paint a
    /// full block claiming an observation nobody made.
    #[test]
    fn an_empty_or_flat_zero_series_renders_an_empty_frame() {
        for series in [vec![], vec![None; 4], bands(&[0.0, 0.0])] {
            let rows = plot_band(&series, &opts(4, 2), &plain());
            assert_eq!(rows.len(), 2);
            assert!(
                rows.iter().all(|r| r.trim().is_empty()),
                "{series:?} drew {rows:?}"
            );
        }
    }

    #[test]
    fn zero_geometry_does_not_panic() {
        assert!(
            plot_band(&bands(&[1.0]), &opts(0, 3), &plain())
                .iter()
                .all(String::is_empty)
        );
        assert!(plot_band(&bands(&[1.0]), &opts(4, 0), &plain()).is_empty());
    }

    /// More buckets than columns must not drop the extremes — squeezing a plot
    /// is exactly when a spike matters most.
    #[test]
    fn downsampling_preserves_extremes() {
        let mut series = bands(&[1.0; 100]);
        series[57] = Some((1.0, 900.0));
        let merged = resample(&series, 10);
        assert_eq!(merged.len(), 10);
        let peak = merged
            .iter()
            .flatten()
            .map(|&(_, hi)| hi)
            .fold(0.0, f64::max);
        assert_eq!(peak, 900.0);
    }

    /// Windows must partition rather than overlap: a shared sample would report
    /// one spike in two adjacent columns and widen it into a plateau.
    #[test]
    fn downsampling_windows_partition_the_input() {
        for (len, n) in [(7usize, 3usize), (100, 10), (5, 4), (13, 5)] {
            let mut spikes = 0;
            for i in 0..len {
                let mut series = bands(&vec![1.0; len]);
                series[i] = Some((1.0, 900.0));
                spikes += resample(&series, n)
                    .iter()
                    .flatten()
                    .filter(|&&(_, hi)| hi == 900.0)
                    .count();
            }
            assert_eq!(spikes, len, "len {len} into {n} columns duplicated samples");
        }
    }

    #[test]
    fn upsampling_repeats_rather_than_inventing_detail() {
        let merged = resample(&bands(&[1.0, 2.0]), 6);
        assert_eq!(merged.len(), 6);
        assert!(merged.iter().all(Option::is_some));
    }

    /// A stack is as tall as its summed columns. Scaling to the tallest single
    /// channel would push the total off the top of the plot.
    #[test]
    fn a_stack_scales_to_the_sum_not_the_tallest_channel() {
        let channels = vec![bands(&[3.0]), bands(&[3.0])];
        let rows = plot_stacked(&stack(&channels), &opts(1, 6), &Palette::plain());
        assert!(rows.iter().all(|r| r.chars().any(solid)), "{rows:?}");
    }

    /// Stack order is bottom-up, and each channel keeps its own cells — the only
    /// way a reader distinguishes a composition shift from a rate shift.
    #[test]
    fn stacked_channels_own_their_own_cells() {
        let raster = rasterize(&borrow(&[bands(&[1.0]), bands(&[3.0])]), &opts(1, 4));
        let bottom = raster.row_of(0);
        let top = raster.row_of(raster.sub_h - 1);
        assert_eq!(
            raster.at(0, bottom).map(|s| s.channel),
            Some(0),
            "bottom belongs to channel 0"
        );
        assert_eq!(
            raster.at(0, top).map(|s| s.channel),
            Some(1),
            "top belongs to channel 1"
        );
    }

    /// Rounding each channel on its own let small pools collide on one sub-cell
    /// and overwrite each other, so four active pools rendered as none. Every
    /// nonzero channel must hold ground of its own.
    #[test]
    fn small_channels_are_not_swallowed_by_a_large_one() {
        let channels: Vec<Vec<Band>> = [1.0, 1.0, 1.0, 1.0, 100.0]
            .iter()
            .map(|&v| bands(&[v]))
            .collect();
        let raster = rasterize(&borrow(&channels), &opts(1, 8));
        let owners: std::collections::BTreeSet<usize> = (0..raster.sub_h)
            .filter_map(|y| raster.at(0, y))
            .map(|s| s.channel)
            .collect();
        assert_eq!(
            owners,
            (0..5).collect(),
            "every active pool must own at least one sub-cell"
        );
    }

    /// The apportionment must exactly spend the height the total earns, however
    /// the channels divide it — that identity is what makes overwrites
    /// impossible rather than merely unlikely.
    #[test]
    fn apportionment_spends_exactly_the_total_height() {
        let cases: [&[f64]; 5] = [
            &[1.0, 1.0, 1.0],
            &[0.3, 0.3, 0.3, 99.1],
            &[50.0, 50.0],
            &[7.0],
            &[0.0, 0.0, 100.0],
        ];
        for values in cases {
            let ceiling: f64 = values.iter().sum();
            for sub_h in [4usize, 16, 32, 33] {
                let rows = apportion(values, ceiling, sub_h);
                assert_eq!(
                    rows.iter().sum::<usize>(),
                    sub_h,
                    "{values:?} into {sub_h} rows"
                );
            }
        }
    }

    /// The comparison rule is the whole differential story on a live plot. It
    /// used to be dropped whenever the axis auto-scaled — which is always.
    #[test]
    fn the_baseline_rule_is_drawn_on_an_auto_scaled_plot() {
        let o = opts(6, 4).baseline(50.0);
        let rows = plot_band(&bands(&[10.0]), &o, &plain());
        assert!(
            rows.iter().any(|r| r.contains('╌')),
            "no baseline drawn: {rows:?}"
        );
    }

    /// A baseline above everything observed pulls the scale rather than being
    /// clipped: "we are far below the run we are compared against" is the most
    /// important thing on the plot, and clipping renders it invisible.
    #[test]
    fn a_baseline_above_the_data_joins_the_scale() {
        let o = opts(4, 4).baseline(400.0);
        let rows = plot_band(&bands(&[10.0; 4]), &o, &plain());
        assert!(rows.iter().any(|r| r.contains('╌')), "{rows:?}");
        assert!(
            row_has(&rows, 3, solid) && !row_has(&rows, 0, solid),
            "data at 10 against a baseline of 400 must sit near the floor: {rows:?}"
        );
    }

    /// The rule must never sit on top of a sample — a comparison line that hid
    /// data would be worse than no line.
    #[test]
    fn the_baseline_never_overwrites_a_filled_cell() {
        let o = opts(4, 4).baseline(50.0);
        let rows = plot_band(&bands(&[100.0; 4]), &o, &plain());
        assert!(
            rows.iter()
                .all(|r| r.chars().any(solid) && !r.contains('╌')),
            "the plot fills every row, so the rule has nowhere to draw: {rows:?}"
        );
    }

    /// A live plot that rescales every frame jitters. Holding the axis at the
    /// largest ceiling shown so far keeps a quiet stretch looking quiet.
    #[test]
    fn at_least_holds_the_axis_against_a_quiet_stretch() {
        let quiet = bands(&[5.0]);
        let free = plot_band(&quiet, &opts(1, 8), &plain());
        let held = plot_band(&quiet, &opts(1, 8).at_least(100.0), &plain());
        assert!(
            free.iter().filter(|r| r.chars().any(solid)).count() >= 8,
            "a free axis fills to the top: {free:?}"
        );
        assert!(
            held.iter().filter(|r| r.chars().any(solid)).count() <= 1,
            "5 of a held 100 should barely register: {held:?}"
        );
        assert_eq!(
            ceiling(&[("", quiet.clone())], &opts(1, 8).at_least(100.0)),
            100.0
        );
        assert_eq!(ceiling(&[("", quiet)], &opts(1, 8)), 5.0);
    }

    #[test]
    fn each_mode_draws_from_its_own_glyph_family() {
        let series = bands(&[1.0, 2.0, 3.0, 4.0]);
        let braille = plot_band(&series, &PlotOpts::new(2, 1, GraphMode::Braille), &plain());
        assert!(
            braille[0]
                .chars()
                .all(|c| ('\u{2800}'..='\u{28FF}').contains(&c)),
            "{braille:?} is not braille"
        );
        let ascii = plot_band(&series, &PlotOpts::new(2, 1, GraphMode::Ascii), &plain());
        assert!(ascii[0].is_ascii(), "{ascii:?} is not ascii");
    }

    /// Construction guarantees the tables match the masks, so what is left to
    /// pin is the masks themselves: an empty cell, a full one, and the stipple
    /// actually being sparser than the solid it shadows.
    #[test]
    fn the_braille_masks_span_empty_to_full() {
        assert_eq!(BRAILLE_SOLID[0], '⠀', "no dots at level (0, 0)");
        assert_eq!(BRAILLE_SOLID[24], '⣿', "every dot at level (4, 4)");
        assert_eq!(BRAILLE_LIGHT[0], '⠀');
        let dots = |c: char| (c as u32 - 0x2800).count_ones();
        for i in 0..25 {
            assert!(
                dots(BRAILLE_LIGHT[i]) <= dots(BRAILLE_SOLID[i]),
                "the stipple must never outweigh the solid at index {i}"
            );
        }
        assert!(dots(BRAILLE_LIGHT[24]) < dots(BRAILLE_SOLID[24]));
    }

    /// Both glyph tables must be indexable at every level pair the folder can
    /// produce, or a full cell panics on a slice bound.
    #[test]
    fn every_level_pair_indexes_a_glyph() {
        for mode in [GraphMode::Braille, GraphMode::Ascii] {
            for fill in [Fill::Solid, Fill::Varying] {
                for l in 0..LEVELS {
                    for r in 0..LEVELS {
                        let _ = table(mode, fill)[l * LEVELS + r];
                    }
                }
            }
        }
    }

    /// A one-row plot still fills its full width, which is what any inline or
    /// footer-sized use depends on.
    #[test]
    fn a_single_row_plot_fills_its_width() {
        let rows = plot_band(&bands(&[1.0, 5.0, 3.0]), &opts(12, 1), &plain());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].chars().count(), 12);
    }

    /// The whole point of keying on names: a pool's colour is a property of the
    /// pool, not of which other pools happened to report. A chain with no sprout
    /// activity plots four bands, and sapling must still be sapling-coloured.
    #[test]
    fn a_pool_keeps_its_colour_when_another_pool_is_absent() {
        let palette = Palette::pools(true);
        let o = opts(2, 4);
        let full = plot_stacked(
            &[
                ("transparent", bands(&[1.0, 1.0])),
                ("sprout", bands(&[1.0, 1.0])),
                ("sapling", bands(&[4.0, 4.0])),
            ],
            &o,
            &palette,
        );
        let sparse = plot_stacked(
            &[
                ("transparent", bands(&[1.0, 1.0])),
                ("sapling", bands(&[4.0, 4.0])),
            ],
            &o,
            &palette,
        );
        let sapling = palette.style_of("sapling");
        let mark = 'x'.style(sapling).to_string();
        let colour = mark.split('x').next().unwrap().to_string();
        assert!(!colour.is_empty(), "the pool palette must be colourised");
        for (label, rendering) in [("full", &full), ("sprout absent", &sparse)] {
            assert!(
                rendering.iter().any(|r| r.contains(&colour)),
                "sapling lost its colour with {label}: {rendering:?}"
            );
        }
    }

    /// Every variation test above states its property over one channel. This is
    /// the case a pool stack actually hits: a channel that swung sitting under
    /// one that did not, where the majority rule decides each boundary cell.
    #[test]
    fn a_stack_marks_variation_per_channel_not_per_column() {
        let rows = plot_stacked(
            &[("a", vec![Some((0.0, 8.0))]), ("b", vec![Some((8.0, 8.0))])],
            &opts(1, 8),
            &plain(),
        );
        assert!(
            rows.iter().any(|r| r.chars().any(light)),
            "the channel that swung must be marked: {rows:?}"
        );
        assert!(
            rows.iter().any(|r| r.chars().any(solid)),
            "the steady channel above it must not be: {rows:?}"
        );
        let lowest_solid = rows.iter().rposition(|r| r.chars().any(solid)).unwrap();
        let highest_light = rows.iter().position(|r| r.chars().any(light)).unwrap();
        assert!(
            highest_light > lowest_solid,
            "the varying channel is below the steady one, so its marks sit lower: {rows:?}"
        );
    }

    /// A cell split evenly between placeable and unplaceable reports the doubt.
    /// Understating what we know beats asserting what we do not.
    #[test]
    fn a_cell_split_between_known_and_unplaceable_reads_as_unplaceable() {
        // Two columns: the first fixes the ceiling at 4, the second observes
        // half of it and then loses the channel above — so the known stack ends
        // exactly halfway up a four-row plot, mid-cell at the boundary.
        let rows = plot_stacked(
            &[
                ("a", vec![Some((2.0, 2.0)), Some((2.0, 2.0))]),
                ("b", vec![Some((2.0, 2.0)), None]),
            ],
            &opts(2, 4),
            &plain(),
        );
        let col: Vec<char> = rows.iter().map(|r| r.chars().nth(1).unwrap()).collect();
        assert!(col.contains(&'/'), "boundary must hatch: {col:?}");
    }

    /// The two knobs interact: holding the axis must not clip a baseline, and a
    /// baseline must not defeat a hold.
    #[test]
    fn at_least_and_baseline_both_reach_the_ceiling() {
        let series = vec![("a", bands(&[10.0]))];
        let o = opts(4, 4);
        assert_eq!(
            ceiling(&series, &o.clone().at_least(50.0).baseline(200.0)),
            200.0
        );
        assert_eq!(
            ceiling(&series, &o.clone().at_least(200.0).baseline(50.0)),
            200.0
        );
        let rows = plot_stacked(&series, &o.at_least(200.0).baseline(50.0), &plain());
        assert!(rows.iter().any(|r| r.contains('╌')), "{rows:?}");
    }

    /// A `NaN` band is a broken observation, not a magnitude. It must drop out
    /// rather than scale every level to `NaN` and paint a plausible plot of
    /// nothing.
    #[test]
    fn a_nan_band_renders_blank_rather_than_garbage() {
        let rows = plot_stacked(
            &[("a", vec![Some((f64::NAN, f64::NAN)), Some((5.0, 5.0))])],
            &opts(2, 4),
            &plain(),
        );
        assert_eq!(rows.len(), 4);
        assert!(
            rows.iter().all(|r| r.starts_with(' ')),
            "the NaN column must stay empty: {rows:?}"
        );
        assert!(
            rows.iter().any(|r| r.chars().nth(1).is_some_and(solid)),
            "the sound column must still draw: {rows:?}"
        );
    }

    /// `Fill::ALL` and `Fill::index` are two statements of one order; folding
    /// reads the tally by index and maps back through `ALL`, so a disagreement
    /// would silently swap what a cell reports.
    #[test]
    fn the_fill_order_agrees_with_its_index() {
        for (i, fill) in Fill::ALL.iter().enumerate() {
            assert_eq!(fill.index(), i, "{fill:?}");
        }
    }

    /// A channel the palette does not name still plots. A renderer must not be
    /// the thing that takes a run down.
    #[test]
    fn an_unnamed_channel_renders_in_the_fallback_colour() {
        let palette = Palette::pools(true);
        let rows = plot_stacked(&[("blocks", bands(&[1.0, 2.0]))], &opts(2, 4), &palette);
        assert!(rows.iter().any(|r| !r.trim().is_empty()), "{rows:?}");
    }

    /// A stack deeper than the palette must keep drawing rather than panicking
    /// on the index.
    #[test]
    fn a_channel_beyond_the_palette_still_renders() {
        let channels = vec![bands(&[1.0]), bands(&[1.0]), bands(&[1.0])];
        let raster = rasterize(&borrow(&channels), &opts(2, 3));
        let styles = vec![Style::new(); channels.len()];
        let rows = render(&raster, &opts(2, 3), &Palette::plain(), &styles);
        assert!(rows.iter().any(|r| r.chars().any(solid)), "{rows:?}");
    }
}
