//! Terminal time-series plots (`ztest sync status`, live dashboard).
//!
//! - Series share one caller-fixed axis → a column means the same instant in every
//!   plot on screen ([`Timeline`](crate::sync::Timeline) supplies the bucketing)
//! - All modes rasterise 2 sub-cols × 4 sub-rows; cell = `table[left*5 + right]`
//!   (btop `Draw::Graph::_create`; tiers differ in data only, no per-cell branch)
//! - Colour = channel identity, never magnitude (`ops/s` has no ceiling to gradient)
//! - One colour per character (split cell → majority owner)
//! - Variation zone carried by glyph, not attribute ([`Fill`]) → survives `NO_COLOR`
//!   terminals and captured CI logs

use owo_colors::{OwoColorize as _, Style};

/// One bucket's observed range, `None` = nothing observed. Never interpolated
/// across (during a partition the absence *is* the observation)
pub type Band = Option<(f64, f64)>;

const SUB_COLS: usize = 2;
const SUB_ROWS: usize = 4;
const LEVELS: usize = SUB_ROWS + 1;

/// Glyph tier a terminal can render = table selector only (all modes rasterise
/// alike). `Braille` packs 2×4 dots per character, needs a braille-capable font
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphMode {
    Braille,
    Ascii,
}

/// What a filled sub-cell represents.
///
/// - `Solid` up to the bucket minimum = throughput that never dropped below it
/// - `Varying` above it = the swing (90s stall in a half-hour bucket reaches the floor)
/// - `Unknown` = above an unobserved channel, unplaceable
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fill {
    Solid,
    Varying,
    Unknown,
}

impl Fill {
    /// Every fill by **increasing uncertainty**.
    ///
    /// - Indexes the [`fold`] tally; ties break to the later entry (split cell reports doubt)
    /// - Both readings come from here, not the discriminant → reordering cannot invert them
    const ALL: [Fill; 3] = [Fill::Solid, Fill::Varying, Fill::Unknown];

    const fn index(self) -> usize {
        match self {
            Fill::Solid => 0,
            Fill::Varying => 1,
            Fill::Unknown => 2,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Sub {
    channel: usize,
    fill: Fill,
}

// ────────────────────────────── glyph tables ──────────────────────────────

/// Braille dot bits by fill level: bottom `level` dots of the left cell column.
/// Dots run 1-2-3-7 downward (bottom = 7, `0x40`), so not sequential. `[solid, stippled]`
const BRAILLE_LEFT: [[u8; LEVELS]; 2] =
    [[0x00, 0x40, 0x44, 0x46, 0x47], [0x00, 0x40, 0x40, 0x42, 0x42]];
/// As [`BRAILLE_LEFT`], right cell column (dots 4-5-6-8)
const BRAILLE_RIGHT: [[u8; LEVELS]; 2] =
    [[0x00, 0x80, 0xA0, 0xB0, 0xB8], [0x00, 0x80, 0x80, 0x90, 0x90]];

/// OR the two columns' dots into the braille block. Derived, not transcribed like
/// btop's 25 literal glyphs (masks = whole spec → transcription error unrepresentable)
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

/// Braille, solid = btop's `braille_up`
const BRAILLE_SOLID: [char; 25] = braille_table(0);

/// Braille, variation zone: same silhouette stippled to alternate dots (zones stay
/// separable without colour). Halved dots collapse levels 1/2 and 3/4 — free, a band
/// edge was never meaningful to a quarter-cell
const BRAILLE_LIGHT: [char; 25] = braille_table(1);

/// ASCII, solid: btop's `tty_up` density ladder ` .+#`
const ASCII_SOLID: [char; 25] = [
    ' ', '.', '.', '+', '+', //
    '.', '.', '+', '+', '#', //
    '.', '+', '+', '+', '#', //
    '+', '+', '+', '#', '#', //
    '+', '#', '#', '#', '#',
];

/// ASCII, variation zone: same ladder one step lighter (`#` never means "swung",
/// `:` never "steady")
const ASCII_LIGHT: [char; 25] = [
    ' ', '.', '.', '.', '.', //
    '.', '.', '.', '.', ':', //
    '.', '.', '.', '.', ':', //
    '.', '.', '.', ':', ':', //
    '.', ':', ':', ':', ':',
];

/// Hatch for the unplaceable region above an unobserved channel. Not level-indexed
/// (region has no magnitude); hatch = "no data", blank = "zero"
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

/// Per-channel colours keyed **by name**, not stack position (callers drop empty
/// channels → positional keying reshuffles every hue above the gap). `colorize`
/// false emits no attribute at all
#[derive(Clone, Debug)]
pub struct Palette {
    channels: Vec<(&'static str, Style)>,
    default: Style,
    pub axis: Style,
    pub baseline: Style,
    colorize: bool,
}

impl Palette {
    /// Pool palette, oldest pool first to match
    /// [`Work::rows`](crate::sync::Work::rows) stacking; hues stay distinguishable when adjacent
    pub fn pools(colorize: bool) -> Palette {
        match colorize {
            false => Palette::plain(),
            true => Palette {
                channels: vec![
                    ("transparent", Style::new().bright_black()),
                    ("transparent in", Style::new().bright_black()),
                    ("transparent out", Style::new().bright_black()),
                    ("sprout", Style::new().magenta()),
                    ("sapling", Style::new().yellow()),
                    ("sapling spends", Style::new().yellow()),
                    ("sapling outputs", Style::new().yellow()),
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

    pub fn plain() -> Palette {
        Palette {
            channels: Vec::new(),
            default: Style::new(),
            axis: Style::new(),
            baseline: Style::new(),
            colorize: false,
        }
    }

    /// Colour for structure set among data (chip separators). Blue: free of every pool
    /// colour, so a separator never reads as a layer
    pub fn separator(&self) -> Style {
        self.baseline
    }

    pub fn style_of(&self, channel: &str) -> Style {
        self.channels
            .iter()
            .find(|(name, _)| *name == channel)
            .map(|(_, style)| *style)
            .unwrap_or(self.default)
    }

    /// Additive polish over the lighter glyph table, never the only signal
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
/// - No ceiling knob (everything plotted = unbounded rate) → y-axis auto-scales to data
///   and `baseline`, so `baseline` is always on scale
/// - `at_least` fed back by the caller against per-frame rescale jitter (memory stays
///   outside → this module is a pure function)
#[derive(Clone, Debug)]
pub struct PlotOpts {
    pub width: usize,
    pub height: usize,
    pub mode: GraphMode,
    pub baseline: Option<f64>,
    pub at_least: Option<f64>,
}

impl PlotOpts {
    pub fn new(width: usize, height: usize, mode: GraphMode) -> PlotOpts {
        PlotOpts { width, height, mode, baseline: None, at_least: None }
    }

    #[cfg(test)]
    pub fn baseline(mut self, value: f64) -> PlotOpts {
        self.baseline = Some(value);
        self
    }

    /// Hold the axis at `value` or above; pair with [`ceiling`] to ratchet
    #[cfg(test)]
    pub fn at_least(mut self, value: f64) -> PlotOpts {
        self.at_least = Some(value);
        self
    }
}

// ──────────────────────────────── plotting ────────────────────────────────

/// One named series in a stack: `(name, bands)`. Name travels with the data (the
/// palette keys on it, position implies nothing)
pub type Channel<'a> = (&'a str, Vec<Band>);

/// Stacked area plot: `channels[0]` on the bottom, envelope = their sum.
///
/// - Stacked not overlaid (distinguishes a *composition* shift from a *rate* shift)
/// - Channel the palette does not name still plots, in the fallback colour
pub fn plot_stacked(channels: &[Channel<'_>], opts: &PlotOpts, palette: &Palette) -> Vec<String> {
    let series: Vec<&[Band]> = channels.iter().map(|(_, b)| b.as_slice()).collect();
    // Per plot, not per cell (name lookup = linear scan, fold runs width × height/frame)
    let styles: Vec<Style> = channels.iter().map(|(name, _)| palette.style_of(name)).collect();
    render(&rasterize(&series, opts), opts, palette, &styles)
}

/// Ceiling [`plot_stacked`] would auto-scale to. Exposed for ratcheting back through
/// [`PlotOpts::at_least`] and for labelling an axis gutter with the plot's own number
pub fn ceiling(channels: &[Channel<'_>], opts: &PlotOpts) -> f64 {
    let sub_w = opts.width * SUB_COLS;
    let resampled: Vec<Vec<Band>> = channels.iter().map(|(_, b)| resample(b, sub_w)).collect();
    let borrowed: Vec<&[Band]> = resampled.iter().map(Vec::as_slice).collect();
    ceiling_of(&borrowed, opts)
}

// ───────────────────────────── rasterisation ─────────────────────────────

/// Sub-cell grid, row 0 at the **top**. `ceiling` = what that top row stands for,
/// resolved once (baseline rule and fill cannot disagree about scale)
struct Raster {
    sub_w: usize,
    sub_h: usize,
    ceiling: f64,
    cells: Vec<Option<Sub>>,
}

impl Raster {
    fn at(&self, x: usize, y: usize) -> Option<Sub> {
        self.cells.get(y * self.sub_w + x).copied().flatten()
    }

    /// Fill level `l` counted from the bottom, as a grid row
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
    // Flat-zero/empty = no scale: empty raster → empty frame ("nothing observed"),
    // where /0 would paint a full-height block claiming the opposite.
    // NaN tested explicitly (propagates out of an empty fold, slips past `<= 0.0`)
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
/// - Floor depends only on channels *below* → an unobserved one invalidates itself and up
/// - Observed prefix drawn in place, hatch from its top to the ceiling (blanking discards
///   real observations, stacking on asserts a total nobody measured)
fn paint_column(raster: &mut Raster, x: usize, channels: &[&[Band]]) {
    let column: Vec<Band> = channels.iter().map(|c| c.get(x).copied().flatten()).collect();

    // Nothing observed at all = gap, not an unplaceable stack
    if column.iter().all(Option::is_none) {
        return;
    }

    let known = column.iter().position(Option::is_none).unwrap_or(column.len());
    let bands: Vec<(f64, f64)> = column[..known].iter().flatten().copied().collect();
    let rows = apportion(
        &bands.iter().map(|&(_, hi)| hi).collect::<Vec<_>>(),
        raster.ceiling,
        raster.sub_h,
    );

    let mut level = 0usize;
    for (channel, (&(lo, hi), &n)) in bands.iter().zip(rows.iter()).enumerate() {
        // Solid part = this channel's own [0, lo] scaled into its granted rows
        // (split cannot exceed the segment; zero-height channel → vacuous)
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
            raster.set(x, l, Sub { channel: known, fill: Fill::Unknown });
        }
    }
}

/// Split `sub_h` sub-rows across `values` by largest remainder. Per-channel rounding
/// loses the total, letting small pools collide on one sub-cell and overwrite it
fn apportion(values: &[f64], ceiling: f64, sub_h: usize) -> Vec<usize> {
    let exact: Vec<f64> =
        values.iter().map(|v| (v / ceiling).clamp(0.0, 1.0) * sub_h as f64).collect();
    // Any positive total claims a sub-row, however small (a rate dwarfed by the
    // baseline it is compared against must not round away into a bare axis)
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

    // Tiny-but-active must not render as idle → lend a row from the largest holder
    for i in 0..rows.len() {
        if rows[i] > 0 || values[i] <= 0.0 {
            continue;
        }
        let Some(donor) = (0..rows.len()).max_by_key(|&j| rows[j]).filter(|&j| rows[j] > 1) else {
            break;
        };
        rows[donor] -= 1;
        rows[i] += 1;
    }
    rows
}

/// Largest column total the plot must fit, never below the baseline.
///
/// - Summed across channels, not the tallest single one (total would run off the top)
/// - Baseline joins the scale, never clipped (a 3× faster comparison run = the headline)
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
/// - Down: union the bands sharing a column (extreme survives; a sub-column gap is
///   absorbed, so size the timeline to the plot if every partition must show)
/// - Up: repeat (honest about there being no finer data)
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
    (0..opts.height).map(|row| render_row(raster, opts, palette, styles, row, baseline)).collect()
}

/// Character row the comparison rule falls on. Always on the plot ([`ceiling_of`]
/// folded the baseline into the scale)
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
            // Empty cell on the baseline row carries the rule, elsewhere blank field.
            // Drawn only where nothing was observed → the rule can never hide data
            None if baseline == Some(row) => out.push_str(&'╌'.style(palette.baseline).to_string()),
            None => out.push(' '),
        }
    }
    out
}

/// Fold one character cell, `None` if nothing in it is filled
fn fold(
    raster: &Raster,
    palette: &Palette,
    styles: &[Style],
    mode: GraphMode,
    row: usize,
    col: usize,
) -> Option<String> {
    // Cell holds SUB_COLS × SUB_ROWS sub-cells = max distinct channels in one, whatever
    // the stack depth. Sizing from the cell's own geometry keeps this allocation-free on
    // a per-frame width × height path *and* exact for any channel count
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

    // Ties → the more uncertain reading, hence the *later* `Fill::ALL` entry
    // (understating what we know beats asserting what we do not).
    // Owner vote below prefers the *lower* channel: both pools real, floor-nearest is stable
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

    /// Single-channel plot. Test affordance, not API (production callers always stack)
    fn plot_band(bands: &[Band], opts: &PlotOpts, palette: &Palette) -> Vec<String> {
        plot_stacked(&[("", bands.to_vec())], opts, palette)
    }

    /// Name a list of series positionally, for geometry tests rather than colour ones
    fn stack(series: &[Vec<Band>]) -> Vec<Channel<'static>> {
        const NAMES: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
        series.iter().enumerate().map(|(i, b)| (NAMES[i], b.clone())).collect()
    }

    /// Borrow series for the rasteriser (slices, so the public entry point need not clone)
    fn borrow(series: &[Vec<Band>]) -> Vec<&[Band]> {
        series.iter().map(Vec::as_slice).collect()
    }

    fn plain() -> Palette {
        Palette::plain()
    }

    /// Any solid-ASCII-ladder glyph ("is there mass here", not which density step)
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

    /// Bigger values reach higher rows; every column = filled mass to the axis, not a line
    #[test]
    fn taller_values_occupy_higher_rows() {
        let rows = plot_band(&bands(&[1.0, 4.0]), &opts(2, 4), &plain());
        let cell = |r: usize, c: usize| rows[r].chars().nth(c).unwrap();
        assert_eq!(cell(0, 0), ' ', "the small value must not reach the top");
        assert!(solid(cell(0, 1)), "the large value must reach the top");
        assert!(solid(cell(3, 0)), "every column is anchored at the axis");
        assert!(solid(cell(3, 1)));
    }

    /// Halves separable **without colour** (collapsed, a bucket holding a stall looks steady)
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

    /// Every mode separates zones by glyph (plain palette emits no attributes, CI log
    /// has no colour to read)
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

    /// Painting through a gap erases the partition or stall the plot exists to show
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

    /// Observed prefix keeps its place, everything above the gap = unplaceable not absent
    #[test]
    fn a_partly_observed_column_keeps_its_prefix_and_hatches_the_rest() {
        let channels = vec![vec![Some((5.0, 5.0)), Some((5.0, 5.0))], vec![Some((5.0, 5.0)), None]];
        let rows = plot_stacked(&stack(&channels), &opts(2, 8), &Palette::plain());
        let col1: Vec<char> = rows.iter().map(|r| r.chars().nth(1).unwrap()).collect();
        assert!(col1.contains(&'/'), "the unplaceable region must be hatched: {col1:?}");
        assert!(
            col1.iter().any(|&c| solid(c)),
            "the observed channel must still be drawn: {col1:?}"
        );
        assert!(
            col1.last().is_some_and(|&c| solid(c)),
            "the observed channel keeps the floor, hatch sits above it: {col1:?}"
        );
    }

    /// Nothing observed = gap, not unplaceable (else a not-yet-started run is solid hatch)
    #[test]
    fn a_wholly_unobserved_column_is_blank_not_hatched() {
        let channels = vec![vec![Some((5.0, 5.0)), None], vec![Some((5.0, 5.0)), None]];
        let rows = plot_stacked(&stack(&channels), &opts(2, 4), &Palette::plain());
        assert!(rows.iter().all(|r| r.chars().nth(1) == Some(' ')), "{rows:?}");
    }

    /// Steady rate = zero band thickness, must still draw ("no variation" != "no data")
    #[test]
    fn a_zero_thickness_band_still_draws() {
        let rows = plot_band(&[Some((7.0, 7.0))], &opts(1, 4), &plain());
        assert!(rows.iter().any(|r| r.chars().any(solid)), "{rows:?}");
    }

    /// Why buckets keep min and max: peak+stall must swing visibly more than steady-to-same-peak
    #[test]
    fn a_wide_band_shows_more_variation_than_a_narrow_one() {
        let swing = |b: Band| {
            plot_band(&[b], &opts(1, 8), &plain()).iter().filter(|r| r.chars().any(light)).count()
        };
        assert!(
            swing(Some((0.0, 100.0))) > swing(Some((90.0, 100.0))),
            "a stall must not look like steady throughput"
        );
    }

    /// Empty frame, never /0 painting a full block nobody observed
    #[test]
    fn an_empty_or_flat_zero_series_renders_an_empty_frame() {
        for series in [vec![], vec![None; 4], bands(&[0.0, 0.0])] {
            let rows = plot_band(&series, &opts(4, 2), &plain());
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|r| r.trim().is_empty()), "{series:?} drew {rows:?}");
        }
    }

    #[test]
    fn zero_geometry_does_not_panic() {
        assert!(plot_band(&bands(&[1.0]), &opts(0, 3), &plain()).iter().all(String::is_empty));
        assert!(plot_band(&bands(&[1.0]), &opts(4, 0), &plain()).is_empty());
    }

    /// More buckets than columns keeps extremes (a squeeze = when a spike matters most)
    #[test]
    fn downsampling_preserves_extremes() {
        let mut series = bands(&[1.0; 100]);
        series[57] = Some((1.0, 900.0));
        let merged = resample(&series, 10);
        assert_eq!(merged.len(), 10);
        let peak = merged.iter().flatten().map(|&(_, hi)| hi).fold(0.0, f64::max);
        assert_eq!(peak, 900.0);
    }

    /// Windows partition, never overlap (a shared sample widens one spike into a plateau)
    #[test]
    fn downsampling_windows_partition_the_input() {
        for (len, n) in [(7usize, 3usize), (100, 10), (5, 4), (13, 5)] {
            let mut spikes = 0;
            for i in 0..len {
                let mut series = bands(&vec![1.0; len]);
                series[i] = Some((1.0, 900.0));
                spikes +=
                    resample(&series, n).iter().flatten().filter(|&&(_, hi)| hi == 900.0).count();
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

    /// Scaling to the tallest single channel pushes the total off the top
    #[test]
    fn a_stack_scales_to_the_sum_not_the_tallest_channel() {
        let channels = vec![bands(&[3.0]), bands(&[3.0])];
        let rows = plot_stacked(&stack(&channels), &opts(1, 6), &Palette::plain());
        assert!(rows.iter().all(|r| r.chars().any(solid)), "{rows:?}");
    }

    /// Bottom-up, each channel keeping its own cells = how a composition shift reads
    /// apart from a rate shift
    #[test]
    fn stacked_channels_own_their_own_cells() {
        let raster = rasterize(&borrow(&[bands(&[1.0]), bands(&[3.0])]), &opts(1, 4));
        let bottom = raster.row_of(0);
        let top = raster.row_of(raster.sub_h - 1);
        assert_eq!(raster.at(0, bottom).map(|s| s.channel), Some(0), "bottom belongs to channel 0");
        assert_eq!(raster.at(0, top).map(|s| s.channel), Some(1), "top belongs to channel 1");
    }

    /// Per-channel rounding let small pools collide on one sub-cell and overwrite each
    /// other → four active pools rendered as none
    #[test]
    fn small_channels_are_not_swallowed_by_a_large_one() {
        let channels: Vec<Vec<Band>> =
            [1.0, 1.0, 1.0, 1.0, 100.0].iter().map(|&v| bands(&[v])).collect();
        let raster = rasterize(&borrow(&channels), &opts(1, 8));
        let owners: std::collections::BTreeSet<usize> =
            (0..raster.sub_h).filter_map(|y| raster.at(0, y)).map(|s| s.channel).collect();
        assert_eq!(owners, (0..5).collect(), "every active pool must own at least one sub-cell");
    }

    /// Spending exactly the earned height = what makes overwrites impossible, not unlikely
    #[test]
    fn apportionment_spends_exactly_the_total_height() {
        let cases: [&[f64]; 5] =
            [&[1.0, 1.0, 1.0], &[0.3, 0.3, 0.3, 99.1], &[50.0, 50.0], &[7.0], &[0.0, 0.0, 100.0]];
        for values in cases {
            let ceiling: f64 = values.iter().sum();
            for sub_h in [4usize, 16, 32, 33] {
                let rows = apportion(values, ceiling, sub_h);
                assert_eq!(rows.iter().sum::<usize>(), sub_h, "{values:?} into {sub_h} rows");
            }
        }
    }

    /// Rule = the whole differential story on a live plot, and the axis always auto-scales
    #[test]
    fn the_baseline_rule_is_drawn_on_an_auto_scaled_plot() {
        let o = opts(6, 4).baseline(50.0);
        let rows = plot_band(&bands(&[10.0]), &o, &plain());
        assert!(rows.iter().any(|r| r.contains('╌')), "no baseline drawn: {rows:?}");
    }

    /// Baseline above the data pulls the scale, never clips ("far below the run we are
    /// compared against" is the plot's headline, and clipping hides it)
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

    /// A comparison line that hid a sample would be worse than no line
    #[test]
    fn the_baseline_never_overwrites_a_filled_cell() {
        let o = opts(4, 4).baseline(50.0);
        let rows = plot_band(&bands(&[100.0; 4]), &o, &plain());
        assert!(
            rows.iter().all(|r| r.chars().any(solid) && !r.contains('╌')),
            "the plot fills every row, so the rule has nowhere to draw: {rows:?}"
        );
    }

    /// Per-frame rescale jitters; holding at the largest ceiling shown keeps quiet quiet
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
        assert_eq!(ceiling(&[("", quiet.clone())], &opts(1, 8).at_least(100.0)), 100.0);
        assert_eq!(ceiling(&[("", quiet)], &opts(1, 8)), 5.0);
    }

    #[test]
    fn each_mode_draws_from_its_own_glyph_family() {
        let series = bands(&[1.0, 2.0, 3.0, 4.0]);
        let braille = plot_band(&series, &PlotOpts::new(2, 1, GraphMode::Braille), &plain());
        assert!(
            braille[0].chars().all(|c| ('\u{2800}'..='\u{28FF}').contains(&c)),
            "{braille:?} is not braille"
        );
        let ascii = plot_band(&series, &PlotOpts::new(2, 1, GraphMode::Ascii), &plain());
        assert!(ascii[0].is_ascii(), "{ascii:?} is not ascii");
    }

    /// Construction already matches tables to masks → pin the masks: empty, full, and
    /// stipple sparser than the solid it shadows
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

    /// Every level pair the folder can produce, else a full cell panics on a slice bound
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

    /// What any inline or footer-sized use depends on
    #[test]
    fn a_single_row_plot_fills_its_width() {
        let rows = plot_band(&bands(&[1.0, 5.0, 3.0]), &opts(12, 1), &plain());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].chars().count(), 12);
    }

    /// Colour = property of the pool, not of which other pools happened to report
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
            &[("transparent", bands(&[1.0, 1.0])), ("sapling", bands(&[4.0, 4.0]))],
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

    /// Case a pool stack actually hits: a channel that swung under one that did not,
    /// majority rule deciding each boundary cell
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

    /// Even split reports the doubt (understating what we know beats asserting what we do not)
    #[test]
    fn a_cell_split_between_known_and_unplaceable_reads_as_unplaceable() {
        // Col 0 fixes the ceiling at 4; col 1 observes half then loses the channel
        // above → known stack ends halfway up a four-row plot, mid-cell at the boundary
        let rows = plot_stacked(
            &[("a", vec![Some((2.0, 2.0)), Some((2.0, 2.0))]), ("b", vec![Some((2.0, 2.0)), None])],
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
        assert_eq!(ceiling(&series, &o.clone().at_least(50.0).baseline(200.0)), 200.0);
        assert_eq!(ceiling(&series, &o.clone().at_least(200.0).baseline(50.0)), 200.0);
        let rows = plot_stacked(&series, &o.at_least(200.0).baseline(50.0), &plain());
        assert!(rows.iter().any(|r| r.contains('╌')), "{rows:?}");
    }

    /// `NaN` band = broken observation, not a magnitude (must drop out, not scale every
    /// level to `NaN` and paint a plausible plot of nothing)
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

    /// `Fill::ALL` and `Fill::index` = two statements of one order (fold indexes the
    /// tally, maps back through `ALL` → disagreement silently swaps what a cell reports)
    #[test]
    fn the_fill_order_agrees_with_its_index() {
        for (i, fill) in Fill::ALL.iter().enumerate() {
            assert_eq!(fill.index(), i, "{fill:?}");
        }
    }

    /// A renderer must not be the thing that takes a run down
    #[test]
    fn an_unnamed_channel_renders_in_the_fallback_colour() {
        let palette = Palette::pools(true);
        let rows = plot_stacked(&[("blocks", bands(&[1.0, 2.0]))], &opts(2, 4), &palette);
        assert!(rows.iter().any(|r| !r.trim().is_empty()), "{rows:?}");
    }

    /// Deeper than the palette keeps drawing, never panics on the index
    #[test]
    fn a_channel_beyond_the_palette_still_renders() {
        let channels = vec![bands(&[1.0]), bands(&[1.0]), bands(&[1.0])];
        let raster = rasterize(&borrow(&channels), &opts(2, 3));
        let styles = vec![Style::new(); channels.len()];
        let rows = render(&raster, &opts(2, 3), &Palette::plain(), &styles);
        assert!(rows.iter().any(|r| r.chars().any(solid)), "{rows:?}");
    }
}
