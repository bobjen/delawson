//! Logging into rotating files, twice for everything but the chatty lines.
//!
//! Every line goes to the full log. The info log takes a target share of that —
//! a tenth by default — so the same bytes reach back hours in one and days in the
//! other. Rare lines pass through whole; the busy sites are sampled, at a stride
//! worked out from a running histogram so the total comes out near the target.
//! Sampling rather than suppressing means the info log still shows what the busy
//! traffic looks like, which is usually what a reader wants from it.
//!
//! Two ways to reach the info log, and they compose. Say `notice` and the line
//! is always in both, whatever its rate; say `info` and the histogram decides.
//! The second is what keeps a line that was quiet when it was written from
//! flooding the info log once the system is busy.
//!
//! **Write a new line at `notice`, and demote it only once a histogram proves it
//! noisy.** The aim is many lines in the code and few in the log, which works
//! because most sites are rare: the ladder is a demotion path rather than a
//! taxonomy —
//!
//! | Level | Where it goes |
//! |---|---|
//! | `notice` | both logs, always: where a line starts |
//! | `info` | the full log, sampled into the info log |
//! | `debug` | nowhere, unless someone turns the level down |
//!
//! Every macro invocation gets its own `Site`, so counting a line is one relaxed
//! increment and deciding where it goes is one relaxed load.
//!
//! **A line is two messages: a constant and the values.** The constant is what the
//! histogram groups on, exactly, so grouping needs no guess about where the words
//! stop and the values start. Put the values in the order that makes their
//! prefixes worth grouping on too — the low-cardinality ones first.
//!
//! The constant is a field of a comma-separated line, so it may not contain a
//! comma. Say "because" rather than ", ".
//!
//! Separate the values with commas too — `"op {},status {}"` — so a line is a row
//! of fields rather than a sentence.
//!
//! **An outcome worth finding deserves its own constant.** A histogram separates
//! kinds by constant, so a refusal logged with the same constant as a success is
//! one row among a million and invisible; logged with its own, it is a row with a
//! count of one, which is how a rare thing is found by someone who does not know
//! what they are looking for.

use crate::{Context, Result};
use std::fmt::Write as _;
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug,
    Info,
    Notice,
    Warning,
    Error,
}

impl Level {
    /// One letter, so the level costs one column.
    pub fn letter(self) -> char {
        match self {
            Level::Debug => 'd',
            Level::Info => 'i',
            Level::Notice => 'n',
            Level::Warning => 'w',
            Level::Error => 'e',
        }
    }

    pub fn parse(name: &str) -> Option<Level> {
        Some(match name {
            "debug" => Level::Debug,
            "info" => Level::Info,
            "notice" => Level::Notice,
            "warning" => Level::Warning,
            "error" => Level::Error,
            _ => return None,
        })
    }
}

/// One place in the code that logs. The macros make one of these per call site,
/// which is what makes counting cheap.
pub struct Site {
    pub file: &'static str,
    pub line: u32,
    count: AtomicU64,
    /// One line in this many reaches the info log. One means all of them.
    stride: AtomicU64,
    /// For a rate-limited site: the second in which it last said anything.
    said: AtomicU64,
    /// How many times a rate-limited site was asked and said nothing. One counter
    /// for every thread: measured, a relaxed increment on it costs nothing worth
    /// seeing even at millions of calls a second.
    skipped: AtomicU64,
    listed: AtomicBool,
    /// The start of the first line this site produced, for reading histograms.
    text: OnceLock<String>,
}

impl Site {
    pub const fn new(file: &'static str, line: u32) -> Site {
        Site {
            file,
            line,
            count: AtomicU64::new(0),
            stride: AtomicU64::new(1),
            said: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            listed: AtomicBool::new(false),
            text: OnceLock::new(),
        }
    }
}

static SITES: Mutex<Vec<&'static Site>> = Mutex::new(Vec::new());

/// Lines written since the hot set was last worked out.
static SINCE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct Logging {
    pub level: Level,
    /// Bytes of lines to accumulate before writing them. Derived from what the log
    /// disk wants of a write and what the lines compress by, so a compressed block
    /// lands at the device's size rather than a sixth of it.
    pub block: usize,
    /// Store the files compressed. Nothing reads them without the log tool, so
    /// there is nothing left for plain text to be convenient for — and lines
    /// compress several fold, which is the difference between minutes and hours
    /// of retention for the same bytes.
    pub compress: bool,
    pub size: u64,
    pub full_files: usize,
    pub info_files: usize,
    /// The share of the full log's lines the info log should carry.
    pub info_share: f64,
    /// Lines between recounts.
    pub recount: usize,
}

#[derive(Clone, Copy)]
enum Compression {
    Plain,
    Zstd,
}

struct Logger {
    config: Logging,
    full: Mutex<Rotating>,
    info: Mutex<Rotating>,
}

/// The current second, kept by the ticker so a rate-limited line costs a load
/// rather than a clock read.
static SECOND: AtomicU64 = AtomicU64::new(0);

/// How long a line may sit in a buffer before it reaches the disk. A crash loses
/// what is still buffered, which is why anything at warning or above is flushed
/// as it is written.
const FLUSH_MS: u64 = 200;

/// How often the second is refreshed. Rate limiting is to the nearest tick,
/// which is close enough for something whose point is "not more than about once
/// a second".
const TICK_MS: u64 = 100;

static LOGGER: OnceLock<Logger> = OnceLock::new();

/// The floor, kept apart from the rest of the configuration so that deciding
/// whether to log is one relaxed load and a compare. A filtered line must cost
/// nothing worth measuring, or it cannot be left in the hot path.
static FLOOR: AtomicU8 = AtomicU8::new(Level::Info as u8);

pub fn enabled(level: Level) -> bool {
    level as u8 >= FLOOR.load(Ordering::Relaxed)
}

/// Start logging into `dir`. Until this is called — and in the client, which
/// never calls it — lines go to standard error.
pub fn open(dir: &Path, identity: &str, config: Logging) -> Result<()> {
    let name = identity.to_lowercase();
    // The same shape as every other line, so the reader needs no special case for
    // it: level `h`, no source location, and the constant `opened`.
    let header = format!(
        "h,{},pid={},tid=0,file=,line=0,opened,identity {identity},host {},version {},times are UTC\n",
        stamp(),
        std::process::id(),
        host(),
        env!("CARGO_PKG_VERSION"),
    );
    let compression = if config.compress { Compression::Zstd } else { Compression::Plain };
    let full = Rotating::new(
        dir,
        &name,
        config.size,
        config.full_files,
        header.clone(),
        compression,
        config.block,
    )?;
    let info = Rotating::new(
        dir,
        &format!("{name}-info"),
        config.size,
        config.info_files,
        header,
        compression,
        config.block,
    )?;
    FLOOR.store(config.level as u8, Ordering::Relaxed);
    let _ = LOGGER.set(Logger { config, full: Mutex::new(full), info: Mutex::new(info) });

    tick();
    Ok(())
}

pub fn level() -> Level {
    LOGGER.get().map(|logger| logger.config.level).unwrap_or(Level::Info)
}

/// Set the floor without opening files, for measuring what a filtered line costs.
pub fn set_floor(level: Level) {
    FLOOR.store(level as u8, Ordering::Relaxed);
}

thread_local! {
    static LINE: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    /// The kernel's thread id, so a line can be matched with what perf, gdb and
    /// top say. Asked for once per thread.
    static TID: i64 = unsafe { libc::syscall(libc::SYS_gettid) };
}

static PID: OnceLock<u32> = OnceLock::new();

/// One thread keeps the clock and pushes the buffers, started by whichever comes
/// first: a logger opening, or a rate-limited line needing to know the second.
fn tick() {
    static TICKING: AtomicBool = AtomicBool::new(false);
    if TICKING.swap(true, Ordering::Relaxed) {
        return;
    }
    SECOND.store(now_secs(), Ordering::Relaxed);
    std::thread::spawn(|| {
        let mut ticks = 0u64;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(TICK_MS));
            SECOND.store(now_secs(), Ordering::Relaxed);
            ticks += 1;
            if ticks.is_multiple_of(FLUSH_MS / TICK_MS) {
                flush();
            }
        }
    });
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// One line a second from this site, and the first of the second is the one that
/// gets through, carrying how many times the site was asked and said nothing. For a
/// place that would otherwise say the same thing millions of times: a sample says
/// what is happening, and the count says how much of it there was.
///
/// The count is one shared counter, incremented by whichever thread was refused.
/// Per-thread counters were tried and made no difference — 12.83M against 12.71M
/// round trips a second — so the simpler thing stays.
pub fn sample(site: &'static Site, level: Level, what: &'static str, values: std::fmt::Arguments) {
    let now = match SECOND.load(Ordering::Relaxed) {
        // Nothing has started the clock, so this line pays for it once.
        0 => {
            tick();
            now_secs()
        }
        second => second,
    };
    let said = site.said.load(Ordering::Relaxed);
    if said == now {
        site.skipped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    // Whichever thread claims the second writes the line; the rest carry on.
    if site.said.compare_exchange(said, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        site.skipped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    emit(site, level, what, values, Some(site.skipped.swap(0, Ordering::Relaxed)));
}

/// Write one line. Called by the macros, which supply the site.
pub fn write(site: &'static Site, level: Level, what: &'static str, values: std::fmt::Arguments) {
    emit(site, level, what, values, None);
}

/// The line itself, with the count of what a rate-limited site left unsaid.
fn emit(
    site: &'static Site,
    level: Level,
    what: &'static str,
    values: std::fmt::Arguments,
    skipped: Option<u64>,
) {
    // The constant is a field of a comma-separated line, so it cannot contain
    // one; this is checked where it is cheap to check rather than trusted.
    debug_assert!(!what.contains(','), "a log constant with a comma in it: {what:?}");
    let stride = site.stride.load(Ordering::Relaxed);
    let seen = site.count.fetch_add(1, Ordering::Relaxed);
    if !site.listed.swap(true, Ordering::Relaxed) {
        SITES.lock().expect("sites").push(site);
    }

    LINE.with(|line| {
        let mut line = line.borrow_mut();
        line.clear();
        let pid = PID.get_or_init(std::process::id);
        let tid = TID.with(|tid| *tid);
        // The constant, then the values: two fields, so a reader can group on the
        // first without cutting into the second.
        let _ = write!(
            line,
            "{},{},pid={pid},tid={tid},file={},line={},{what},",
            level.letter(),
            stamp(),
            site.file,
            site.line
        );
        let _ = line.write_fmt(values);
        if let Some(skipped) = skipped {
            // A value, so the histogram takes no notice of it: it groups on the
            // constant and reads no further.
            let _ = write!(line, ",skipped {skipped}");
        }
        line.push('\n');

        if site.text.get().is_none() {
            let _ = site.text.set(what.to_string());
        }

        match LOGGER.get() {
            None => eprint!("{line}"),
            Some(logger) => {
                // Anything worse than ordinary goes to the disk now; the rest
                // waits for the buffer to fill or for the flusher to come round,
                // because a write per line costs more than the line does.
                let urgent = level >= Level::Warning;
                let _ = logger.full.lock().expect("log").write(line.as_bytes(), urgent);
                // Everything above info, and a sampled share of the rest.
                if level > Level::Info || stride <= 1 || seen.is_multiple_of(stride) {
                    let _ = logger.info.lock().expect("log").write(line.as_bytes(), urgent);
                }
            }
        }
    });

    if SINCE_COUNT.fetch_add(1, Ordering::Relaxed) >= recount_every() {
        SINCE_COUNT.store(0, Ordering::Relaxed);
        recount();
    }
}

/// Bytes of lines given to the logger, and bytes the files took, so the ratio a
/// run achieved is a fact rather than the guess the block size was derived from.
static LINE_BYTES: AtomicU64 = AtomicU64::new(0);
static FILE_BYTES: AtomicU64 = AtomicU64::new(0);

/// The bytes of lines a block gathers before it is written.
pub fn block() -> usize {
    LOGGER.get().map(|logger| logger.config.block).unwrap_or(0)
}

pub fn written() -> (u64, u64) {
    (LINE_BYTES.load(Ordering::Relaxed), FILE_BYTES.load(Ordering::Relaxed))
}

/// Push both buffers to the disk. Called on a timer, and worth calling before a
/// process stops.
pub fn flush() {
    if let Some(logger) = LOGGER.get() {
        let _ = logger.full.lock().expect("log").flush();
        let _ = logger.info.lock().expect("log").flush();
    }
}

fn recount_every() -> usize {
    LOGGER.get().map(|logger| logger.config.recount).unwrap_or(usize::MAX)
}

/// Work out how much of each site the info log should carry. Everything rare
/// passes whole; what is left of the budget is shared out among the busy sites
/// as one stride, so the info log comes out near its target share whatever the
/// mix of sites is.
///
/// Counts are halved rather than cleared, so the answer follows the workload
/// instead of being decided by whatever happened first.
fn recount() {
    let Some(logger) = LOGGER.get() else { return };
    let sites = SITES.lock().expect("sites");
    let mut counted: Vec<(u64, &&Site)> =
        sites.iter().map(|site| (site.count.load(Ordering::Relaxed), site)).collect();
    counted.sort_by_key(|a| a.0);

    let total: u64 = counted.iter().map(|(count, _)| count).sum();
    let target = (total as f64 * logger.config.info_share) as u64;

    // Least busy first, while the budget lasts.
    let mut spent = 0;
    let mut busy = Vec::new();
    let mut busy_total = 0;
    for (count, site) in &counted {
        if spent + count <= target {
            spent += count;
            site.stride.store(1, Ordering::Relaxed);
        } else {
            busy_total += count;
            busy.push(*site);
        }
    }
    let left = target.saturating_sub(spent).max(1);
    let stride = busy_total.div_ceil(left).max(1);
    for site in busy {
        site.stride.store(stride, Ordering::Relaxed);
    }

    for (count, site) in counted {
        site.count.store(count / 2, Ordering::Relaxed);
    }
}

/// What the histogram knows: every site, busiest first.
pub fn histogram() -> Vec<(u64, u64, String)> {
    let sites = SITES.lock().expect("sites");
    let mut rows: Vec<(u64, u64, String)> = sites
        .iter()
        .map(|site| {
            (
                site.count.load(Ordering::Relaxed),
                site.stride.load(Ordering::Relaxed),
                format!(
                    "{}:{} {}",
                    site.file,
                    site.line,
                    site.text.get().map(String::as_str).unwrap_or("")
                ),
            )
        })
        .collect();
    rows.sort_by_key(|b| std::cmp::Reverse(b.0));
    rows
}

/// Where a block has to end: at the last line boundary inside the limit, or at the
/// end of what is pending. A whole line always fits, since a line is far shorter
/// than a block.
fn block_end(pending: &[u8], from: usize) -> usize {
    let room = crate::logread::MAX_BLOCK;
    if pending.len() - from <= room {
        return pending.len();
    }
    match pending[from..from + room].iter().rposition(|byte| *byte == b'\n') {
        Some(last) => from + last + 1,
        // No line boundary in a whole block: one absurd line, cut rather than held
        // for ever.
        None => from + room,
    }
}

/// A run of numbered files, oldest deleted once there are too many.
struct Rotating {
    dir: PathBuf,
    name: String,
    size: u64,
    keep: usize,
    seq: usize,
    written: u64,
    file: File,
    /// Written at the top of every file. Rotation means a reader may hold only
    /// one of them, and a line alone does not say which process or machine it
    /// came from.
    header: String,
    /// Lines waiting to be written. With compression they are a block; without,
    /// they are just the next write.
    pending: Vec<u8>,
    /// How many bytes of lines to gather first.
    block: usize,
    compression: Compression,
}

impl Rotating {
    fn new(
        dir: &Path,
        name: &str,
        size: u64,
        keep: usize,
        header: String,
        compression: Compression,
        block: usize,
    ) -> Result<Rotating> {
        std::fs::create_dir_all(dir).context(dir.display())?;
        let mut rotating = Rotating {
            dir: dir.to_path_buf(),
            name: name.to_string(),
            size,
            keep: keep.max(1),
            seq: 0,
            written: 0,
            file: File::create(path(dir, name, 0, compression)).context(dir.display())?,
            header,
            pending: Vec::with_capacity(block),
            block,
            compression,
        };
        let header = rotating.header.clone();
        let _ = rotating.write(header.as_bytes(), true);
        Ok(rotating)
    }

    fn write(&mut self, bytes: &[u8], urgent: bool) -> std::io::Result<()> {
        LINE_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.pending.extend_from_slice(bytes);
        if urgent || self.pending.len() >= self.block {
            self.flush()?;
        }
        Ok(())
    }

    /// Put what is pending into the file: one compressed block, or several when
    /// there is more pending than a block may hold.
    ///
    /// A reader can only read whole blocks, so a block has a limit and the pending
    /// bytes have to be cut to fit it — always at a line boundary, since a line
    /// split across blocks would be a line no grep could find.
    fn flush(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Taken out of `self` so the file can be rolled while it is held.
        let pending = std::mem::take(&mut self.pending);
        let mut at = 0;
        let mut packed = Vec::new();
        while at < pending.len() {
            let end = if !matches!(self.compression, Compression::Plain) {
                block_end(&pending, at)
            } else {
                pending.len()
            };
            let piece = &pending[at..end];
            at = end;

            let bytes: &[u8] = if !matches!(self.compression, Compression::Plain) {
                packed.clear();
                let result = crate::logread::pack_zstd(piece, &mut packed);
                match result {
                    Ok(()) => &packed,
                    // Written as it stands rather than lost; the file says per
                    // block which it is.
                    Err(_) => piece,
                }
            } else {
                piece
            };

            if self.written + bytes.len() as u64 > self.size {
                self.roll()?;
            }
            self.file.write_all(bytes)?;
            FILE_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            self.written += bytes.len() as u64;
        }
        let mut pending = pending;
        pending.clear();
        self.pending = pending;
        Ok(())
    }

    fn roll(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.seq += 1;
        self.file = File::create(path(&self.dir, &self.name, self.seq, self.compression))?;
        self.written = 0;
        // The header goes at the top of the new file as its own block.
        let header = self.header.clone();
        let mut first = Vec::new();
        let bytes: &[u8] = if !matches!(self.compression, Compression::Plain) {
            let result = crate::logread::pack_zstd(header.as_bytes(), &mut first);
            match result {
                Ok(()) => &first,
                Err(_) => header.as_bytes(),
            }
        } else {
            header.as_bytes()
        };
        self.file.write_all(bytes)?;
        self.written = bytes.len() as u64;
        if let Some(gone) = self.seq.checked_sub(self.keep) {
            let _ = std::fs::remove_file(path(&self.dir, &self.name, gone, self.compression));
        }
        Ok(())
    }
}

/// This machine's name, for a log line that has travelled.
fn host() -> String {
    let mut name = [0u8; 64];
    let got = unsafe { libc::gethostname(name.as_mut_ptr() as *mut libc::c_char, name.len()) };
    if got != 0 {
        return "unknown".to_string();
    }
    let end = name.iter().position(|byte| *byte == 0).unwrap_or(name.len());
    String::from_utf8_lossy(&name[..end]).into_owned()
}

/// Every file of one log, oldest first. The names carry the order, so this is a
/// sort rather than a walk of anything's state.
pub fn files(dir: &Path, name: &str) -> crate::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let listing = match std::fs::read_dir(dir) {
        Ok(listing) => listing,
        // A process that has logged nothing has no directory, which is not an
        // error to ask about.
        Err(_) => return Ok(found),
    };
    for entry in listing.flatten() {
        let path = entry.path();
        let file = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
        // `<name>-0000000.log` or `.zst`, and not `<name>-info-0000000.log` when
        // asked for `<name>`.
        if let Some(tail) = file.strip_prefix(&format!("{name}-")) {
            let numbered = tail.split('.').next().is_some_and(|n| n.len() == 7);
            if numbered && (tail.ends_with(".log") || tail.ends_with(".zst")) {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

/// Where a process's logs are, given its run directory.
pub fn dir_of(run_dir: &Path) -> PathBuf {
    run_dir.join("logs")
}

fn path(dir: &Path, name: &str, seq: usize, compression: Compression) -> PathBuf {
    // The extension says which, because a compressed file that claims to be text
    // is a trap for whoever tries to read it with the wrong thing.
    let kind = match compression {
        Compression::Plain => "log",
        Compression::Zstd => "zst",
    };
    dir.join(format!("{name}-{seq:07}.{kind}"))
}

/// `2026-08-16 15:04:05.123`, from the epoch, without pulling in a calendar.
fn stamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:06}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        now.subsec_micros()
    )
}

/// Days since 1970 to a calendar date, by the usual shifted-era arithmetic.
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[macro_export]
macro_rules! logd {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Debug) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Debug, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Debug) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(
                &SITE,
                $crate::log::Level::Debug,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

/// At most one line a second from this site, at notice: rate-limited already, so
/// it is not the chatty kind and belongs in both logs. The first message of the
/// second is the one written, as a sample of what the rest would have said.
#[macro_export]
macro_rules! logsec {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::sample(&SITE, $crate::log::Level::Notice, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::sample(
                &SITE,
                $crate::log::Level::Notice,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

#[macro_export]
macro_rules! logi {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Info) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Info, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Info) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(
                &SITE,
                $crate::log::Level::Info,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

#[macro_export]
macro_rules! logn {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Notice, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(
                &SITE,
                $crate::log::Level::Notice,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

#[macro_export]
macro_rules! logw {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Warning) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Warning, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Warning) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(
                &SITE,
                $crate::log::Level::Warning,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

#[macro_export]
macro_rules! loge {
    ($what:literal) => {{
        if $crate::log::enabled($crate::log::Level::Error) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Error, $what, format_args!(""));
        }
    }};
    ($what:literal, $($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Error) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(
                &SITE,
                $crate::log::Level::Error,
                $what,
                format_args!($($arg)*),
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_and_a_leap_day() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(59), (1970, 3, 1));
        // 2024-02-29, a leap day, is day 19782.
        assert_eq!(civil(19_782), (2024, 2, 29));
        assert_eq!(civil(20_682), (2026, 8, 17));
    }

    #[test]
    fn rotation_keeps_the_last_few() {
        let dir = std::env::temp_dir().join(format!("laws-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rotating =
            Rotating::new(&dir, "test", 64, 3, String::new(), Compression::Plain, 1).unwrap();
        for n in 0..40 {
            rotating.write(format!("line {n} of some length\n").as_bytes(), true).unwrap();
        }
        let left: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(left.len(), 3, "should keep three files");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod cost {
    use super::*;

    /// Only an optimized build has anything to say about nanoseconds.
    fn too_slow(each: f64) {
        if cfg!(debug_assertions) {
            return;
        }
        assert!(each < 20.0, "{each} ns is too much for a line nobody writes");
    }

    /// What a rate-limited line costs when it is not the one being written.
    #[test]
    fn a_rate_limited_line_is_nearly_free() {
        set_floor(Level::Info);
        crate::logsec!("start the clock");
        let rounds = 10_000_000;
        let mut sink = 0u64;
        let start = std::time::Instant::now();
        for n in 0..rounds {
            sink = sink.wrapping_add(n);
            crate::logsec!("a line", "value {n} another {sink}");
        }
        let each = start.elapsed().as_secs_f64() * 1e9 / rounds as f64;
        println!("rate limited: {each:.2} ns per line (sink {sink})");
        too_slow(each);
    }

    /// What a line costs when the level filters it out. It has to be nothing
    /// worth measuring, or a debug line cannot be left in a hot path.
    #[test]
    fn a_filtered_line_is_nearly_free() {
        set_floor(Level::Notice);
        let rounds = 10_000_000;
        let mut sink = 0u64;
        let start = std::time::Instant::now();
        for n in 0..rounds {
            sink = sink.wrapping_add(n);
            crate::logd!("a line", "value {n} another {sink}");
        }
        let each = start.elapsed().as_secs_f64() * 1e9 / rounds as f64;
        println!("filtered: {each:.2} ns per line (sink {sink})");
        set_floor(Level::Info);
        too_slow(each);
    }
}

#[cfg(test)]
mod blocks {
    use super::*;

    /// More pending than a block may hold has to become several blocks, cut at
    /// line boundaries, and read back as the same lines.
    #[test]
    fn a_long_delay_becomes_several_blocks() {
        let dir = std::env::temp_dir().join(format!("laws-blocks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // A block target larger than a block may be, which is what an HDD's
        // 14 MB of text asks for.
        let block = crate::logread::MAX_BLOCK * 3;
        let mut rotating =
            Rotating::new(&dir, "dn0", u64::MAX, 4, String::new(), Compression::Zstd, block)
                .unwrap();

        let line = "i,2026-08-17 01:00:00.000001,pid=1,tid=1,file=x.rs,line=1,said,value 1\n";
        let count = block / line.len() + 10;
        for _ in 0..count {
            rotating.write(line.as_bytes(), false).unwrap();
        }
        rotating.flush().unwrap();

        let logs = crate::logread::Logs::open(&dir, "dn0", u64::MAX).unwrap();
        let (seen, rows) = logs.histogram("", "").unwrap();
        assert_eq!(seen, count as u64, "every line should come back");
        assert_eq!(rows.len(), 1, "{rows:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
