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
//! Every macro invocation gets its own `Site`, so counting a line is one relaxed
//! increment and deciding where it goes is one relaxed load.
//!
//! Write messages with the words first and the values last — `appending to %s
//! length %s`, not `%s: appending, len %s` — because the histogram is read by
//! the beginning of the line.

use crate::{Context, Result};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufWriter, Write as _};
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
    pub size: u64,
    pub full_files: usize,
    pub info_files: usize,
    /// The share of the full log's lines the info log should carry.
    pub info_share: f64,
    /// Lines between recounts.
    pub recount: usize,
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
    let header = format!(
        "h,{},pid={},identity={identity},host={},version={},times are UTC\n",
        stamp(),
        std::process::id(),
        host(),
        env!("CARGO_PKG_VERSION"),
    );
    let full = Rotating::new(dir, &name, config.size, config.full_files, header.clone())?;
    let info = Rotating::new(dir, &format!("{name}-info"), config.size, config.info_files, header)?;
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
            if ticks % (FLUSH_MS / TICK_MS) == 0 {
                flush();
            }
        }
    });
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// One line a second from this site, and the first of the second is the one that
/// gets through. For a place that would otherwise say the same thing millions of
/// times: a sample says what is happening, and the rest says nothing new.
///
/// The suppressed path is a load and a compare, with no atomic write — counting
/// what was skipped would put every thread onto one cache line, which is the cost
/// this exists to avoid.
pub fn sample(site: &'static Site, level: Level, message: std::fmt::Arguments) {
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
        return;
    }
    // Whichever thread claims the second writes the line; the rest carry on.
    if site.said.compare_exchange(said, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        return;
    }
    write(site, level, message);
}

/// Write one line. Called by the macros, which supply the site.
pub fn write(site: &'static Site, level: Level, message: std::fmt::Arguments) {
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
        let _ = write!(
            line,
            "{},{},pid={pid},tid={tid},file={},line={},",
            level.letter(),
            stamp(),
            site.file,
            site.line
        );
        let text_at = line.len();
        let _ = line.write_fmt(message);
        line.push('\n');

        if site.text.get().is_none() {
            let end = line[text_at..].char_indices().map(|(i, _)| i).nth(60).unwrap_or(0);
            let _ = site.text.set(line[text_at..text_at + end].to_string());
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
                if level > Level::Info || stride <= 1 || seen % stride == 0 {
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
    counted.sort_by(|a, b| a.0.cmp(&b.0));

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
    rows.sort_by(|a, b| b.0.cmp(&a.0));
    rows
}

/// Buffer per log file. Big enough that a busy process writes whole pages.
const BUFFER_BYTES: usize = 64 << 10;

/// A run of numbered files, oldest deleted once there are too many.
struct Rotating {
    dir: PathBuf,
    name: String,
    size: u64,
    keep: usize,
    seq: usize,
    written: u64,
    file: BufWriter<File>,
    /// Written at the top of every file. Rotation means a reader may hold only
    /// one of them, and a line alone does not say which process or machine it
    /// came from.
    header: String,
}

impl Rotating {
    fn new(dir: &Path, name: &str, size: u64, keep: usize, header: String) -> Result<Rotating> {
        std::fs::create_dir_all(dir).context(dir.display())?;
        let mut rotating = Rotating {
            dir: dir.to_path_buf(),
            name: name.to_string(),
            size,
            keep: keep.max(1),
            seq: 0,
            written: 0,
            file: BufWriter::with_capacity(
                BUFFER_BYTES,
                File::create(path(dir, name, 0)).context(dir.display())?,
            ),
            header,
        };
        let header = rotating.header.clone();
        let _ = rotating.write(header.as_bytes(), true);
        Ok(rotating)
    }

    fn write(&mut self, bytes: &[u8], urgent: bool) -> std::io::Result<()> {
        if self.written + bytes.len() as u64 > self.size {
            self.roll()?;
        }
        self.file.write_all(bytes)?;
        self.written += bytes.len() as u64;
        if urgent {
            self.file.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    fn roll(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.seq += 1;
        self.file = BufWriter::with_capacity(
            BUFFER_BYTES,
            File::create(path(&self.dir, &self.name, self.seq))?,
        );
        self.written = 0;
        let header = self.header.clone();
        self.file.write_all(header.as_bytes())?;
        self.written = header.len() as u64;
        if let Some(gone) = self.seq.checked_sub(self.keep) {
            let _ = std::fs::remove_file(path(&self.dir, &self.name, gone));
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

fn path(dir: &Path, name: &str, seq: usize) -> PathBuf {
    dir.join(format!("{name}-{seq:07}.log"))
}

/// `2026-08-16 15:04:05.123`, from the epoch, without pulling in a calendar.
fn stamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        now.subsec_millis()
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
    ($($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Debug) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Debug, format_args!($($arg)*));
        }
    }};
}

/// At most one line a second from this site, at notice: rate-limited already, so
/// it is not the chatty kind and belongs in both logs. The first message of the
/// second is the one written, as a sample of what the rest would have said.
#[macro_export]
macro_rules! logsec {
    ($($arg:tt)*) => {{
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::sample(&SITE, $crate::log::Level::Notice, format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! logi {
    ($($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Info) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Info, format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! logn {
    ($($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Notice) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Notice, format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! logw {
    ($($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Warning) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Warning, format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! loge {
    ($($arg:tt)*) => {{
        // The floor is tested first, so a filtered line does not even evaluate
        // its arguments.
        if $crate::log::enabled($crate::log::Level::Error) {
            static SITE: $crate::log::Site = $crate::log::Site::new(file!(), line!());
            $crate::log::write(&SITE, $crate::log::Level::Error, format_args!($($arg)*));
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
        let mut rotating = Rotating::new(&dir, "test", 64, 3, String::new()).unwrap();
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
            crate::logsec!("a line with a value {n} and another {sink}");
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
            crate::logd!("a line with a value {n} and another {sink}");
        }
        let each = start.elapsed().as_secs_f64() * 1e9 / rounds as f64;
        println!("filtered: {each:.2} ns per line (sink {sink})");
        set_floor(Level::Info);
        too_slow(each);
    }
}
