//! Reading log files: a histogram, a grep, and a range of lines.
//!
//! Nobody logs into a machine to read these. The three answers here are what
//! replaces that, and they are deliberately the three that between them settle
//! most questions: what is this process saying, which lines mention this thing,
//! and what did it say around then.
//!
//! **A histogram is read from both ends.** The top says what is happening most,
//! which is the obvious use; the bottom says what happened once, which is the
//! needle in the haystack when the thing being chased is rare. So it returns every
//! row rather than the busiest few, and it takes a time range, because a rare event
//! is usually being looked for in the minute it happened.
//!
//! It lives in `common` because it has to run on the machine holding the files,
//! and a machine that only runs signed code has two ways to do that: ask the
//! process, which already runs there, or ship one signed tool. Both use this, so
//! neither is a second implementation.
//!
//! **Serial and unhurried, on purpose.** Reading a log can take minutes, and it is
//! competing with a server for the same CPU and the same disk — so it reads one
//! file at a time, a block at a time, holding only what an answer needs rather
//! than everything it read. The caller supplies the patience.

use crate::log::files;
use crate::{Context, Error, Result};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

/// A parsed line. Fields are as written: level, time, pid, tid, file, line, then
/// the message.
pub struct Line<'a> {
    pub level: &'a str,
    pub time: &'a str,
    pub file: &'a str,
    pub at: &'a str,
    /// The constant part, which is what a histogram groups on.
    pub what: &'a str,
    /// Everything that varied, in the order the writer chose.
    pub values: &'a str,
}

impl<'a> Line<'a> {
    /// `l,time,pid=,tid=,file=,line=,constant,values`. A line that is not that
    /// shape is still returned, with empty fields, since a log is not the place to
    /// be strict about what it will show you.
    pub fn of(whole: &'a str) -> Line<'a> {
        let mut fields = whole.splitn(8, ',');
        let level = fields.next().unwrap_or("");
        let time = fields.next().unwrap_or("");
        let _pid = fields.next();
        let _tid = fields.next();
        let file = fields.next().unwrap_or("").trim_start_matches("file=");
        let at = fields.next().unwrap_or("").trim_start_matches("line=");
        let what = fields.next().unwrap_or("");
        let values = fields.next().unwrap_or("");
        Line { level, time, file, at, what, values }
    }
}

/// One process's log, as something to read rather than something read.
pub struct Logs {
    paths: Vec<PathBuf>,
}

impl Logs {
    /// The files of one log, oldest first, the newest `budget` bytes of them.
    /// `u64::MAX` is all of it, which is the normal thing to want.
    pub fn open(dir: &Path, name: &str, budget: u64) -> Result<Logs> {
        let all = files(dir, name)?;
        let mut paths = Vec::new();
        let mut room = budget;
        for path in all.iter().rev() {
            let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            paths.push(path.clone());
            room = room.saturating_sub(size);
            if room == 0 {
                break;
            }
        }
        paths.reverse();
        Ok(Logs { paths })
    }

    /// Every line, oldest first, one file and one block at a time. Returns how
    /// many lines went past — including the ones a time range excluded, since
    /// "nothing in that minute" and "nothing at all" are different answers.
    pub fn scan(&self, mut each: impl FnMut(&str)) -> Result<u64> {
        let mut seen = 0;
        for path in &self.paths {
            let mut file = std::fs::File::open(path).context(path.display())?;
            let mut text = String::new();
            if path.extension().is_some_and(|kind| kind == "logz") {
                // Blocks, each a pair of lengths and the bytes. A torn last block
                // is a process that died mid-write: it ends the walk rather than
                // failing the answer, since the rest is still worth reading.
                loop {
                    let mut sizes = [0u8; 8];
                    if file.read_exact(&mut sizes).is_err() {
                        break;
                    }
                    let loose = u32::from_le_bytes(sizes[..4].try_into().unwrap()) as usize;
                    let tight = u32::from_le_bytes(sizes[4..].try_into().unwrap()) as usize;
                    if loose > MAX_BLOCK || tight > MAX_BLOCK {
                        break;
                    }
                    let mut block = vec![0u8; tight];
                    if file.read_exact(&mut block).is_err() {
                        break;
                    }
                    let Ok(loosened) = lz4_flex::decompress(&block, loose) else { break };
                    text.clear();
                    text.push_str(&String::from_utf8_lossy(&loosened));
                    for line in text.lines() {
                        seen += 1;
                        each(line);
                    }
                }
            } else {
                file.read_to_string(&mut text).context(path.display())?;
                for line in text.lines() {
                    seen += 1;
                    each(line);
                }
            }
        }
        Ok(seen)
    }

    /// `000000123,i,file=…,line=…,constant`, sorted by frequency: a row per place
    /// in the code that said anything, in a window of time.
    ///
    /// The fields are spelled as the lines spell them, so everything after the
    /// count and the level is **literally a substring of the lines it counts** —
    /// a row is what to paste into the next query.
    ///
    /// **Every row comes back**, because the bottom of the list is half the point:
    /// a count of one is the rare event someone is looking for, and truncating to
    /// the busiest rows would throw away exactly that.
    pub fn histogram(&self, from: &str, to: &str) -> Result<(u64, Vec<String>)> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        let mut key = String::new();
        let seen = self.scan(|whole| {
            let line = Line::of(whole);
            if !within(line.time, from, to) {
                return;
            }
            key.clear();
            key.push_str(line.level);
            key.push_str(",file=");
            key.push_str(line.file);
            key.push_str(",line=");
            key.push_str(line.at);
            key.push(',');
            key.push_str(line.what);
            match counts.get_mut(&key) {
                Some(count) => *count += 1,
                None => {
                    // A log of nothing but unique lines would be a histogram of
                    // itself, so there is a stop — said out loud when it is hit.
                    if counts.len() < MAX_ROWS {
                        counts.insert(key.clone(), 1);
                    }
                }
            }
        })?;

        let full = counts.len() >= MAX_ROWS;
        let mut counted: Vec<String> =
            counts.into_iter().map(|(what, count)| format!("{count:09},{what}")).collect();
        // Zero-padded, so the plain order is the frequency order.
        counted.sort_by(|a, b| b.cmp(a));
        if full {
            counted.push(format!("stopped counting at {MAX_ROWS} kinds of line"));
        }
        Ok((seen, counted))
    }

    /// Whole lines containing `what`, oldest first, in a window of time.
    pub fn grep(
        &self,
        what: &str,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<(u64, Vec<String>)> {
        let mut found = Vec::new();
        let seen = self.scan(|line| {
            if found.len() < limit && line.contains(what) && within(Line::of(line).time, from, to) {
                found.push(line.to_string());
            }
        })?;
        Ok((seen, found))
    }

    /// Lines whose time falls between two stamps, or the last `limit` lines when
    /// no stamps are given. Times are written so that comparing the text is
    /// comparing the instant.
    pub fn range(&self, from: &str, to: &str, limit: usize) -> Result<(u64, Vec<String>)> {
        let tail = from.is_empty() && to.is_empty();
        let mut found: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        let seen = self.scan(|line| {
            if tail {
                // Only the last few are wanted, so only the last few are held.
                found.push_back(line.to_string());
                if found.len() > limit {
                    found.pop_front();
                }
                return;
            }
            if found.len() < limit && within(Line::of(line).time, from, to) {
                found.push_back(line.to_string());
            }
        })?;
        Ok((seen, found.into()))
    }
}

/// Whether a line's time is in the window. Times are written so that comparing
/// the text is comparing the instant, and an empty end is no bound.
fn within(time: &str, from: &str, to: &str) -> bool {
    (from.is_empty() || time >= from) && (to.is_empty() || time <= to)
}

/// A histogram of a log that is all unique lines would be as large as the log.
const MAX_ROWS: usize = 10_000;

/// The largest block a writer may emit, and so the largest a reader will allocate
/// for one. A writer with more than this pending cuts it at a line boundary.
pub const MAX_BLOCK: usize = 8 << 20;

/// Compress one block of lines for a log file: the loose length, the tight
/// length, then the bytes. Lengths first, so a reader can walk without a table.
pub fn pack(lines: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if lines.len() > MAX_BLOCK {
        return Err(Error::new(format!("a log block of {} bytes", lines.len())));
    }
    let tight = lz4_flex::compress(lines);
    out.extend((lines.len() as u32).to_le_bytes());
    out.extend((tight.len() as u32).to_le_bytes());
    out.extend(tight);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
i,2026-08-17 01:00:00.000001,pid=1,tid=1,file=dn/src/main.rs,line=10,answered,op create,status ok,us 5
i,2026-08-17 01:00:00.000002,pid=1,tid=2,file=dn/src/main.rs,line=10,answered,op create,status ok,us 7
i,2026-08-17 01:00:00.000003,pid=1,tid=3,file=dn/src/main.rs,line=10,answered,op object-info,status ok,us 2
n,2026-08-17 01:00:01.000000,pid=1,tid=1,file=common/src/server.rs,line=99,listening,address 127.0.0.1:9601
";
    /// A log of one file, plain or compressed, in a directory of its own.
    fn logs(packed: bool) -> Logs {
        // Tests run at the same time in one process, so the name has to be
        // unique per call rather than per process.
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mine = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("laws-read-{}-{mine}-{packed}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut bytes = Vec::new();
        if packed {
            pack(SAMPLE.as_bytes(), &mut bytes).unwrap();
        } else {
            bytes.extend(SAMPLE.as_bytes());
        }
        let kind = if packed { "logz" } else { "log" };
        std::fs::write(dir.join(format!("dn0-0000000.{kind}")), bytes).unwrap();
        Logs::open(&dir, "dn0", u64::MAX).unwrap()
    }

    /// A row per site, whole, with nothing cut and nothing to choose — and the rare
    /// one at the bottom, which is the half of the answer that finds a needle.
    #[test]
    fn a_row_per_site_including_the_rare_one() {
        let (seen, rows) = logs(false).histogram("", "").unwrap();
        assert_eq!(seen, 4);
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0], "000000003,i,file=dn/src/main.rs,line=10,answered", "{rows:?}");
        assert_eq!(
            rows[rows.len() - 1],
            "000000001,n,file=common/src/server.rs,line=99,listening",
            "{rows:?}"
        );
    }

    /// A window narrows it, because a rare event is looked for in the minute it
    /// happened rather than in everything the process ever said.
    #[test]
    fn a_window_narrows_the_histogram() {
        let (seen, rows) = logs(false).histogram("2026-08-17 01:00:01", "").unwrap();
        assert_eq!(seen, 4, "every line is still read");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(rows[0].ends_with("listening"), "{rows:?}");
    }

    /// And a row is what to grep for: the part after the count and the level is a
    /// substring of every line it counted, so the second query needs no thought.
    #[test]
    fn a_row_says_what_to_grep_for() {
        let logs = logs(false);
        let (_, rows) = logs.histogram("", "").unwrap();
        let site = rows[0].splitn(3, ',').nth(2).expect("count, level, then the rest");
        let (_, found) = logs.grep(site, "", "", 10).unwrap();
        assert_eq!(found.len(), 3, "{found:?}");
    }

    #[test]
    fn a_compressed_log_reads_the_same() {
        assert_eq!(logs(false).histogram("", "").unwrap(), logs(true).histogram("", "").unwrap());
    }

    #[test]
    fn grep_returns_whole_lines() {
        let (_, found) = logs(true).grep("object-info", "", "", 10).unwrap();
        assert_eq!(found.len(), 1);
        assert!(found[0].starts_with("i,2026-08-17 01:00:00.000003,pid=1"));
    }

    #[test]
    fn a_range_is_by_time_and_a_tail_when_it_is_not() {
        let logs = logs(true);
        assert_eq!(logs.range("2026-08-17 01:00:01", "", 10).unwrap().1.len(), 1);
        assert_eq!(logs.range("", "2026-08-17 01:00:00.000002", 10).unwrap().1.len(), 2);
        let (_, tail) = logs.range("", "", 2).unwrap();
        assert_eq!(tail.len(), 2);
        assert!(tail[1].contains("listening"));
    }
}
