//! `logs`, the standalone log reader: the same three questions the services
//! answer about their own files, asked of the files directly.
//!
//! It exists for the case the in-process answer cannot cover: a process that has
//! died, or is wedged, or is too busy to answer — which is exactly when its logs
//! are wanted. It reads files and nothing else, so a machine that only runs
//! signed code needs only this one extra signed thing.

use clap::{Parser, Subcommand};
use common::logread::Logs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "logs", version, about = "Read DeLawson log files")]
struct Cli {
    /// The directory holding the files, which is `logs` under a run directory.
    #[arg(long, default_value = "logs")]
    dir: PathBuf,
    /// Whose log: a process identity such as `dn0`, or `dn0-info` for the log
    /// that reaches back further.
    #[arg(long)]
    of: String,
    /// Newest bytes to read. All of it by default: reading a log takes as long
    /// as it takes, and it is serial so that it stays out of the servers' way.
    #[arg(long, default_value_t = u64::MAX)]
    budget: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// What this process is mostly saying, most frequent first.
    Histogram {
        /// Rows to return, most frequent first.
        #[arg(long, default_value_t = 40)]
        rows: usize,
    },
    /// Whole lines containing something.
    Grep {
        what: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Lines between two times, or the last few when no times are given.
    Lines {
        #[arg(long, default_value = "")]
        from: String,
        #[arg(long, default_value = "")]
        to: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    let logs = match Logs::open(&cli.dir, &cli.of, cli.budget) {
        Ok(logs) => logs,
        Err(e) => {
            eprintln!("logs: {e}");
            std::process::exit(1);
        }
    };
    let read = match &cli.cmd {
        Cmd::Histogram { rows } => logs.histogram(*rows),
        Cmd::Grep { what, limit } => logs.grep(what, *limit),
        Cmd::Lines { from, to, limit } => logs.range(from, to, *limit),
    };
    match read {
        Err(e) => {
            eprintln!("logs: {e}");
            std::process::exit(1);
        }
        Ok((seen, lines)) => {
            for line in lines {
                println!("{line}");
            }
            eprintln!("{seen} lines read");
        }
    }
}
