//! Local log commands deliberately run before loading any cluster configuration.
use crate::Cmd;
use common::logread::Logs;
use common::{Error, Result};
use std::path::PathBuf;

#[derive(clap::Args)]
pub(crate) struct LocalLogs {
    /// Read a local log file or directory, without contacting any service.
    #[arg(long)]
    path: Option<PathBuf>,
    /// Process log basename for a directory (for example ks0-member0).
    #[arg(long, requires = "path")]
    log_name: Option<String>,
}

pub(crate) fn run(command: &Cmd) -> Result<bool> {
    let (local, which) = match command {
        Cmd::LogHistogram { local, which, .. }
        | Cmd::LogGrep { local, which, .. }
        | Cmd::LogLines { local, which, .. } => (local, which),
        _ => return Ok(false),
    };
    let Some(path) = &local.path else { return Ok(false) };
    if which != "full" && which != "info" {
        return Err(Error::new("--which must be full or info"));
    }
    let logs = if std::fs::metadata(path)?.is_dir() {
        let name = local
            .log_name
            .as_ref()
            .ok_or_else(|| Error::new("a log directory requires --log-name"))?;
        let mut name = name.to_lowercase();
        if which == "info" {
            name.push_str("-info");
        }
        if common::log::files(path, &name)?.is_empty() {
            return Err(Error::new("no matching log files in directory"));
        }
        Logs::open(path, &name, u64::MAX)?
    } else {
        if local.log_name.is_some() || which != "full" {
            return Err(Error::new(
                "a single file selects its log directly; omit --log-name and --which",
            ));
        }
        Logs::file(path)?
    };
    let (seen, rows) = match command {
        Cmd::LogHistogram { from, to, .. } => logs.histogram(from, to)?,
        Cmd::LogGrep { what, from, to, limit, .. } => logs.grep(what, from, to, *limit)?,
        Cmd::LogLines { from, to, limit, .. } => logs.range(from, to, *limit)?,
        _ => unreachable!(),
    };
    println!("{} {seen}", path.display());
    for row in rows {
        println!("{row}");
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn local_flags_are_available_on_all_log_commands() {
        for command in ["log-histogram", "log-grep", "log-lines"] {
            let mut args = vec!["laws", command];
            if command == "log-grep" {
                args.push("needle");
            }
            args.extend(["--path", "/saved/logs", "--log-name", "ks0-member0"]);
            let parsed = crate::Cli::try_parse_from(args);
            assert!(parsed.is_ok(), "{command}");
        }
        let missing_path = crate::Cli::try_parse_from(["laws", "log-lines", "--log-name", "ks0"]);
        assert!(missing_path.is_err());
    }

    #[test]
    fn remote_commands_do_not_invoke_local_reader() {
        let cli = crate::Cli::try_parse_from(["laws", "log-histogram"]).unwrap();
        let local = run(&cli.cmd).unwrap();
        assert!(!local);
    }
}
