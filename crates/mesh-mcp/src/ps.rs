//! `agent-mesh ps` and `agent-mesh kill`: the user's view of everything the hub runs.
//!
//! These talk to a running hub only. They never register as a node (the caller is a shell, not
//! an agent) and never start a hub just to report that nothing is running.

use crate::client;
use crate::hub::{ProcessReport, Request};

type Outcome = Result<(), Box<dyn std::error::Error>>;

pub async fn ps() -> Outcome {
    let Some(data) = client::call_if_running(&Request::Ps).await else {
        println!("no agent-mesh hub is running, so the mesh has no agent processes");
        return Ok(());
    };
    let report: ProcessReport = serde_json::from_value(data)?;

    println!(
        "{} of {} agent processes (max_processes)\n",
        report.processes.len(),
        report.max_processes
    );
    if !report.processes.is_empty() {
        println!(
            "{:<24} {:<9} {:<9} {:>7} {:>8} {:>8}  SPAWNED BY",
            "ID", "KIND", "PID", "MEM", "AGE", "IDLE"
        );
        for p in &report.processes {
            println!(
                "{:<24} {:<9} {:<9} {:>7} {:>8} {:>8}  {}",
                p.id,
                p.kind,
                p.pid,
                format!("{}M", p.rss_mib),
                p.age_seconds.map(duration).unwrap_or_else(|| "-".to_owned()),
                p.idle_seconds.map(duration).unwrap_or_else(|| "-".to_owned()),
                p.spawned_by.as_deref().unwrap_or("-"),
            );
        }
        let total: u64 = report.processes.iter().map(|p| p.rss_mib).sum();
        println!("\ntotal memory: {total}M");
        if let Some(watch) = report.processes.iter().find_map(|p| p.tmux_session.as_deref()) {
            println!("watch a tmux node: tmux attach -t {watch}");
        }
    }

    if !report.unmanaged_nodes.is_empty() {
        println!("\nyour own sessions on the mesh (not counted, never killed):");
        for n in &report.unmanaged_nodes {
            println!(
                "  {:<22} pid {:<8} {}",
                n.id,
                n.pid.map(|p| p.to_string()).unwrap_or_default(),
                n.cwd
            );
        }
    }
    Ok(())
}

pub async fn kill(target: Option<String>) -> Outcome {
    let Some(target) = target else {
        eprintln!("usage: agent-mesh kill <id> | --all   (ids come from `agent-mesh ps`)");
        std::process::exit(2);
    };
    let request = if target == "--all" {
        Request::Kill {
            target: None,
            all: true,
        }
    } else {
        Request::Kill {
            target: Some(target),
            all: false,
        }
    };

    match client::call_existing(&request).await {
        Ok(data) => {
            let killed: Vec<String> =
                serde_json::from_value(data["killed"].clone()).unwrap_or_default();
            if killed.is_empty() {
                println!("nothing to stop");
            } else {
                println!("stopped: {}", killed.join(", "));
            }
            Ok(())
        }
        Err(err) => {
            eprintln!("agent-mesh: {err}");
            std::process::exit(1);
        }
    }
}

/// `95` -> `1m35s`, `7300` -> `2h1m`.
fn duration(seconds: u64) -> String {
    match seconds {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m{}s", s / 60, s % 60),
        s => format!("{}h{}m", s / 3600, (s % 3600) / 60),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_read_naturally() {
        assert_eq!(duration(5), "5s");
        assert_eq!(duration(95), "1m35s");
        assert_eq!(duration(7300), "2h1m");
    }
}
