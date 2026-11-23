use std::path::Path;
use std::time::Duration;

pub const HOUR_SECONDS: u64 = 3600;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wake {
    CodexApp {
        session: String,
        prompt: String,
    },
    CodexExec {
        session: Option<String>,
        prompt: String,
    },
    ClaudePrint {
        session: Option<String>,
        prompt: String,
    },
}

pub struct Budget {
    limit: u64,
    hits: Vec<u64>,
}

impl Budget {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            hits: Vec::new(),
        }
    }

    pub fn allow(&mut self, now: u64) -> bool {
        self.hits
            .retain(|hit| now.saturating_sub(*hit) < HOUR_SECONDS);
        if self.hits.len() as u64 >= self.limit {
            return false;
        }
        self.hits.push(now);
        true
    }
}

pub fn plan(
    agent: &str,
    session: Option<&str>,
    prompt: &str,
    from_email: Option<&str>,
    allowed: &[String],
    idle: bool,
    budget_ok: bool,
) -> Option<Wake> {
    if !idle || !budget_ok {
        return None;
    }
    let email = from_email?;
    if !allowed.iter().any(|item| item.eq_ignore_ascii_case(email)) {
        return None;
    }
    let prompt = prompt.to_owned();
    match agent {
        "codex" if session.is_some() => Some(Wake::CodexApp {
            session: session.unwrap_or_default().to_owned(),
            prompt,
        }),
        "codex" => Some(Wake::CodexExec {
            session: None,
            prompt,
        }),
        "claude" => Some(Wake::ClaudePrint {
            session: session.map(str::to_owned),
            prompt,
        }),
        _ => None,
    }
}

pub fn codex_exec_args(program: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
    let mut args = vec![
        program.to_owned(),
        "exec".to_owned(),
        "--skip-git-repo-check".to_owned(),
        "--json".to_owned(),
        "--dangerously-bypass-approvals-and-sandbox".to_owned(),
    ];
    if let Some(session) = session {
        args.push("resume".to_owned());
        args.push(session.to_owned());
    }
    args.push(prompt.to_owned());
    args
}

pub fn claude_print_args(program: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
    let mut args = vec![
        program.to_owned(),
        "-p".to_owned(),
        prompt.to_owned(),
        "--output-format".to_owned(),
        "json".to_owned(),
    ];
    if let Some(session) = session {
        args.push("-r".to_owned());
        args.push(session.to_owned());
    }
    args
}

pub async fn run(program: &str, args: &[String], cwd: &Path) -> Result<(), String> {
    let mut command = tokio::process::Command::new(program);
    if args.len() > 1 {
        command.args(&args[1..]);
    }
    let status = tokio::time::timeout(Duration::from_secs(120), command.current_dir(cwd).status())
        .await
        .map_err(|_| "wake timed out".to_owned())?
        .map_err(|err| err.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("wake exited {status}"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn a_stranger_is_not_woken() {
        assert!(
            plan(
                "codex",
                Some("thread-1"),
                "hi",
                Some("cara@example.com"),
                &["alice@example.com".to_owned()],
                true,
                true
            )
            .is_none()
        );
    }

    #[test]
    fn an_idle_codex_session_prefers_the_app_server() {
        assert_eq!(
            plan(
                "codex",
                Some("thread-1"),
                "hi",
                Some("alice@example.com"),
                &["alice@example.com".to_owned()],
                true,
                true
            ),
            Some(Wake::CodexApp {
                session: "thread-1".to_owned(),
                prompt: "hi".to_owned(),
            })
        );
    }

    #[test]
    fn the_hourly_budget_stops_further_wakes() {
        let mut budget = Budget::new(1);
        assert!(budget.allow(1_000));
        assert!(!budget.allow(1_100));
        assert!(budget.allow(1_000 + HOUR_SECONDS));
    }

    #[test]
    fn codex_resume_args_match_a_single_turn() {
        let args = codex_exec_args("codex", Some("thread-1"), "hi");
        assert_eq!(
            args,
            vec![
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                "resume",
                "thread-1",
                "hi",
            ]
        );
    }
}
