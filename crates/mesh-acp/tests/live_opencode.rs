//! Integration tests against the real `opencode acp` binary.
//!
//! These drive an actual agent process and a real (free) model, because the behaviour under test
//! is precisely what a mock would paper over: whether ACP `session/load` really replays a prior
//! conversation into a *new* process. That replay is the primitive the whole control plane rests
//! on, so it must be verified against the vendor, not a stub.
//!
//! Skipped automatically when `opencode` is absent so the suite stays green on a bare machine.
//!
//! Each test gets its own workspace directory and holds a process-wide lock, because these drive
//! real agent processes: two running at once contend over the same agent state and fail in ways
//! that look like protocol bugs.

use mesh_acp::{AcpLaunch, AcpTransport};
use mesh_core::{AgentId, AgentTransport, Speaker};
use std::path::PathBuf;

/// A model that costs nothing, so the suite is safe to run repeatedly in CI.
const FREE_MODEL: &str = "opencode/deepseek-v4-flash-free";

fn opencode_present() -> bool {
    std::process::Command::new("opencode")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn transport() -> AcpTransport {
    AcpTransport::new(AcpLaunch {
        agent: AgentId::new("opencode"),
        program: "opencode".to_owned(),
        args: vec!["acp".to_owned()],
        model: Some(FREE_MODEL.to_owned()),
    })
}

/// A private, canonical workspace per test. Canonical matters on macOS, where `/tmp` is a symlink
/// to `/private/tmp`: the mesh canonicalizes paths, so a session created under one spelling would
/// not be found under the other.
fn workspace(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("agent-mesh-live-{name}"));
    std::fs::create_dir_all(&dir).expect("create workspace");
    std::fs::canonicalize(&dir).expect("canonicalize workspace")
}

/// Serializes these tests within the process. `cargo test` runs integration tests in threads by
/// default, and concurrent agent processes interfere with each other.
fn exclusive() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A session can be created and prompted, and the agent's words come back. Without this the
/// transport is inert.
#[tokio::test(flavor = "multi_thread")]
async fn opens_a_session_and_gets_a_reply() {
    let _guard = exclusive();
    if !opencode_present() {
        eprintln!("skipping: opencode not installed");
        return;
    }

    let acp = transport();
    let cwd = workspace("open");

    let opened = acp.open(&cwd).await.expect("session/new should succeed");
    assert!(
        !opened.vendor.as_str().is_empty(),
        "the agent must mint a session id"
    );

    let reply = acp
        .prompt(
            &opened.vendor,
            "Reply with exactly this one word and nothing else: PINEAPPLE",
        )
        .await
        .expect("session/prompt should succeed");

    assert!(
        reply.text.to_uppercase().contains("PINEAPPLE"),
        "expected the agent's reply to contain PINEAPPLE, got: {:?}",
        reply.text
    );
}

/// The load-bearing regression: a session created and prompted through ONE transport must be
/// reachable from a SECOND, independently spawned transport, with the prior conversation
/// replayed. This is what lets one agent read and continue another agent's session; if
/// `attach` ever stops collecting the replay, this fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_survives_across_processes_with_transcript_replay() {
    let _guard = exclusive();
    if !opencode_present() {
        eprintln!("skipping: opencode not installed");
        return;
    }

    let cwd = workspace("resume");
    let marker = "PINEAPPLE";

    // First process: create a session and put a distinctive exchange in it.
    let first = transport();
    let opened = first.open(&cwd).await.expect("session/new");
    let vendor = opened.vendor.clone();
    let reply = first
        .prompt(
            &vendor,
            &format!("Reply with exactly this one word and nothing else: {marker}"),
        )
        .await
        .expect("session/prompt");
    assert!(
        reply.text.to_uppercase().contains(marker),
        "setup failed; agent did not answer as asked: {:?}",
        reply.text
    );
    drop(first);

    // Second, entirely separate transport and process.
    let second = transport();
    let attached = second
        .attach(&vendor, &cwd)
        .await
        .expect("session/load should reach the existing session");

    assert_eq!(attached.vendor, vendor, "must attach to the same session");
    assert!(
        !attached.replayed.is_empty(),
        "session/load must replay the prior transcript, got nothing"
    );

    let user_turns: Vec<_> = attached
        .replayed
        .turns
        .iter()
        .filter(|t| t.speaker == Speaker::User)
        .collect();
    let agent_turns: Vec<_> = attached
        .replayed
        .turns
        .iter()
        .filter(|t| t.speaker == Speaker::Agent)
        .collect();

    assert!(
        user_turns.iter().any(|t| t.text.contains(marker)),
        "the replayed transcript must include the earlier user prompt; got {:?}",
        attached.replayed.turns
    );
    assert!(
        agent_turns
            .iter()
            .any(|t| t.text.to_uppercase().contains(marker)),
        "the replayed transcript must include the earlier agent reply; got {:?}",
        attached.replayed.turns
    );
}

/// A session that was attached (not created) must be promptable, since continuing a peer's
/// conversation is the point of the mesh.
#[tokio::test(flavor = "multi_thread")]
async fn attached_session_can_still_be_prompted() {
    let _guard = exclusive();
    if !opencode_present() {
        eprintln!("skipping: opencode not installed");
        return;
    }

    let cwd = workspace("context");
    let first = transport();
    let opened = first.open(&cwd).await.expect("session/new");
    let vendor = opened.vendor.clone();
    first
        .prompt(&vendor, "Remember the word PINEAPPLE. Reply with just: ok")
        .await
        .expect("first prompt");
    drop(first);

    let second = transport();
    second.attach(&vendor, &cwd).await.expect("session/load");

    let reply = second
        .prompt(&vendor, "What word did I ask you to remember? One word only.")
        .await
        .expect("prompt after attach should work");

    assert!(
        reply.text.to_uppercase().contains("PINEAPPLE"),
        "the reattached session must retain its context; got {:?}",
        reply.text
    );
}

/// Loading a session id the agent has never seen must surface an error rather than silently
/// hand back a blank session that looks live.
#[tokio::test(flavor = "multi_thread")]
async fn attaching_unknown_session_is_an_error() {
    let _guard = exclusive();
    if !opencode_present() {
        eprintln!("skipping: opencode not installed");
        return;
    }

    let acp = transport();
    let bogus = mesh_core::VendorSessionId::new("ses_definitely_not_a_real_session");

    let outcome = acp.attach(&bogus, &workspace("unknown")).await;

    assert!(
        outcome.is_err(),
        "attaching a nonexistent session must fail, got {outcome:?}"
    );
}

/// `session/list` must surface a session the mesh just created, so work started elsewhere is
/// discoverable rather than invisible.
#[tokio::test(flavor = "multi_thread")]
async fn created_session_appears_in_list() {
    let _guard = exclusive();
    if !opencode_present() {
        eprintln!("skipping: opencode not installed");
        return;
    }

    let acp = transport();
    let cwd = workspace("list");
    let opened = acp.open(&cwd).await.expect("session/new");

    let listed = acp.list_sessions(&cwd).await.expect("session/list");

    assert!(
        listed.contains(&opened.vendor),
        "session/list must include the session we just created"
    );
}
