use mesh_core::registry::{ChainRejection, Route};
use mesh_core::{AgentId, AskChain, SessionRegistry, SessionState, VendorSessionId};
use std::path::PathBuf;

fn agent() -> AgentId {
    AgentId::new("opencode")
}

fn cwd() -> PathBuf {
    PathBuf::from("/tmp/project")
}

/// A newly registered session has no vendor session behind it yet, so the only correct first
/// move is to create one. Routing it straight to a prompt would send a pinned id to a
/// create-only CLI (grok/gemini) and hard-error.
#[test]
fn new_session_routes_to_create() {
    let reg = SessionRegistry::new();
    let s = reg.register_new(agent(), cwd());

    assert_eq!(reg.route(&s).unwrap(), Route::Create { cwd: cwd() });
}

/// The regression that matters most: once a session is live, turn 2 must NOT route back to
/// create. `grok -s` and `gemini --session-id` hard-error when the id already exists, so a
/// single "upsert" code path passes turn 1 and breaks turn 2.
#[test]
fn live_session_routes_to_prompt_not_create() {
    let reg = SessionRegistry::new();
    let s = reg.register_new(agent(), cwd());
    let vendor = VendorSessionId::new("ses_abc123");

    reg.mark_live(&s, vendor.clone()).unwrap();

    let route = reg.route(&s).unwrap();
    assert_eq!(route, Route::PromptDirect { vendor });
    assert!(
        !matches!(route, Route::Create { .. }),
        "a live session must never be re-created"
    );
}

/// A session whose process died still exists vendor-side, so it must be reattached (ACP
/// `session/load`) before prompting rather than created fresh, which would lose all context.
#[test]
fn detached_session_routes_to_reattach() {
    let reg = SessionRegistry::new();
    let s = reg.register_new(agent(), cwd());
    let vendor = VendorSessionId::new("ses_abc123");
    reg.mark_live(&s, vendor.clone()).unwrap();

    reg.mark_detached(&s).unwrap();

    assert_eq!(
        reg.route(&s).unwrap(),
        Route::ReattachThenPrompt {
            vendor,
            cwd: cwd()
        }
    );
}

/// Detaching a session that never started must not invent a vendor id.
#[test]
fn detaching_unstarted_session_stays_unstarted() {
    let reg = SessionRegistry::new();
    let s = reg.register_new(agent(), cwd());

    reg.mark_detached(&s).unwrap();

    assert_eq!(reg.get(&s).unwrap().state, SessionState::NotStarted);
    assert_eq!(reg.route(&s).unwrap(), Route::Create { cwd: cwd() });
}

/// A session discovered from the vendor (e.g. via session/list) was not created by the mesh, so
/// it must start detached and reattach rather than be created.
#[test]
fn existing_session_starts_detached() {
    let reg = SessionRegistry::new();
    let vendor = VendorSessionId::new("ses_found");

    let s = reg.register_existing(agent(), cwd(), vendor.clone());

    assert_eq!(
        reg.route(&s).unwrap(),
        Route::ReattachThenPrompt { vendor, cwd: cwd() }
    );
}

#[test]
fn unknown_session_is_an_error_not_a_silent_create() {
    let reg = SessionRegistry::new();
    let bogus = mesh_core::SessionRef::parse("opencode:does-not-exist");

    assert!(reg.route(&bogus).is_err());
    assert!(reg.get(&bogus).is_err());
}

#[test]
fn sessions_are_isolated_per_ref() {
    let reg = SessionRegistry::new();
    let a = reg.register_new(agent(), cwd());
    let b = reg.register_new(agent(), cwd());
    assert_ne!(a, b, "each registration must mint a distinct ref");

    reg.mark_live(&a, VendorSessionId::new("ses_a")).unwrap();

    assert_eq!(reg.route(&b).unwrap(), Route::Create { cwd: cwd() });
}

#[test]
fn list_filters_by_agent() {
    let reg = SessionRegistry::new();
    reg.register_new(AgentId::new("opencode"), cwd());
    reg.register_new(AgentId::new("codex"), cwd());

    assert_eq!(reg.list(None).len(), 2);
    assert_eq!(reg.list(Some(&AgentId::new("codex"))).len(), 1);
    assert_eq!(reg.list(Some(&AgentId::new("claude"))).len(), 0);
}

/// Handing the mesh a raw vendor id must find the existing entry rather than duplicate it,
/// otherwise two mesh refs would race on one vendor session.
#[test]
fn find_by_vendor_matches_agent_and_id() {
    let reg = SessionRegistry::new();
    let s = reg.register_new(agent(), cwd());
    let vendor = VendorSessionId::new("ses_xyz");
    reg.mark_live(&s, vendor.clone()).unwrap();

    assert_eq!(reg.find_by_vendor(&agent(), &vendor).unwrap().session, s);
    // Same vendor id, different agent: must not match.
    assert!(reg.find_by_vendor(&AgentId::new("codex"), &vendor).is_none());
}

// --- loop guard ---

/// A session asking itself makes no progress and must be refused.
#[test]
fn ask_chain_refuses_self_ask() {
    let a = mesh_core::SessionRef::parse("claude:a");
    let chain = AskChain::root().push(&a, 8).unwrap();

    assert_eq!(
        chain.push(&a, 8),
        Err(ChainRejection::SelfAsk { session: a })
    );
}

/// Returning to a session already in the relay is the whole point of the mesh: ask codex, then go
/// back and tell opencode what it said. Refusing every revisit would break that workflow, so A->B->A
/// must be ALLOWED and bounded by depth instead.
#[test]
fn ask_chain_allows_returning_to_an_earlier_session() {
    let a = mesh_core::SessionRef::parse("opencode:a");
    let b = mesh_core::SessionRef::parse("codex:b");

    let chain = AskChain::root().push(&a, 8).unwrap().push(&b, 8).unwrap();

    let back = chain
        .push(&a, 8)
        .expect("relaying an answer back to an earlier session is legitimate");
    assert_eq!(back.depth(), 3);
}

/// Even an acyclic chain must terminate, or N agents could relay forever.
#[test]
fn ask_chain_enforces_depth_limit() {
    let chain = AskChain::root();
    let a = mesh_core::SessionRef::parse("x:1");
    let b = mesh_core::SessionRef::parse("x:2");

    let chain = chain.push(&a, 2).unwrap();
    let chain = chain.push(&b, 2).unwrap();

    assert_eq!(
        chain.push(&mesh_core::SessionRef::parse("x:3"), 2),
        Err(ChainRejection::TooDeep { limit: 2 })
    );
}

#[test]
fn ask_chain_allows_distinct_hops_and_tracks_depth() {
    let chain = AskChain::root();
    assert_eq!(chain.depth(), 0);

    let chain = chain
        .push(&mesh_core::SessionRef::parse("a:1"), 8)
        .unwrap()
        .push(&mesh_core::SessionRef::parse("b:2"), 8)
        .unwrap()
        .push(&mesh_core::SessionRef::parse("c:3"), 8)
        .unwrap();

    assert_eq!(chain.depth(), 3);
    assert_eq!(chain.hops().len(), 3);
}

/// Pushing must not mutate the parent chain; sibling asks from one session are independent.
#[test]
fn ask_chain_is_immutable_across_branches() {
    let root = AskChain::root().push(&mesh_core::SessionRef::parse("a:1"), 8).unwrap();

    let left = root.push(&mesh_core::SessionRef::parse("b:2"), 8).unwrap();
    let right = root.push(&mesh_core::SessionRef::parse("c:3"), 8).unwrap();

    assert_eq!(root.depth(), 1, "parent chain must be unchanged");
    assert_eq!(left.depth(), 2);
    assert_eq!(right.depth(), 2);
    // The two branches must not see each other's hops.
    assert!(!left.hops().contains(&mesh_core::SessionRef::parse("c:3")));
    assert!(!right.hops().contains(&mesh_core::SessionRef::parse("b:2")));
}

// --- cwd resolution ---

/// A relative path must be made absolute: ACP requires absolute paths, and a relative one would
/// resolve against the mesh process's directory rather than the caller's.
#[test]
fn absolute_cwd_resolves_relative_paths() {
    let resolved = mesh_core::absolute_cwd(std::path::Path::new(".")).expect("cwd exists");
    assert!(resolved.is_absolute());
}

/// An absolute path that does not exist must fail here rather than being trusted, or it surfaces
/// later as an opaque spawn failure from the agent.
#[test]
fn absolute_cwd_rejects_missing_directory() {
    assert!(mesh_core::absolute_cwd(std::path::Path::new("/definitely/not/real/xyz")).is_err());
}

/// A path to a file is not a working directory.
#[test]
fn absolute_cwd_rejects_a_file() {
    let path = std::env::temp_dir().join("mesh-cwd-probe.txt");
    std::fs::write(&path, b"x").expect("write");
    assert!(mesh_core::absolute_cwd(&path).is_err());
    let _ = std::fs::remove_file(&path);
}
