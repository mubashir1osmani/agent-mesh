use mesh_pro::{BoardMember, BoardTask, BoardView, RelayClient};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Started {
    pub id: String,
    pub code: String,
    #[schemars(with = "i64")]
    pub rev: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MemberStatus {
    pub node: String,
    pub agent: String,
    pub email: String,
    pub presence: String,
    #[schemars(with = "i64")]
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BoardStatus {
    pub code: String,
    pub members: Vec<MemberStatus>,
    pub todo: Vec<String>,
    pub doing: Vec<String>,
    pub blocked: Vec<String>,
    pub review: Vec<String>,
    pub done: Vec<String>,
    pub mine: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TaskLines {
    pub tasks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptLine {
    pub id: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NoteDetail {
    pub by: String,
    pub kind: String,
    pub text: String,
    pub receipts: Vec<ReceiptLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TaskDetail {
    pub id: String,
    pub title: String,
    pub status: String,
    pub owner: Option<String>,
    #[schemars(with = "i64")]
    pub rev: u64,
    pub notes: Vec<NoteDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TaskOut {
    pub id: String,
    pub title: String,
    pub status: String,
    pub owner: Option<String>,
    #[schemars(with = "i64")]
    pub rev: u64,
}

#[derive(Serialize, Deserialize)]
struct Saved {
    #[serde(default)]
    nodes: BTreeMap<String, Current>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Current {
    id: String,
    code: String,
}

pub fn session_file(home: &Path) -> PathBuf {
    home.join("sessions.json")
}

pub struct Link<'a> {
    pub api_base: &'a str,
    pub token: &'a str,
    pub account_id: &'a str,
    pub machine_id: &'a str,
    pub node: &'a str,
    pub agent: &'a str,
    pub home: &'a Path,
}

pub async fn start(link: &Link<'_>, name: &str) -> Result<Started, String> {
    let client = client(link)?;
    let view = client
        .session_create(name, link.node, link.agent)
        .await
        .map_err(|err| err.to_string())?;
    let saved = save(link.home, link.node, &view).map(|()| started(view));
    finish(link, "session_start", name, saved).await
}

pub async fn join(link: &Link<'_>, code: &str) -> Result<BoardStatus, String> {
    let client = client(link)?;
    let view = client
        .session_join(code, link.node, link.agent)
        .await
        .map_err(|err| err.to_string())?;
    let saved =
        save(link.home, link.node, &view).map(|()| status_of(&view, link.account_id, link.node));
    finish(link, "session_join", code, saved).await
}

pub async fn status(link: &Link<'_>) -> Result<BoardStatus, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let view = client
        .session_get(&current.id, 0, link.node)
        .await
        .map_err(|err| err.to_string())?;
    finish(
        link,
        "session_status",
        "",
        Ok(status_of(&view, link.account_id, link.node)),
    )
    .await
}

pub async fn list_tasks(
    link: &Link<'_>,
    status_filter: Option<&str>,
    owner_filter: Option<&str>,
) -> Result<TaskLines, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let view = client
        .session_get(&current.id, 0, link.node)
        .await
        .map_err(|err| err.to_string())?;
    let tasks = view
        .tasks
        .iter()
        .filter(|task| status_filter.is_none_or(|status| task.status == status))
        .filter(|task| {
            owner_filter.is_none_or(|owner| {
                task.owner.as_deref().is_some_and(|value| {
                    value == owner
                        || value.starts_with(&format!("{owner}@"))
                        || task.owner_node.as_deref() == Some(owner)
                        || task.owner_account_id.as_deref() == Some(owner)
                })
            })
        })
        .map(|task| line(&view, task))
        .collect();
    finish(link, "task_list", "", Ok(TaskLines { tasks })).await
}

pub async fn get_task(link: &Link<'_>, task_id: &str) -> Result<TaskDetail, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let view = client
        .session_get(&current.id, 0, link.node)
        .await
        .map_err(|err| err.to_string())?;
    let task = view
        .tasks
        .iter()
        .find(|task| task.id == task_id)
        .ok_or_else(|| format!("no task {task_id}"))?;
    let ids: Vec<String> = task
        .notes
        .iter()
        .flat_map(|note| note.receipts.clone())
        .collect();
    let known = if ids.is_empty() {
        Vec::new()
    } else {
        client.receipts(&ids).await.map_err(|err| err.to_string())?
    };
    let notes = task
        .notes
        .iter()
        .map(|note| NoteDetail {
            by: note.by.clone(),
            kind: note.kind.clone(),
            text: note.text.clone(),
            receipts: note
                .receipts
                .iter()
                .map(|id| ReceiptLine {
                    state: known
                        .iter()
                        .find(|receipt| &receipt.id == id)
                        .map(|receipt| receipt.state.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    id: id.clone(),
                })
                .collect(),
        })
        .collect();
    finish(
        link,
        "task_get",
        task_id,
        Ok(TaskDetail {
            id: task.id.clone(),
            title: task.title.clone(),
            status: task.status.clone(),
            owner: task.owner.clone(),
            rev: task.rev,
            notes,
        }),
    )
    .await
}

pub async fn add_task(
    link: &Link<'_>,
    title: &str,
    owner: Option<&str>,
    note: Option<&str>,
) -> Result<TaskOut, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let task = client
        .task_add(&current.id, link.node, title, owner, note)
        .await
        .map_err(|err| err.to_string())?;
    finish(link, "task_add", title, Ok(task_out(task))).await
}

pub async fn claim_task(link: &Link<'_>, task_id: &str) -> Result<TaskOut, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let view = client
        .session_get(&current.id, 0, link.node)
        .await
        .map_err(|err| err.to_string())?;
    let rev = view
        .tasks
        .iter()
        .find(|task| task.id == task_id)
        .map(|task| task.rev)
        .ok_or_else(|| format!("no task {task_id}"))?;
    let task = client
        .task_claim(&current.id, task_id, link.node, rev)
        .await
        .map_err(|err| err.to_string())?;
    finish(link, "task_claim", task_id, Ok(task_out(task))).await
}

pub async fn update_task(
    link: &Link<'_>,
    task_id: &str,
    status: Option<&str>,
    owner: Option<&str>,
    title: Option<&str>,
) -> Result<TaskOut, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let view = client
        .session_get(&current.id, 0, link.node)
        .await
        .map_err(|err| err.to_string())?;
    let rev = view
        .tasks
        .iter()
        .find(|task| task.id == task_id)
        .map(|task| task.rev)
        .ok_or_else(|| format!("no task {task_id}"))?;
    let task = client
        .task_update(
            &current.id,
            task_id,
            link.node,
            rev,
            mesh_pro::TaskFields {
                status,
                owner,
                title,
            },
        )
        .await
        .map_err(|err| err.to_string())?;
    finish(link, "task_update", task_id, Ok(task_out(task))).await
}

pub async fn note_task(
    link: &Link<'_>,
    task_id: &str,
    text: &str,
    handoff: bool,
) -> Result<TaskDetail, String> {
    let client = client(link)?;
    let current = load(link.home, link.node)?;
    let kind = if handoff { "handoff" } else { "note" };
    client
        .task_note(&current.id, task_id, link.node, kind, text)
        .await
        .map_err(|err| err.to_string())?;
    finish(link, "task_note", task_id, get_task(link, task_id).await).await
}

async fn finish<T>(
    link: &Link<'_>,
    tool: &str,
    args: &str,
    result: Result<T, String>,
) -> Result<T, String> {
    let ok = result.is_ok();
    let error = result.as_ref().err().cloned().unwrap_or_default();
    trace(link, tool, args, ok, &error).await;
    result
}

fn client(link: &Link<'_>) -> Result<RelayClient, String> {
    RelayClient::new(link.api_base, link.token, link.machine_id).map_err(|err| err.to_string())
}

fn started(view: BoardView) -> Started {
    Started {
        id: view.session.id,
        code: view.code,
        rev: view.rev,
    }
}

fn task_out(task: BoardTask) -> TaskOut {
    TaskOut {
        id: task.id,
        title: task.title,
        status: task.status,
        owner: task.owner,
        rev: task.rev,
    }
}

fn status_of(view: &BoardView, account_id: &str, node: &str) -> BoardStatus {
    let mut todo = Vec::new();
    let mut doing = Vec::new();
    let mut blocked = Vec::new();
    let mut review = Vec::new();
    let mut done = Vec::new();
    let mut mine = Vec::new();
    for task in &view.tasks {
        let text = line(view, task);
        if task.owner.as_ref().is_some_and(|owner| {
            owns(
                owner,
                task.owner_account_id.as_deref(),
                task.owner_node.as_deref(),
                account_id,
                node,
            )
        }) {
            mine.push(text.clone());
        }
        match task.status.as_str() {
            "doing" => doing.push(text),
            "blocked" => blocked.push(text),
            "review" => review.push(text),
            "done" => done.push(text),
            _ => todo.push(text),
        }
    }
    BoardStatus {
        code: view.code.clone(),
        members: view
            .session
            .members
            .iter()
            .filter(|member| member_visible(member))
            .map(member_status)
            .collect(),
        todo,
        doing,
        blocked,
        review,
        done,
        mine,
    }
}

fn member_status(member: &BoardMember) -> MemberStatus {
    let age = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
        .saturating_sub(member.last_seen);
    MemberStatus {
        node: member.node.clone(),
        agent: member.agent.clone(),
        email: member.email.clone(),
        presence: if age <= 45 {
            "online".to_owned()
        } else {
            "idle".to_owned()
        },
        last_seen: member.last_seen,
    }
}

fn owns(
    owner: &str,
    owner_account: Option<&str>,
    owner_node: Option<&str>,
    account_id: &str,
    node: &str,
) -> bool {
    if let (Some(owner_account), Some(owner_node)) = (owner_account, owner_node)
        && !owner_account.is_empty()
        && !account_id.is_empty()
    {
        return owner_account == account_id && owner_node == node;
    }
    owner == node || owner.starts_with(&format!("{node}@"))
}

fn member_visible(member: &BoardMember) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    member.last_seen == 0 || now.saturating_sub(member.last_seen) <= 45
}

fn line(_view: &BoardView, task: &BoardTask) -> String {
    let who = task.owner.clone().unwrap_or_else(|| "-".to_owned());
    format!("{} [{}] {who} {}", task.id, task.status, task.title)
}

pub async fn trace(link: &Link<'_>, tool: &str, args: &str, ok: bool, error: &str) {
    let Ok(current) = load(link.home, link.node) else {
        return;
    };
    let Ok(client) = client(link) else {
        return;
    };
    let summary = format!("{tool} {args}");
    let summary: String = summary.chars().take(180).collect();
    let args: String = args.chars().take(180).collect();
    let _ = client
        .post_event(
            &current.id,
            &serde_json::json!({
                "node": link.node,
                "kind": "tool",
                "ok": ok,
                "summary": summary,
                "tool": tool,
                "args": args,
                "error": error,
            }),
        )
        .await;
}

fn save(home: &Path, node: &str, view: &BoardView) -> Result<(), String> {
    std::fs::create_dir_all(home).map_err(|err| err.to_string())?;
    let path = session_file(home);
    let mut saved = std::fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Saved>(&bytes).ok())
        .unwrap_or_else(|| Saved {
            nodes: BTreeMap::new(),
        });
    saved.nodes.insert(
        node.to_owned(),
        Current {
            id: view.session.id.clone(),
            code: view.code.clone(),
        },
    );
    let bytes = serde_json::to_vec_pretty(&saved).map_err(|err| err.to_string())?;
    std::fs::write(path, bytes).map_err(|err| err.to_string())
}

fn load(home: &Path, node: &str) -> Result<Current, String> {
    let bytes = std::fs::read(session_file(home))
        .map_err(|_| "no session; call session_start or session_join".to_owned())?;
    let saved: Saved = serde_json::from_slice(&bytes).map_err(|err| err.to_string())?;
    saved
        .nodes
        .get(node)
        .cloned()
        .ok_or_else(|| "no session; call session_start or session_join".to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use axum::extract::{Path, State};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Fake {
        inner: Arc<Mutex<Board>>,
    }

    struct Board {
        tasks: Vec<Value>,
        next: u64,
    }

    async fn serve() -> String {
        let app = Router::new()
            .route("/v1/sessions", post(create))
            .route("/v1/sessions/join", post(join_route))
            .route("/v1/sessions/{id}", get(fetch))
            .route("/v1/sessions/{id}/tasks", post(add))
            .route("/v1/sessions/{id}/tasks/{task}/claim", post(claim))
            .route("/v1/sessions/{id}/tasks/{task}", post(update))
            .route("/v1/sessions/{id}/tasks/{task}/notes", post(note))
            .route("/v1/relay/receipts", post(receipts))
            .with_state(Fake {
                inner: Arc::new(Mutex::new(Board {
                    tasks: Vec::new(),
                    next: 1,
                })),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn view(state: &Board, code: &str) -> Value {
        json!({
            "session": {
                "id": "ses_demo",
                "team_id": "team",
                "name": "support-bot",
                "created_by": "alice",
                "created_at": 1,
                "closed_at": null,
                "members": [
                    {"account_id":"a","machine_id":"m-a","node":"codex","agent":"codex","email":"alice@example.com","joined_at":1,"last_seen":9000000000u64},
                    {"account_id":"b","machine_id":"m-b","node":"codex-b","agent":"codex","email":"bob@example.com","joined_at":1,"last_seen":9000000000u64},
                    {"account_id":"old","machine_id":"m-old","node":"codex-old","agent":"codex","email":"old@example.com","joined_at":1,"last_seen":1}
                ],
                "rev": 2
            },
            "code": code,
            "tasks": state.tasks,
            "rev": 2
        })
    }

    async fn create(State(app): State<Fake>, Json(body): Json<Value>) -> Json<Value> {
        let state = app.inner.lock().unwrap();
        let name = body["name"].as_str().unwrap_or("support-bot");
        Json(view(&state, &format!("acme/{name}")))
    }

    async fn join_route(State(app): State<Fake>, Json(body): Json<Value>) -> Json<Value> {
        let state = app.inner.lock().unwrap();
        Json(view(
            &state,
            body["code"].as_str().unwrap_or("acme/support-bot"),
        ))
    }

    async fn fetch(State(app): State<Fake>) -> Json<Value> {
        let state = app.inner.lock().unwrap();
        Json(view(&state, "acme/support-bot"))
    }

    async fn add(State(app): State<Fake>, Json(body): Json<Value>) -> Json<Value> {
        let mut state = app.inner.lock().unwrap();
        let n = state.tasks.len() + 1;
        let task = json!({
            "id": format!("t{n}"),
            "session_id": "ses_demo",
            "title": body["title"],
            "status": if body["owner"].is_null() { "todo" } else { "doing" },
            "owner": body["owner"],
            "created_by": body["node"],
            "updated_at": 1,
            "rev": 1,
            "notes": []
        });
        state.tasks.push(task.clone());
        Json(task)
    }

    async fn claim(
        State(app): State<Fake>,
        Path((_, task_id)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
        let mut state = app.inner.lock().unwrap();
        let node = body["node"].as_str().unwrap_or("").to_owned();
        let task = state
            .tasks
            .iter_mut()
            .find(|task| task["id"] == task_id)
            .unwrap();
        if let Some(owner) = task["owner"].as_str()
            && owner != node
            && !owner.starts_with(&format!("{node}@"))
        {
            return Err((
                axum::http::StatusCode::CONFLICT,
                Json(json!({"error": format!("owned by {owner}"), "rev": task["rev"]})),
            ));
        }
        let person = if node == "codex" { "alice" } else { "bob" };
        task["owner"] = json!(format!("{node}@{person}"));
        task["owner_node"] = json!(node);
        task["owner_account_id"] = json!(if node == "codex" { "alice" } else { "bob" });
        task["status"] = json!("doing");
        task["rev"] = json!(task["rev"].as_u64().unwrap_or(1) + 1);
        Ok(Json(task.clone()))
    }

    async fn update(
        State(app): State<Fake>,
        Path((_, task_id)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let mut state = app.inner.lock().unwrap();
        let task = state
            .tasks
            .iter_mut()
            .find(|task| task["id"] == task_id)
            .unwrap();
        if let Some(status) = body["status"].as_str() {
            task["status"] = json!(status);
        }
        task["rev"] = json!(task["rev"].as_u64().unwrap_or(1) + 1);
        Json(task.clone())
    }

    async fn note(
        State(app): State<Fake>,
        Path((_, task_id)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let mut state = app.inner.lock().unwrap();
        let id = format!("msg-{}", state.next);
        state.next += 1;
        let task = state
            .tasks
            .iter_mut()
            .find(|task| task["id"] == task_id)
            .unwrap();
        task["notes"].as_array_mut().unwrap().push(json!({
            "at": 2,
            "by": body["node"],
            "kind": body["kind"],
            "text": body["text"],
            "receipts": [id]
        }));
        Json(task.clone())
    }

    async fn receipts(State(app): State<Fake>, Json(body): Json<Value>) -> Json<Value> {
        let state = app.inner.lock().unwrap();
        let ids = body["ids"].as_array().cloned().unwrap_or_default();
        let receipts: Vec<Value> = ids
            .iter()
            .filter_map(|id| id.as_str())
            .map(
                |id| json!({"id": id, "state": if state.next > 1 { "surfaced" } else { "queued" }}),
            )
            .collect();
        Json(json!({ "receipts": receipts }))
    }

    #[tokio::test]
    async fn tools_share_a_board_and_show_receipts() {
        let base = serve().await;
        let home = std::env::temp_dir().join(format!("mesh-session-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        std::fs::create_dir_all(&home).unwrap();
        let alice_home = home.join("alice");
        let bob_home = home.join("bob");
        let alice = Link {
            api_base: &base,
            token: "alice",
            account_id: "alice",
            machine_id: "m-a",
            node: "codex",
            agent: "codex",
            home: &alice_home,
        };
        let bob = Link {
            api_base: &base,
            token: "bob",
            account_id: "bob",
            machine_id: "m-b",
            node: "codex-b",
            agent: "codex",
            home: &bob_home,
        };
        let started = start(&alice, "support-bot").await.unwrap();
        assert_eq!(started.code, "acme/support-bot");
        join(&bob, "acme/support-bot").await.unwrap();
        add_task(&alice, "add retry on webhook 5xx", None, None)
            .await
            .unwrap();
        add_task(&alice, "write tests for retry", None, None)
            .await
            .unwrap();
        let claimed = claim_task(&alice, "t1").await.unwrap();
        assert_eq!(claimed.owner.as_deref(), Some("codex@alice"));
        let conflict = claim_task(&bob, "t1").await.unwrap_err();
        assert!(conflict.contains("owned by codex@alice"), "{conflict}");
        claim_task(&bob, "t2").await.unwrap();
        let updated = update_task(&alice, "t1", Some("review"), None, None)
            .await
            .unwrap();
        assert_eq!(updated.status, "review");
        note_task(&alice, "t1", "backoff 1s,2s,4s", true)
            .await
            .unwrap();
        let detail = get_task(&bob, "t1").await.unwrap();
        assert_eq!(detail.notes[0].kind, "handoff");
        assert_eq!(detail.notes[0].receipts[0].state, "surfaced");
        let lines = list_tasks(&bob, None, None).await.unwrap();
        assert!(
            lines
                .tasks
                .iter()
                .any(|line| line.contains("t1 [review] codex@alice"))
        );
        let board = status(&alice).await.unwrap();
        assert_eq!(board.review.len(), 1);
        assert!(board.mine.iter().any(|line| line.contains("t1")));
        assert!(
            board
                .members
                .iter()
                .all(|member| member.node != "codex-old")
        );
        assert_eq!(board.members.len(), 2);
        let _ = std::fs::remove_dir_all(&home);
    }
}
