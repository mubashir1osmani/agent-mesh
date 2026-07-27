use mesh_core::{AgentId, TransportError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

/// An inbound message from the agent that is not a response to one of our requests.
///
/// ACP is bidirectional: the agent both pushes notifications (`session/update`) and *asks us
/// things* (`session/request_permission`, `fs/read_text_file`). An agent-initiated request
/// carries both a `method` and an `id`; failing to answer one stalls the turn forever, so the
/// id is preserved here rather than discarded.
#[derive(Debug, Clone)]
pub struct Inbound {
    pub method: String,
    pub params: serde_json::Value,
    /// `Some` when the agent expects a response at this id; `None` for a plain notification.
    pub id: Option<serde_json::Value>,
}

impl Inbound {
    pub fn is_request(&self) -> bool {
        self.id.is_some()
    }
}

/// A long-lived JSON-RPC-over-stdio connection to one agent process.
///
/// Owns the read loop so notifications keep draining even when no request is in flight. This
/// matters for ACP: `session/load` delivers the replayed transcript as notifications that
/// arrive *before* the response, so a request/response-only client would drop them.
pub struct Connection {
    agent: AgentId,
    stdin: Mutex<ChildStdin>,
    next_id: AtomicI64,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Result<serde_json::Value, RpcFailure>>>>>,
    subscribers: Arc<RwLock<Vec<mpsc::UnboundedSender<Inbound>>>>,
    child: Mutex<Child>,
}

#[derive(Debug, Clone)]
pub struct RpcFailure {
    pub code: i64,
    pub message: String,
}

impl Connection {
    /// Spawn `program args...` and start pumping its stdout.
    pub async fn spawn(
        agent: AgentId,
        program: &str,
        args: &[String],
        cwd: &std::path::Path,
    ) -> Result<Arc<Self>, TransportError> {
        let mut child = Command::new(program)
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| TransportError::Spawn {
                command: format!("{program} {}", args.join(" ")),
                source,
            })?;

        let stdin = child.stdin.take().ok_or_else(|| TransportError::Protocol {
            agent: agent.clone(),
            detail: "child stdin was not piped".to_owned(),
        })?;
        let stdout = child.stdout.take().ok_or_else(|| TransportError::Protocol {
            agent: agent.clone(),
            detail: "child stdout was not piped".to_owned(),
        })?;

        // Drain stderr so a chatty agent cannot fill its pipe buffer and deadlock.
        if let Some(stderr) = child.stderr.take() {
            let agent_for_log = agent.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(agent = %agent_for_log, "stderr: {line}");
                }
            });
        }

        let conn = Arc::new(Self {
            agent: agent.clone(),
            stdin: Mutex::new(stdin),
            next_id: AtomicI64::new(1),
            pending: Arc::new(Mutex::new(HashMap::new())),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            child: Mutex::new(child),
        });

        let pending = Arc::clone(&conn.pending);
        let subscribers = Arc::clone(&conn.subscribers);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) if line.trim().is_empty() => continue,
                    Ok(Some(line)) => {
                        Self::dispatch(&line, &pending, &subscribers).await;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            // Process is gone: fail everything still waiting rather than hang forever.
            let mut guard = pending.lock().await;
            for (_, tx) in guard.drain() {
                let _ = tx.send(Err(RpcFailure {
                    code: -1,
                    message: "connection closed".to_owned(),
                }));
            }
        });

        Ok(conn)
    }

    async fn dispatch(
        line: &str,
        pending: &Mutex<HashMap<i64, oneshot::Sender<Result<serde_json::Value, RpcFailure>>>>,
        subscribers: &RwLock<Vec<mpsc::UnboundedSender<Inbound>>>,
    ) {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(line) else {
            tracing::warn!("non-JSON line from agent: {line}");
            return;
        };

        // Route on `method`, not on `id`: an agent-initiated *request* has both, so keying off
        // `id` alone would misfile permission prompts as responses and hang the turn.
        if let Some(method) = msg.get("method").and_then(serde_json::Value::as_str) {
            let inbound = Inbound {
                method: method.to_owned(),
                params: msg.get("params").cloned().unwrap_or(serde_json::Value::Null),
                id: msg.get("id").cloned(),
            };
            let subs = subscribers.read().await;
            for tx in subs.iter() {
                let _ = tx.send(inbound.clone());
            }
            return;
        }

        if let Some(id) = msg.get("id").and_then(serde_json::Value::as_i64) {
            let outcome = match msg.get("error") {
                Some(err) => Err(RpcFailure {
                    code: err.get("code").and_then(serde_json::Value::as_i64).unwrap_or(-1),
                    message: err
                        .get("message")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown error")
                        .to_owned(),
                }),
                None => Ok(msg.get("result").cloned().unwrap_or(serde_json::Value::Null)),
            };
            if let Some(tx) = pending.lock().await.remove(&id) {
                let _ = tx.send(outcome);
            }
        }
    }

    /// Subscribe to every notification from this connection. Subscribing before issuing a
    /// request is what makes `session/load`'s pre-response replay observable.
    pub async fn subscribe(&self) -> mpsc::UnboundedReceiver<Inbound> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers.write().await.push(tx);
        rx
    }

    /// Drop subscribers whose receiver has been closed, so a long-lived connection does not
    /// accumulate dead senders across many prompts.
    pub async fn prune_subscribers(&self) {
        let mut subs = self.subscribers.write().await;
        let live: Vec<_> = subs.drain(..).filter(|tx| !tx.is_closed()).collect();
        *subs = live;
    }

    pub async fn request<Req, Res>(&self, method: &str, params: &Req) -> Result<Res, TransportError>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let envelope = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let mut wire = serde_json::to_vec(&envelope).map_err(|e| TransportError::Protocol {
            agent: self.agent.clone(),
            detail: format!("could not encode {method}: {e}"),
        })?;
        wire.push(b'\n');

        {
            let mut stdin = self.stdin.lock().await;
            stdin
                .write_all(&wire)
                .await
                .map_err(|_| TransportError::ConnectionClosed {
                    agent: self.agent.clone(),
                })?;
            stdin
                .flush()
                .await
                .map_err(|_| TransportError::ConnectionClosed {
                    agent: self.agent.clone(),
                })?;
        }

        let raw = match rx.await {
            Ok(Ok(value)) => value,
            Ok(Err(failure)) => {
                return Err(TransportError::AgentRefused {
                    agent: self.agent.clone(),
                    message: format!("{} (code {})", failure.message, failure.code),
                });
            }
            Err(_) => {
                return Err(TransportError::ConnectionClosed {
                    agent: self.agent.clone(),
                });
            }
        };

        serde_json::from_value(raw).map_err(|e| TransportError::Decode {
            agent: self.agent.clone(),
            detail: format!("{method}: {e}"),
        })
    }

    pub async fn notify<Req: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &Req,
    ) -> Result<(), TransportError> {
        let envelope = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let mut wire = serde_json::to_vec(&envelope).map_err(|e| TransportError::Protocol {
            agent: self.agent.clone(),
            detail: format!("could not encode {method}: {e}"),
        })?;
        wire.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&wire)
            .await
            .map_err(|_| TransportError::ConnectionClosed {
                agent: self.agent.clone(),
            })?;
        stdin
            .flush()
            .await
            .map_err(|_| TransportError::ConnectionClosed {
                agent: self.agent.clone(),
            })
    }

    /// Respond to a request the agent made of us (permission prompts, file reads).
    pub async fn respond(
        &self,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<(), TransportError> {
        let envelope = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result });
        let mut wire = serde_json::to_vec(&envelope).map_err(|e| TransportError::Protocol {
            agent: self.agent.clone(),
            detail: format!("could not encode response: {e}"),
        })?;
        wire.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&wire)
            .await
            .map_err(|_| TransportError::ConnectionClosed {
                agent: self.agent.clone(),
            })?;
        stdin
            .flush()
            .await
            .map_err(|_| TransportError::ConnectionClosed {
                agent: self.agent.clone(),
            })
    }

    pub async fn shutdown(&self) {
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
    }
}
