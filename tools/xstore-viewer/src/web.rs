//! Web-server mode: serves the same frontend the desktop webview loads, over
//! HTTP + WebSocket.
//!
//! Session model:
//!   - One ZMQ client session per browser (sid): a lazily-spawned worker
//!     thread owning its own REQ socket, so concurrent users never share — or
//!     block — each other's connection. Idle sessions are reaped.
//!   - The SysStats PUB subscription is shared process-wide (every browser
//!     sees the same host stats): one SUB socket, snapshots fanned out to all
//!     connected browsers over a broadcast channel.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::header;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, get_service, post};
use axum::{Json, Router};
use tower_http::services::ServeDir;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::broadcast;
use tower_http::compression::CompressionLayer;

use crate::client::{self, ClientMsg, ClientRes};
use crate::stats::{self, StatsCmd};

const KNOWN_CMDS: [&str; 6] = ["connect", "info", "range_query", "unary_query", "aggregate", "ping"];
// Each session = one worker thread + one ZMQ context. Sids are stable per
// browser tab, so this bounds abuse (random sids), not normal multi-user use.
const MAX_SESSIONS: usize = 1024;
// Reap sessions idle for this long so per-browser worker threads and ZMQ
// contexts do not accumulate.
const SESSION_TTL_MS: u64 = 30 * 60 * 1000;

struct Session {
    tx: mpsc::Sender<ClientMsg>,
    last_used: AtomicU64,
}

pub struct WebState {
    sessions: Arc<DashMap<String, Arc<Session>>>,
    stats_tx: mpsc::Sender<StatsCmd>,
    stats_bcast: broadcast::Sender<Value>,
}

impl WebState {
    // Reuse the session for this sid or spawn its worker thread on first use.
    // Capacity is checked BEFORE entry(): DashMap's entry holds a shard lock,
    // and calling len() (which locks all shards) while it is live deadlocks.
    fn session_for(&self, sid: &str) -> Option<Arc<Session>> {
        let at_capacity = self.sessions.len() >= MAX_SESSIONS;
        let s = match self.sessions.entry(sid.to_string()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) if !at_capacity => {
                let s = Arc::new(Session {
                    tx: client::spawn(),
                    last_used: AtomicU64::new(now_ms()),
                });
                v.insert(s.clone());
                s
            }
            Entry::Vacant(_) => return None,
        };
        s.last_used.store(now_ms(), Ordering::Relaxed);
        Some(s)
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

struct ApiError {
    status: StatusCode,
    body: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.body).into_response()
    }
}

#[derive(Deserialize)]
struct Qp {
    sid: Option<String>,
}

// Command args arrive as the same JSON the desktop sends to Tauri invoke:
// the frontend transport shim forwards `invoke(cmd, args)` unchanged.
fn build_msg(cmd: &str, a: &Value, reply: client::Reply) -> Option<ClientMsg> {
    match cmd {
        "connect" => Some(ClientMsg::Connect {
            host: a["host"].as_str()?.to_string(),
            port: a["port"].as_u64()? as u16,
            reply,
        }),
        "info" => Some(ClientMsg::Info { reply }),
        "range_query" => Some(ClientMsg::RangeQuery {
            db: a["db"].as_str()?.to_string(),
            start: a["start"].as_u64()?,
            end: a["end"].as_u64()?,
            raw: a["raw"].as_bool().unwrap_or(false),
            reply,
        }),
        "unary_query" => Some(ClientMsg::UnaryQuery {
            db: a["db"].as_str()?.to_string(),
            ts: a["ts"].as_u64()?,
            reply,
        }),
        "aggregate" => Some(ClientMsg::Aggregate {
            op: a["op"].as_str()?.to_string(),
            db: a["db"].as_str()?.to_string(),
            start: a["start"].as_u64()?,
            end: a["end"].as_u64()?,
            col: a["col"].as_i64()? as i32,
            reply,
        }),
        "ping" => Some(ClientMsg::Ping { reply }),
        _ => None,
    }
}

async fn api(
    State(st): State<Arc<WebState>>,
    Path(cmd): Path<String>,
    Query(q): Query<Qp>,
    Json(args): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let sid = q.sid.filter(|s| !s.is_empty()).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        body: "missing session id (sid)".into(),
    })?;
    if !KNOWN_CMDS.contains(&cmd.as_str()) {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: format!("unknown command: {cmd}"),
        });
    }
    let sess = st
        .session_for(&sid)
        .ok_or_else(|| ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: format!("session limit reached ({MAX_SESSIONS})"),
        })?;
    // Mirror the desktop connect semantics: drop the previous stats
    // subscription BEFORE the attempt (a dead host eats the full handshake
    // timeout and the old feed must not keep repainting during it), then
    // re-subscribe only on success.
    let is_connect = cmd == "connect";
    let connect_target = if is_connect {
        Some((
            args["host"].as_str().map(str::to_string),
            args["port"].as_u64().unwrap_or(0) as u16,
        ))
    } else {
        None
    };
    if is_connect {
        let _ = st.stats_tx.send(StatsCmd::Unsubscribe);
    }
    let (reply, rx) = tokio::sync::oneshot::channel();
    let Some(msg) = build_msg(&cmd, &args, reply) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            body: format!("missing argument for command: {cmd}"),
        });
    };
    let tx = sess.tx.clone();
    let res: ClientRes = if tx.send(msg).is_err() {
        Err("client worker gone".into())
    } else {
        rx.await.unwrap_or_else(|_| Err("client worker gone".into()))
    };
    if let Some((Some(host), port)) = connect_target {
        if res.is_ok() && port > 1 {
            let _ = st.stats_tx.send(StatsCmd::Subscribe { host, port: port - 1 });
        }
    }
    res.map(Json).map_err(|body| ApiError {
        status: StatusCode::BAD_GATEWAY,
        body,
    })
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(st): State<Arc<WebState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_stats(socket, st.stats_bcast.clone()))
}

// One browser's share of the shared stats feed: the server holds the single
// SysStats SUB, this connection just receives broadcast snapshots.
async fn ws_stats(mut socket: WebSocket, bcast: broadcast::Sender<Value>) {
    let mut rx = bcast.subscribe();
    loop {
        tokio::select! {
            r = rx.recv() => match r {
                Ok(v) => {
                    if socket.send(Message::text(v.to_string())).await.is_err() {
                        break;
                    }
                }
                // Lagged: the newest snapshot supersedes the missed ones (the
                // SUB socket conflates to the latest tick anyway).
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                // Stats worker gone.
                Err(broadcast::error::RecvError::Closed) => break,
            },
            m = socket.recv() => match m {
                Some(Ok(Message::Close(_))) | None => break,
                _ => {}
            },
        }
    }
}

// Cache policy for the served frontend: HTML is revalidated (a new build
// shows up immediately; the BUILD string in the status line still flags a
// stale pair), assets are cached 24h. API and WS responses are untouched.
async fn static_cache(req: Request, next: Next) -> Response {
    let p = req.uri().path().to_string();
    let mut resp = next.run(req).await;
    if !p.starts_with("/api/") && resp.status().is_success() {
        let is_html = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|ct| ct.to_str().is_ok_and(|s| s.contains("html")));
        let val: header::HeaderValue =
            (if is_html { "no-cache" } else { "public, max-age=86400" }).parse().unwrap();
        resp.headers_mut().insert(header::CACHE_CONTROL, val);
    }
    resp
}

pub async fn run(port: u16) -> std::io::Result<()> {
    let (stats_tx, stats_rx) = stats::spawn();
    // ONE stats worker for the whole server, not per browser: every browser
    // receives the same host stats, so a single SUB socket + broadcast
    // fan-out is the entire cost. The std mpsc crosses into tokio here.
    let (btx, _) = broadcast::channel::<Value>(16);
    {
        let btx = btx.clone();
        tokio::task::spawn_blocking(move || {
            while let Ok(v) = stats_rx.recv() {
                let _ = btx.send(v);
            }
        });
    }
    let state = Arc::new(WebState {
        sessions: Arc::new(DashMap::new()),
        stats_tx,
        stats_bcast: btx,
    });
    // Reap idle sessions: Shutdown ends the worker thread (and its ZMQ
    // context), bounding thread count for long-lived server runs.
    {
        let sessions = state.sessions.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(60));
            loop {
                iv.tick().await;
                let now = now_ms();
                sessions.retain(|_, s| {
                    if now.saturating_sub(s.last_used.load(Ordering::Relaxed)) < SESSION_TTL_MS {
                        return true;
                    }
                    let _ = s.tx.send(ClientMsg::Shutdown);
                    false
                });
            }
        });
    }
    // The frontend dir sits next to the crate; fall back to the compile-time
    // manifest dir when the server is started from elsewhere.
    let dir = {
        let cwd = PathBuf::from("frontend");
        if cwd.is_dir() {
            cwd
        } else {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("frontend")
        }
    };
    let app = Router::new()
        .route("/api/{cmd}", post(api))
        .route("/api/stats", get(ws_upgrade))
        .fallback_service(get_service(ServeDir::new(dir)))
        .layer(axum::middleware::from_fn(static_cache))
        .layer(CompressionLayer::new())
        .with_state(state);
    // Bind every interface: the viewer is meant to be reachable from other
    // machines on the network, not just the host it runs on.
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    println!("xstore-viewer: web mode listening on http://0.0.0.0:{port}");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await
}
