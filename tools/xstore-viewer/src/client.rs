use std::sync::mpsc::{self, Receiver, Sender};

use prost::Message as _;
use tokio::sync::oneshot;

use crate::convert;
use crate::proto::xstore as pb;

pub type ClientRes = Result<serde_json::Value, String>;
// One-shot reply handle: the caller awaits the receiver (async, no thread
// parked in a blocking wait) — the worker thread owns the blocking ZMQ I/O.
pub type Reply = oneshot::Sender<ClientRes>;

pub enum ClientMsg {
    Connect {
        host: String,
        port: u16,
        reply: Reply,
    },
    Info {
        reply: Reply,
    },
    RangeQuery {
        db: String,
        start: u64,
        end: u64,
        // table mode: skip the plot-mode downsampling, return every row
        raw: bool,
        reply: Reply,
    },
    UnaryQuery {
        db: String,
        ts: u64,
        reply: Reply,
    },
    Aggregate {
        op: String,
        db: String,
        start: u64,
        end: u64,
        col: i32,
        reply: Reply,
    },
    Ping {
        reply: Reply,
    },
    Shutdown,
}

const TIMEOUT_MS: i32 = 60_000;
// Handshake-only timeout: a dead/unreachable server must fail fast so the
// UI isn't frozen for the full business-RPC timeout.
const CONNECT_TIMEOUT_MS: i32 = 5_000;

pub fn spawn() -> Sender<ClientMsg> {
    let (tx, rx) = mpsc::channel::<ClientMsg>();
    std::thread::Builder::new()
        .name("xstore-client".into())
        .spawn(move || worker(rx))
        .expect("failed to spawn XStore client worker thread");
    tx
}

// Send one command to the worker and await its reply. The blocking ZMQ
// roundtrip happens on the worker thread; the caller only suspends on the
// oneshot, so an async runtime never parks a runtime thread (no
// spawn_blocking hop, no blocking-pool saturation under load).
pub async fn call(tx: &Sender<ClientMsg>, build: impl FnOnce(Reply) -> ClientMsg) -> ClientRes {
    let (reply, rx) = oneshot::channel();
    if tx.send(build(reply)).is_err() {
        return Err("client worker gone".into());
    }
    rx.await.unwrap_or_else(|_| Err("client worker gone".into()))
}

fn new_req(ctx: &zmq::Context, host: &str, port: u16) -> Result<zmq::Socket, String> {
    let sock = ctx.socket(zmq::REQ).map_err(|e| e.to_string())?;
    sock.set_linger(0).map_err(|e| e.to_string())?;
    sock.set_rcvtimeo(CONNECT_TIMEOUT_MS).map_err(|e| e.to_string())?;
    sock.set_sndtimeo(CONNECT_TIMEOUT_MS).map_err(|e| e.to_string())?;
    let addr = format!("tcp://{}:{}", host, port);
    sock.connect(&addr).map_err(|e| e.to_string())?;
    Ok(sock)
}

// Ping warmup + business timeouts for a fresh REQ socket (shared by the
// explicit Connect command and the transparent auto-reconnect below).
fn warm_up(sock: &zmq::Socket) -> Result<(), String> {
    // Ping RPC forces the lazy TCP + ZMTP handshake so the first business
    // RPC doesn't eat it (mirrors the C++ client).
    let ping = pb::Request {
        payload: Some(pb::request::Payload::Ping(pb::ReqCmd { r_type: 0 })),
    };
    rpc_bytes(sock, &ping).map_err(|e| format!("ping failed: {}", e))?;
    // handshake done; widen timeouts for business RPCs
    let _ = sock.set_rcvtimeo(TIMEOUT_MS);
    let _ = sock.set_sndtimeo(TIMEOUT_MS);
    Ok(())
}

// Business command needs a live socket: reuse the current one, or — if it was
// dropped after a timeout — transparently reconnect to the last connected
// host. Without this, a dropped socket left every subsequent query failing
// with "not connected" until the user clicked Connect again. The caller
// sends the error reply itself: a oneshot sender is consumed on send.
fn ensure_sock<'a>(
    sock: &'a mut Option<zmq::Socket>,
    conn: &Option<(String, u16)>,
    ctx: &zmq::Context,
) -> Result<&'a zmq::Socket, String> {
    if sock.is_some() {
        return Ok(sock.as_ref().unwrap());
    }
    let Some((host, port)) = conn else {
        return Err("not connected".into());
    };
    let s = new_req(ctx, host, *port)?;
    warm_up(&s)?;
    *sock = Some(s);
    Ok(sock.as_ref().unwrap())
}

fn rpc_bytes(sock: &zmq::Socket, req: &pb::Request) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf).map_err(|e| e.to_string())?;
    sock.send(buf, zmq::DONTWAIT).map_err(|e| e.to_string())?;
    // Streamed RangeQuery/BatchQuery replies arrive as several frames: the
    // server sends ~64MB chunks of ONE logical REP's serialized bytes,
    // SNDMORE on all but the last. A protobuf message is just a byte stream,
    // so the frames concatenate in order into the original message before
    // parsing (same reassembly as the C++ client's SendRecv). Single-frame
    // replies (everything else) end the loop after the first recv, so this
    // also works unchanged against pre-streaming servers.
    let mut msg = zmq::Message::new();
    sock.recv(&mut msg, 0)
        .map_err(|e| format!("ZMQ recv failed (timeout or disconnect): {}", e))?;
    let mut out = msg.to_vec();
    while sock.get_rcvmore().map_err(|e| e.to_string())? {
        let mut next = zmq::Message::new();
        sock.recv(&mut next, 0)
            .map_err(|e| format!("ZMQ recv failed (timeout or disconnect): {}", e))?;
        out.extend_from_slice(&next);
    }
    Ok(out)
}

fn info_request() -> pb::Request {
    pb::Request {
        payload: Some(pb::request::Payload::Info(pb::ReqCmd { r_type: 0 })),
    }
}

fn fetch_info(sock: &zmq::Socket) -> Result<serde_json::Value, String> {
    let bytes = rpc_bytes(sock, &info_request())?;
    let rep = pb::SysInfoRep::decode(&bytes[..])
        .map_err(|e| format!("failed to parse SysInfo_REP: {}", e))?;
    Ok(convert::sys_info_to_json(&rep))
}

fn send_info(sock: &zmq::Socket, reply: Reply) {
    let _ = reply.send(fetch_info(sock));
}

fn worker(rx: Receiver<ClientMsg>) {
    let ctx = zmq::Context::new();
    let mut sock: Option<zmq::Socket> = None;
    // Last successful connect target; a dropped socket (recv timeout) is
    // auto-reconnected here on the next command (see ensure_sock).
    let mut conn: Option<(String, u16)> = None;

    while let Some(msg) = rx.recv().ok() {
        match msg {
            ClientMsg::Connect { host, port, reply } => {
                let s = match new_req(&ctx, &host, port) {
                    Ok(s) => s,
                    Err(e) => {
                        // failed connect keeps the previous server: queries
                        // still target conn/sock as before
                        let _ = reply.send(Err(e));
                        continue;
                    }
                };
                let t0 = std::time::Instant::now();
                match warm_up(&s) {
                    Ok(()) => {
                        let ping_ms = t0.elapsed().as_secs_f64() * 1000.0;
                        let t1 = std::time::Instant::now();
                        let res = fetch_info(&s);
                        let info_ms = t1.elapsed().as_secs_f64() * 1000.0;
                        sock = Some(s);
                        conn = Some((host, port));
                        match res {
                            Ok(mut v) => {
                                v["connectMs"] = serde_json::json!({ "ping": ping_ms, "info": info_ms });
                                let _ = reply.send(Ok(v));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            ClientMsg::Info { reply } => match ensure_sock(&mut sock, &conn, &ctx) {
                Ok(s) => send_info(s, reply),
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            ClientMsg::RangeQuery { db, start, end, raw, reply } => match ensure_sock(&mut sock, &conn, &ctx) {
                Ok(s) => {
                    let req = pb::Request {
                        payload: Some(pb::request::Payload::Rangequery(pb::RangeQueryReq {
                            r_type: 0,
                            start_time: start,
                            end_time: end,
                            db_name: db,
                        })),
                    };
                    let t0 = std::time::Instant::now();
                    match rpc_bytes(s, &req) {
                        Ok(bytes) => match pb::RangeQueryRep::decode(&bytes[..]) {
                            Ok(rep) => {
                                let mut v = convert::range_query_to_json(&rep, raw);
                                // ZMQ roundtrip only — isolates server+wire time
                                // from JS merge/render time on the frontend
                                v["rpcMs"] = (t0.elapsed().as_secs_f64() * 1000.0).into();
                                let _ = reply.send(Ok(v));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(format!("failed to parse RangeQuery_REP: {}", e)));
                            }
                        },
                        Err(e) => {
                            // REQ socket is in undefined state after a timeout:
                            // drop it; the next command auto-reconnects (ensure_sock).
                            sock = None;
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            ClientMsg::UnaryQuery { db, ts, reply } => match ensure_sock(&mut sock, &conn, &ctx) {
                Ok(s) => {
                    let req = pb::Request {
                        payload: Some(pb::request::Payload::Unaryquery(pb::UnaryQueryReq {
                            r_type: 0,
                            timestamp: ts,
                            db_name: db,
                        })),
                    };
                    match rpc_bytes(s, &req) {
                        Ok(bytes) => match pb::UnaryQueryRep::decode(&bytes[..]) {
                            Ok(rep) => {
                                let _ = reply.send(Ok(convert::unary_query_to_json(&rep)));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(format!("failed to parse UnaryQuery_REP: {}", e)));
                            }
                        },
                        Err(e) => {
                            sock = None;
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            ClientMsg::Aggregate { op, db, start, end, col, reply } => match ensure_sock(&mut sock, &conn, &ctx) {
                Ok(s) => {
                    let payload = match op.as_str() {
                        "min" => pb::request::Payload::Min(pb::MinReq {
                            r_type: 0,
                            start_time: start,
                            end_time: end,
                            db_name: db,
                            column_idx: col,
                        }),
                        "max" => pb::request::Payload::Max(pb::MaxReq {
                            r_type: 0,
                            start_time: start,
                            end_time: end,
                            db_name: db,
                            column_idx: col,
                        }),
                        "sum" => pb::request::Payload::Sum(pb::SumReq {
                            r_type: 0,
                            start_time: start,
                            end_time: end,
                            db_name: db,
                            column_idx: col,
                        }),
                        _ => pb::request::Payload::Avg(pb::AvgReq {
                            r_type: 0,
                            start_time: start,
                            end_time: end,
                            db_name: db,
                            column_idx: col,
                        }),
                    };
                    let req = pb::Request { payload: Some(payload) };
                    match rpc_bytes(s, &req) {
                        Ok(bytes) => {
                            let res = match op.as_str() {
                                "min" => pb::MinRep::decode(&bytes[..]).map(|r| {
                                    serde_json::json!({
                                        "value": convert::fdata_to_json(r.op_result.as_ref()),
                                        "status": r.op_status,
                                    })
                                }),
                                "max" => pb::MaxRep::decode(&bytes[..]).map(|r| {
                                    serde_json::json!({
                                        "value": convert::fdata_to_json(r.op_result.as_ref()),
                                        "status": r.op_status,
                                    })
                                }),
                                "sum" => pb::SumRep::decode(&bytes[..]).map(|r| {
                                    serde_json::json!({ "value": r.op_result, "status": r.op_status })
                                }),
                                _ => pb::AvgRep::decode(&bytes[..]).map(|r| {
                                    serde_json::json!({ "value": r.op_result, "status": r.op_status })
                                }),
                            };
                            let _ = reply.send(res.map_err(|e| e.to_string()));
                        }
                        Err(e) => {
                            sock = None;
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            ClientMsg::Ping { reply } => match ensure_sock(&mut sock, &conn, &ctx) {
                Ok(s) => {
                    let ping = pb::Request {
                        payload: Some(pb::request::Payload::Ping(pb::ReqCmd { r_type: 0 })),
                    };
                    let t = std::time::Instant::now();
                    match rpc_bytes(s, &ping) {
                        Ok(_) => {
                            let _ = reply.send(Ok(serde_json::json!({ "ms": t.elapsed().as_secs_f64() * 1000.0 })));
                        }
                        Err(e) => {
                            sock = None;
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            ClientMsg::Shutdown => break,
        }
    }
}
