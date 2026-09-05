// End-to-end check of the viewer's ZMQ client against a live XStore server
// running the streamed RangeQuery protocol: a response bigger than the
// server's 64MB chunk size arrives as MULTIPLE frames (SNDMORE on all but
// the last) that must be concatenated in order into one RangeQuery_REP
// before parsing (client::rpc_bytes). This test issues such a query, then
// runs more RPCs on the same socket — if the first reply weren't fully
// drained, the next reply would read a stale frame and the REQ socket
// protocol would desync.
//
// Gated on the XStore server binary existing (default <repo>/build/XStore,
// override with XSTORE_BIN) — skipped otherwise so `cargo test` stays green
// on machines without the C++ build.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::mpsc;
use std::time::Duration;

use xstore_viewer::client::{spawn, ClientMsg, ClientRes, Reply};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 19998;
// 2026-01-01 00:00:00Z, +59 days (second resolution). TESTDB holds one
// all-holes year file (2026, rowLength 128): 5,097,600 hole entries ~=
// 66MB on the wire -> 2 frames at the server's 64MB chunk size.
const START: u64 = 1_767_225_600;
const END: u64 = START + 5_097_600 - 1;
const EXPECTED_ROWS: u64 = 5_097_600;

type Tx = mpsc::Sender<ClientMsg>;

// The reply path is a tokio oneshot; the test stays std-threaded, so a
// throwaway current-thread runtime bridges the oneshot await with a timeout.
fn call_with_timeout(
    tx: &Tx,
    d: Duration,
    build: impl FnOnce(Reply) -> ClientMsg,
) -> Option<ClientRes> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .ok()?;
    rt.block_on(async move {
        let (reply, rx) = tokio::sync::oneshot::channel();
        tx.send(build(reply)).ok()?;
        // rx itself resolves to a Result (Err if the worker dies); None on
        // timeout, on worker death, or on send failure
        match tokio::time::timeout(d, rx).await {
            Ok(Ok(res)) => Some(res),
            Ok(Err(_)) | Err(_) => None,
        }
    })
}

fn connect(tx: &Tx) -> Option<ClientRes> {
    call_with_timeout(tx, Duration::from_secs(3), |reply| {
        ClientMsg::Connect { host: HOST.into(), port: PORT, reply }
    })
}

fn range_query(tx: &Tx, db: &str, start: u64, end: u64, raw: bool, d: Duration) -> Option<ClientRes> {
    call_with_timeout(tx, d, move |reply| {
        ClientMsg::RangeQuery { db: db.into(), start, end, raw, reply }
    })
}

fn ping(tx: &Tx) -> Option<ClientRes> {
    call_with_timeout(tx, Duration::from_secs(10), |reply| ClientMsg::Ping { reply })
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

fn find_server() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("XSTORE_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let p = repo_root().join("build/XStore");
    p.is_file().then_some(p)
}

#[test]
fn range_query_reassembles_streamed_frames() {
    let Some(bin) = find_server() else {
        eprintln!("skipping: XStore server binary not found (build it or set XSTORE_BIN)");
        return;
    };
    let mut server: Child = Command::new(&bin)
        .args(["-H", HOST, "-p", &PORT.to_string()])
        .current_dir(repo_root())
        .spawn()
        .expect("failed to spawn XStore server");

    let tx = spawn();

    // wait for the server to accept the ZMQ handshake
    let mut connected = false;
    for _ in 0..50 {
        if let Some(Ok(v)) = connect(&tx) {
            assert!(v["dbs"].as_array().map(|a| a.len()).unwrap_or(0) >= 1);
            connected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(connected, "XStore server did not come up on port {PORT}");

    // 1) the multi-frame query: response ~=66MB -> 2+ streamed frames
    let v = range_query(&tx, "TESTDB", START, END, false, Duration::from_secs(120))
        .expect("range query timed out")
        .expect("range query failed");
    assert_eq!(v["rowsReturned"].as_u64(), Some(EXPECTED_ROWS));
    assert_eq!(v["status"].as_str(), Some(""));
    let plotted = v["timestamps"].as_array().unwrap().len();
    assert!((1..=20_000).contains(&plotted), "plot mode downsampled to {plotted}");

    // 2) same socket must still be in sync: this reply must be the ping's
    //    reply, not a leftover streamed frame of the query above
    let v = ping(&tx)
        .expect("ping timed out")
        .expect("ping failed");
    assert!(v["ms"].as_f64().unwrap() >= 0.0);

    // 3) a second small range query on the same socket
    let v = range_query(&tx, "TESTDB", START, START + 999, false, Duration::from_secs(30))
        .expect("small range query timed out")
        .expect("small range query failed");
    assert_eq!(v["rowsReturned"].as_u64(), Some(1_000));

    let _ = tx.send(ClientMsg::Shutdown);
    let _ = server.kill();
    let _ = server.wait();
}
