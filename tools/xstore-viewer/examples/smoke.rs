// Headless smoke test for the ZMQ+proto client path (no GUI needed).
// Usage: XSTORE_HOST=127.0.0.1 XSTORE_PORT=5555 cargo run --example smoke
use std::sync::mpsc;

fn main() {
    let host = std::env::var("XSTORE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("XSTORE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5555);

    let tx = xstore_viewer::client::spawn();

    // 1. Connect (ping warmup + SysInfo)
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::Connect {
        host: host.clone(),
        port,
        reply: rtx,
    })
    .unwrap();
    let info = rrx.recv().unwrap().expect("connect failed");
    println!("== CONNECT OK ==");
    println!("server: {}", info);

    // 2. Range query: first 1000 rows of 2020 (epoch seconds)
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::RangeQuery {
        db: "BENCHDB".into(),
        start: 1_577_836_800,
        end: 1_577_836_800 + 999,
        raw: false,
        reply: rtx,
    })
    .unwrap();
    let q = rrx.recv().unwrap().expect("range query failed");
    println!("== RANGE QUERY OK ==");
    println!(
        "rowsReturned={} rowsPlotted={} columns={} firstTs={:?}",
        q["rowsReturned"],
        q["rowsPlotted"],
        q["columns"],
        q["timestamps"].get(0)
    );
    if let Some(cols) = q["cols"].as_object() {
        for (k, v) in cols.iter().take(3) {
            let arr = v.as_array().unwrap();
            println!(
                "  col {} ({} pts): head = {:?}",
                k,
                arr.len(),
                arr.iter().take(5).map(|x| x.to_string()).collect::<Vec<_>>()
            );
        }
    }

    // 3. Unary query at start of 2020
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::UnaryQuery {
        db: "BENCHDB".into(),
        ts: 1_577_836_800,
        reply: rtx,
    })
    .unwrap();
    let u = rrx.recv().unwrap().expect("unary query failed");
    println!("== UNARY QUERY OK ==");
    println!("{}", serde_json::to_string_pretty(&u).unwrap().chars().take(800).collect::<String>());

    // 4. AVG of column 0 over full year 2020
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::Aggregate {
        op: "avg".into(),
        db: "BENCHDB".into(),
        start: 1_577_836_800,
        end: 1_609_455_999,
        col: 0,
        reply: rtx,
    })
    .unwrap();
    let a = rrx.recv().unwrap().expect("aggregate failed");
    println!("== AVG OK ==");
    println!("{}", a);

    tx.send(xstore_viewer::client::ClientMsg::Shutdown).unwrap();
    println!("SMOKE PASS");
}
