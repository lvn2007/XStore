use std::sync::mpsc;
use std::time::Duration;

use xstore_viewer::client::{call, spawn, ClientMsg, ClientRes, Reply};

fn rpc(rt: &tokio::runtime::Runtime, tx: &mpsc::Sender<ClientMsg>, build: impl FnOnce(Reply) -> ClientMsg) -> ClientRes {
    rt.block_on(call(tx, build))
}

fn main() {
    // args: [host] [port] [db] [start] [end] — defaults match the local bench box
    let a: Vec<String> = std::env::args().collect();
    let host = a.get(1).cloned().unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(5555);
    let db = a.get(3).cloned().unwrap_or_else(|| "BENCHDB".into());
    let start: u64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1577838490);
    let end: u64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(1577838510);

    let tx = spawn();
    // replies are tokio oneshots — a single-thread runtime bridges the awaits
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // 1) main RPC port: connect (ping+info) — verify rowLength + per-file sizes
    match rpc(&rt, &tx, |reply| ClientMsg::Connect {
        host: host.clone(),
        port,
        reply,
    }) {
        Ok(info) => {
            println!("server={} workers={}", info["serverAddr"], info["threadCount"]);
            for d in info["dbs"].as_array().unwrap() {
                println!(
                    "db={} res={} sizeBytes={} rowLength={}",
                    d["name"], d["resolution"], d["sizeBytes"], d["rowLength"]
                );
                for f in d["files"].as_array().unwrap().iter().take(4) {
                    println!(
                        "  file={} actual={} apparent={}",
                        f["name"], f["actualBytes"], f["apparentBytes"]
                    );
                }
            }
        }
        Err(e) => panic!("connect failed: {}", e),
    }

    // 2) range query — verify rowSizes sizing stats
    let q = rpc(&rt, &tx, |reply| ClientMsg::RangeQuery {
        db,
        start,
        end,
        raw: false,
        reply,
    })
    .unwrap();
    println!("rowsReturned={} rowSizes={}", q["rowsReturned"], q["rowSizes"]);

    // 3) stats PUB port (main-1): subscribe, collect 3 snapshots, unsubscribe
    let (stx, srx) = xstore_viewer::stats::spawn();
    stx.send(xstore_viewer::stats::StatsCmd::Subscribe {
        host,
        port: port - 1,
    })
    .unwrap();
    let t0 = std::time::Instant::now();
    let mut got = 0;
    while let Ok(v) = srx.recv_timeout(Duration::from_secs(10)) {
        println!(
            "stats: cpu={} mem={}/{} net={}/{} diskRdWr={}/{} free/total={}/{} snapMs={}",
            v["cpu"], v["memUsed"], v["memTotal"],
            v["netRx"], v["netTx"],
            v["diskRdIops"], v["diskWrIops"],
            v["diskFree"], v["diskTotal"], v["snapMs"]
        );
        got += 1;
        if got >= 3 {
            break;
        }
    }
    stx.send(xstore_viewer::stats::StatsCmd::Unsubscribe).unwrap();
    println!("stats: {} snapshots in {:.1}s — {}", got, t0.elapsed().as_secs_f64(), if got >= 3 { "PASS" } else { "FAIL" });

    tx.send(ClientMsg::Shutdown).unwrap();
    std::process::exit(if got >= 3 { 0 } else { 1 });
}
