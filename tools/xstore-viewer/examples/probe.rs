use xstore_viewer::client::{call, spawn, ClientMsg};

fn main() {
    let tx = spawn();
    // replies are tokio oneshots — a single-thread runtime bridges the awaits
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _ = rt
        .block_on(call(&tx, |reply| ClientMsg::Connect {
            host: "127.0.0.1".into(),
            port: 5555,
            reply,
        }))
        .unwrap();
    // known data row: row 1699 of 2020.XSTORE -> ts 1577838499 (int64 col0)
    let q = rt
        .block_on(call(&tx, |reply| ClientMsg::RangeQuery {
            db: "BENCHDB".into(),
            start: 1577838490,
            end: 1577838510,
            raw: false,
            reply,
        }))
        .unwrap();
    println!("rowsReturned={} columns={}", q["rowsReturned"], q["columns"]);
    if let Some(ts) = q["timestamps"].as_array() {
        println!("ts: {:?}", ts.iter().take(6).map(|x| x.as_u64()).collect::<Vec<_>>());
    }
    if let Some(cols) = q["cols"].as_object() {
        for (k, v) in cols.iter().take(4) {
            let a = v.as_array().unwrap();
            println!("col {}: {:?}", k, a.iter().take(12).map(|x| x.as_f64()).collect::<Vec<_>>());
        }
    }
    tx.send(ClientMsg::Shutdown).unwrap();
}
