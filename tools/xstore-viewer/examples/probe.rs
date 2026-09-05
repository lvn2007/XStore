use std::sync::mpsc;
fn main() {
    let tx = xstore_viewer::client::spawn();
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::Connect { host: "127.0.0.1".into(), port: 5555, reply: rtx }).unwrap();
    let _ = rrx.recv().unwrap();
    // known data row: row 1699 of 2020.XSTORE -> ts 1577838499 (int64 col0)
    let (rtx, rrx) = mpsc::channel();
    tx.send(xstore_viewer::client::ClientMsg::RangeQuery { db: "BENCHDB".into(), start: 1577838490, end: 1577838510, raw: false, reply: rtx }).unwrap();
    let q = rrx.recv().unwrap().unwrap();
    println!("rowsReturned={} columns={}", q["rowsReturned"], q["columns"]);
    if let Some(ts) = q["timestamps"].as_array() { println!("ts: {:?}", ts.iter().take(6).map(|x| x.as_u64()).collect::<Vec<_>>()); }
    if let Some(cols) = q["cols"].as_object() {
        for (k, v) in cols.iter().take(4) {
            let a = v.as_array().unwrap();
            println!("col {}: {:?}", k, a.iter().take(12).map(|x| x.as_f64()).collect::<Vec<_>>());
        }
    }
    tx.send(xstore_viewer::client::ClientMsg::Shutdown).unwrap();
}
