// Diagnostic: find the size at which prost 0.13's RangeQueryRep::decode fails
// on a valid message. Reproduces the viewer's "failed to parse RangeQuery_REP"
// for wide-DB chunks (BENCH_DB: 1M rows x 128-col rows ~= 1.4GB on the wire).
// Builds a message, serializes it, drops the source tree, then decodes the
// bytes — exactly what client.rs does after reassembling the streamed frames.
use prost::Message;
use xstore_viewer::proto::xstore as pb;

fn build(rows: u64, cols: u64) -> pb::RangeQueryRep {
    let mut rep = pb::RangeQueryRep::default();
    rep.query_response.reserve(rows as usize);
    for i in 0..rows {
        let mut fd = Vec::with_capacity(cols as usize);
        for c in 0..cols {
            fd.push(pb::FData {
                one_of_data: Some(pb::f_data::OneOfData::DoubleData((i as f64) * 1e-6 + (c as f64) * 1e-9)),
            });
        }
        rep.query_response.push(pb::UnaryQueryRep {
            timestamp: i,
            data: Some(pb::RowData { field_data: fd }),
            query_status: None,
        });
    }
    rep
}

#[test]
fn prost_decode_size_limit() {
    let mut control_ok = false;
    for rows in [100_000u64, 500_000, 750_000, 1_000_000] {
        let rep = build(rows, 128);
        let cap = rep.encoded_len();
        let mut bytes = Vec::with_capacity(cap);
        rep.encode(&mut bytes).expect("encode failed");
        drop(rep); // free the source tree before decoding to bound peak memory
        match pb::RangeQueryRep::decode(&bytes[..]) {
            Ok(r) => {
                println!("rows={rows} wire_bytes={} ({:.2} GB) -> OK (decoded {} rows)",
                    bytes.len(), bytes.len() as f64 / 1e9, r.query_response.len());
                if rows == 100_000 { control_ok = true; }
            }
            Err(e) => {
                println!("rows={rows} wire_bytes={} ({:.2} GB) -> FAILED: {:?}",
                    bytes.len(), bytes.len() as f64 / 1e9, e);
            }
        }
    }
    assert!(control_ok, "100K-row control decode failed unexpectedly");
}

// Decode the REAL BENCH_DB 1M-row chunk (dumped by client_src/Python) that the
// viewer fails on. If prost decodes these exact bytes fine, the decoder and the
// data are exonerated and the bug is in the viewer's ZMQ reassembly.
#[test]
fn prost_decode_real_bench_chunk() {
    let path = std::env::var("BENCH_BIN").unwrap_or_else(|_| "/tmp/kilo/bench_1m.bin".into());
    let bytes = std::fs::read(&path).expect("read bench dump");
    println!("file={path} bytes={} ({:.3} GB)", bytes.len(), bytes.len() as f64 / 1e9);
    match pb::RangeQueryRep::decode(&bytes[..]) {
        Ok(r) => println!("prost OK rows={} status={:?}", r.query_response.len(), r.query_status),
        Err(e) => println!("prost FAILED: {:?}", e),
    }
}
