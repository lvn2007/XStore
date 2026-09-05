use std::sync::mpsc::{self, Receiver, Sender};

use prost::Message as _;

use crate::proto::xstore as pb;

// Host resource stats arrive as raw SysStats_PUB frames on a dedicated PUB
// socket (main port - 1 by default) — no RPC envelope. One dedicated thread
// owns the SUB socket; decoded snapshots cross into the app as JSON values
// and are emitted as Tauri events by the emitter task in lib.rs.
pub enum StatsCmd {
    Subscribe { host: String, port: u16 },
    Unsubscribe,
}

pub fn spawn() -> (Sender<StatsCmd>, Receiver<serde_json::Value>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<StatsCmd>();
    let (data_tx, data_rx) = mpsc::channel::<serde_json::Value>();
    std::thread::Builder::new()
        .name("xstore-stats".into())
        .spawn(move || worker(cmd_rx, data_tx))
        .expect("failed to spawn XStore stats subscriber thread");
    (cmd_tx, data_rx)
}

fn worker(cmd_rx: Receiver<StatsCmd>, out: Sender<serde_json::Value>) {
    let ctx = zmq::Context::new();
    // A command consumed by the inner loop's try_recv is parked here so the
    // outer loop processes it — this is what lets a new Subscribe REPLACE a
    // live subscription (the old code dropped such commands, so the worker
    // kept receiving from the previous host's feed forever)
    let mut pending: Option<StatsCmd> = None;
    loop {
        let cmd = match pending.take() {
            Some(c) => c,
            None => match cmd_rx.recv() {
                Ok(c) => c,
                Err(_) => break,
            },
        };
        let (host, port) = match cmd {
            StatsCmd::Subscribe { host, port } => (host, port),
            StatsCmd::Unsubscribe => continue, // nothing subscribed at this level
        };
        let sock = match ctx.socket(zmq::SUB) {
            Ok(s) => s,
            Err(_) => continue,
        };
        // conflate: keep only the newest tick so a stalled frontend never
        // drains a backlog of stale snapshots
        let _ = sock.set_conflate(true);
        let _ = sock.set_subscribe(b""); // empty filter = subscribe to all
        if sock.connect(&format!("tcp://{}:{}", host, port)).is_err() {
            continue;
        }
        loop {
            // Any pending command (unsubscribe OR a new subscribe) takes
            // priority: park it and let the outer loop act on it. Non-blocking
            // recv + short sleep, so commands are honored within ~50ms.
            if let Ok(c) = cmd_rx.try_recv() {
                pending = Some(c);
                break;
            }
            let mut msg = zmq::Message::new();
            match sock.recv(&mut msg, zmq::DONTWAIT) {
                Ok(_) => {
                    let bytes = msg.to_vec();
                    if let Ok(s) = pb::SysStatsPub::decode(&bytes[..]) {
                        let _ = out.send(serde_json::json!({
                            "cpu": s.cpu_util_percent,
                            "memTotal": s.mem_total_bytes,
                            "memUsed": s.mem_used_bytes,
                            "netRx": s.net_rx_k_bps,
                            "netTx": s.net_tx_k_bps,
                            "diskRdIops": s.disk_read_iops,
                            "diskWrIops": s.disk_write_iops,
                            "diskFree": s.disk_free_bytes,
                            "diskTotal": s.disk_total_bytes,
                            "snapMs": s.snapshot_unix_ms,
                        }));
                    }
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
        drop(sock);
    }
}
