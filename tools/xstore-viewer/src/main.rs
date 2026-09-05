#[cfg_attr(mobile, tauri::mobile_entry_point)]
fn main() {
    // `xstore-viewer web [--port N]` runs the web server (default port 8080,
    // bound to 0.0.0.0); no subcommand runs the desktop app.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(i) = args.iter().position(|a| a == "web") {
        let port = args[i + 1..]
            .iter()
            .position(|a| a == "--port")
            .and_then(|j| args.get(i + 1 + j + 1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(8080);
        xstore_viewer::run_web(port).expect("failed to run XStore Viewer web server");
        return;
    }
    xstore_viewer::run().expect("failed to run XStore Viewer");
}
