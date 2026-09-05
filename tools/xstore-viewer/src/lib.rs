pub mod client;
pub mod convert;
pub mod proto;
pub mod stats;
pub mod web;

use std::sync::mpsc;

use tauri::menu::{Menu, MenuItem, PredefinedMenuItem, Submenu};
use tauri::{Emitter, Manager, State, WebviewUrl, WebviewWindowBuilder};

pub struct AppState {
    tx: mpsc::Sender<client::ClientMsg>,
    stats_tx: mpsc::Sender<stats::StatsCmd>,
}

// Tauri runs sync commands on the main (UI) thread; a blocking ZMQ wait there
// freezes the whole window (the "hang"). Every command is async: the blocking
// roundtrip happens on the client worker thread, this task only suspends on
// the oneshot reply.
async fn run_cmd(
    state: State<'_, AppState>,
    build: impl FnOnce(client::Reply) -> client::ClientMsg + Send + 'static,
) -> Result<serde_json::Value, String> {
    let tx = state.tx.clone();
    client::call(&tx, build).await
}

#[tauri::command]
async fn connect(state: State<'_, AppState>, host: String, port: u16) -> Result<serde_json::Value, String> {
    let stats_tx = state.stats_tx.clone();
    let host_rpc = host.clone();
    // Drop the previous subscription BEFORE the connect attempt: a dead host
    // eats the full 5s handshake timeout, and during that window the old
    // server's feed must not keep repainting the monitor (a failed connect
    // looked like the app was still on the old server).
    let _ = stats_tx.send(stats::StatsCmd::Unsubscribe);
    let res = run_cmd(state, move |reply| client::ClientMsg::Connect { host: host_rpc, port, reply }).await;
    // stats feed rides a PUB socket on main port - 1 (server default)
    if res.is_ok() && port > 1 {
        let _ = stats_tx.send(stats::StatsCmd::Subscribe { host, port: port - 1 });
    }
    res
}

#[tauri::command]
async fn info(state: State<'_, AppState>) -> Result<serde_json::Value, String> {
    run_cmd(state, |reply| client::ClientMsg::Info { reply }).await
}

#[tauri::command]
async fn range_query(
    state: State<'_, AppState>,
    db: String,
    start: u64,
    end: u64,
    // table mode: no downsampling — every row the server returned
    raw: bool,
) -> Result<serde_json::Value, String> {
    run_cmd(state, move |reply| client::ClientMsg::RangeQuery { db, start, end, raw, reply }).await
}

#[tauri::command]
async fn unary_query(state: State<'_, AppState>, db: String, ts: u64) -> Result<serde_json::Value, String> {
    run_cmd(state, move |reply| client::ClientMsg::UnaryQuery { db, ts, reply }).await
}

#[tauri::command]
async fn ping(state: State<'_, AppState>) -> Result<serde_json::Value, String> {
    run_cmd(state, |reply| client::ClientMsg::Ping { reply }).await
}

#[tauri::command]
async fn aggregate(
    state: State<'_, AppState>,
    op: String,
    db: String,
    start: u64,
    end: u64,
    col: i32,
) -> Result<serde_json::Value, String> {
    run_cmd(
        state,
        move |reply| client::ClientMsg::Aggregate { op, db, start, end, col, reply },
    )
    .await
}

// "About XStore Viewer" is a custom menu item (handled in run()'s
// on_menu_event): the native NSAboutPanel is positioned by the system — it
// cannot be centered — and its text area has no clickable links, so the About
// page is a small window of its own, centered on every open.
fn show_about(app: &tauri::AppHandle) {
    if let Some(w) = app.get_webview_window("about") {
        // already open: re-center instead of stacking a duplicate
        let _ = w.center();
        let _ = w.set_focus();
        return;
    }
    let _ = WebviewWindowBuilder::new(app, "about", WebviewUrl::App("about.html".into()))
        .title("About XStore Viewer")
        .inner_size(460.0, 330.0)
        .resizable(false)
        .center()
        .build();
}

// Native menu bar. The default menu names the app after the Cargo package
// ("xstore-viewer"), so the app menu is built explicitly.
// The web-mode UI has no native menu, so the header "About" button opens the
// same page: a centered Tauri window here, or a new browser tab in web mode
// (the frontend picks the path — see app.js).
#[tauri::command]
fn open_about(app: tauri::AppHandle) {
    show_about(&app);
}

fn build_menu(app: &tauri::App) -> tauri::Result<()> {
    let handle = app.handle();
    let about = MenuItem::with_id(handle, "about", "About XStore Viewer", true, None::<&str>)?;
    let app_menu = Submenu::with_items(
        handle,
        "XStore Viewer",
        true,
        &[
            &about,
            &PredefinedMenuItem::separator(handle)?,
            &PredefinedMenuItem::hide(handle, None)?,
            &PredefinedMenuItem::hide_others(handle, None)?,
            &PredefinedMenuItem::show_all(handle, None)?,
            &PredefinedMenuItem::separator(handle)?,
            &PredefinedMenuItem::quit(handle, None)?,
        ],
    )?;
    let edit_menu = Submenu::with_items(
        handle,
        "Edit",
        true,
        &[
            &PredefinedMenuItem::undo(handle, None)?,
            &PredefinedMenuItem::redo(handle, None)?,
            &PredefinedMenuItem::separator(handle)?,
            &PredefinedMenuItem::cut(handle, None)?,
            &PredefinedMenuItem::copy(handle, None)?,
            &PredefinedMenuItem::paste(handle, None)?,
            &PredefinedMenuItem::select_all(handle, None)?,
        ],
    )?;
    let window_menu = Submenu::with_items(
        handle,
        "Window",
        true,
        &[
            &PredefinedMenuItem::minimize(handle, None)?,
            &PredefinedMenuItem::maximize(handle, None)?,
            &PredefinedMenuItem::fullscreen(handle, None)?,
        ],
    )?;
    let menu = Menu::with_items(handle, &[&app_menu, &edit_menu, &window_menu])?;
    app.set_menu(menu)?;
    Ok(())
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let tx = client::spawn();
    tauri::Builder::default()
        // external links in the About window (repo URL, mailto) open in the
        // system browser / mail client; the webview swallows plain <a> clicks
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            build_menu(app)?;
            let (stats_tx, stats_rx) = stats::spawn();
            app.manage(AppState {
                tx,
                stats_tx,
            });
            // decoded host-stats snapshots -> Tauri event; the frontend
            // listens instead of polling (server pushes every ~2s)
            let handle = app.handle().clone();
            tauri::async_runtime::spawn_blocking(move || {
                while let Ok(v) = stats_rx.recv() {
                    let _ = handle.emit("sys-stats", v);
                }
            });
            // this tauri version has no config-level window icon; set it here
            if let Some(w) = app.get_webview_window("main") {
                let bytes = include_bytes!("../icons/icon.png");
                let icon = tauri::image::Image::from_bytes(bytes).expect("invalid icon PNG");
                let _ = w.set_icon(icon);
            }
            Ok(())
        })
        .on_menu_event(|app, event| {
            if event.id() == "about" {
                show_about(app);
            }
        })
        .invoke_handler(tauri::generate_handler![
            connect,
            info,
            range_query,
            unary_query,
            aggregate,
            ping,
            open_about
        ])
        .run(tauri::generate_context!())
        .expect("failed to run XStore Viewer");
    Ok(())
}

// Web-server mode: the same frontend over HTTP + WebSocket, one ZMQ client
// session per browser (see web.rs). Own runtime — the Tauri desktop path
// above keeps its.
pub fn run_web(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    Ok(rt.block_on(web::run(port))?)
}
