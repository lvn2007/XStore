#[cfg_attr(mobile, tauri::mobile_entry_point)]
fn main() {
    xstore_viewer::run().expect("failed to run XStore Viewer");
}
