use clap::{CommandFactory, Parser, Subcommand};

/// XStore Viewer: view time-series data managed by an XStore server.
///
/// With no subcommand it runs the native desktop (Tauri) app. Use the `web`
/// subcommand to serve the same frontend over HTTP + WebSocket instead (for
/// headless hosts or plain browser use).
#[derive(Parser, Debug)]
#[command(name = "xstore-viewer", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run as a web server (HTTP + WebSocket) instead of the desktop app.
    Web {
        /// Port to bind the web server to.
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
    },
}

// A headless Linux host has neither an X11 nor a Wayland session to draw the
// desktop window in. macOS/Windows always have a display, so they report false.
#[cfg(target_os = "linux")]
fn headless() -> bool {
    std::env::var_os("DISPLAY").is_none() && std::env::var_os("WAYLAND_DISPLAY").is_none()
}

#[cfg(not(target_os = "linux"))]
fn headless() -> bool {
    false
}

// Point the user at web mode and print the full help, then exit non-zero.
fn print_headless_help() -> ! {
    eprintln!(
        "\nxstore-viewer: could not open the desktop app — no display is available on this host\n\
         (it looks headless). Run it as a web server instead and open the printed\n\
         URL in a browser:\n\n    xstore-viewer web [--port N]      (default port 8080)\n\n\
         Full options:\n"
    );
    eprintln!("{}", Cli::command().render_help());
    std::process::exit(1)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Web { port }) => {
            xstore_viewer::run_web(port).expect("failed to run XStore Viewer web server");
        }
        // No subcommand: the desktop app is the default.
        None => {
            if headless() {
                print_headless_help();
            }
            // Safety net for hosts we couldn't pre-detect as headless (e.g. a
            // display env var that points at a dead server): a display/GTK init
            // failure panics inside Tauri. Surface the same guidance instead
            // of leaving the user with a bare panic.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| xstore_viewer::run())) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("failed to run XStore Viewer: {e}");
                    std::process::exit(1);
                }
                Err(_) => print_headless_help(),
            }
        }
    }
}
