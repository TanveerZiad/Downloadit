use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    sync::{Mutex as AsyncMutex, RwLock, Semaphore, watch},
    time::interval,
};
use tokio_util::io::ReaderStream;
use tower_http::services::ServeDir;
use url::Url;
use uuid::Uuid;

const MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const MIN_PART: u64 = 1024 * 1024; // 1 MiB minimum per segment
const DEFAULT_CONNECTIONS: usize = 6;
const MAX_PARTS: usize = 1024;

/// The longest file name kept, in bytes (most file systems allow 255).
pub const MAX_NAME: usize = 200;
/// What a file is called when nothing better is known.
pub const FALLBACK: &str = "download";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
    pub pos: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Part {
    pub start: u64,
    pub end: u64,
    pub pos: u64,
    pub taken: bool,
}

impl Part {
    pub fn left(&self) -> u64 {
        self.end.saturating_sub(self.pos)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadSignal {
    Run,
    Pause,
    Cancel,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoQuality {
    pub label: String,
    pub selector: String,
    pub size: Option<u64>,
    pub needs_ffmpeg: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoInfo {
    pub title: String,
    pub file_name: String,
    pub qualities: Vec<VideoQuality>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Job {
    id: String,
    name: String,
    mime: String,
    size: i64,
    downloaded: u64,
    status: String,
    scan: String,
    error: String,
    created: u64,
    updated: u64,
    #[serde(default)]
    speed: u64,
    #[serde(default)]
    eta: i64,
    #[serde(default)]
    segments: Vec<Segment>,
    #[serde(skip_serializing_if = "String::is_empty")]
    url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    selector: String,
    #[serde(skip)]
    cancel: Option<watch::Sender<DownloadSignal>>,
}

#[derive(Clone)]
struct App {
    jobs: Arc<RwLock<HashMap<String, Arc<AsyncMutex<Job>>>>>,
    db: Arc<Mutex<Connection>>,
    files: PathBuf,
    scanner: Option<String>,
    permits: Arc<Semaphore>,
}

#[derive(Deserialize)]
struct UrlInput {
    url: String,
}

#[derive(Deserialize)]
struct CreateInput {
    url: String,
    name: Option<String>,
    selector: Option<String>,
}

#[derive(Serialize)]
struct ErrBody {
    error: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = PathBuf::from(env::var("DOWNLOAD_DIR").unwrap_or_else(|_| "data/files".into()));
    tokio::fs::create_dir_all(&files).await?;

    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "data/downloads.db".into());
    if let Some(p) = FsPath::new(&db_path).parent() {
        std::fs::create_dir_all(p)?;
    }
    let db = Connection::open(db_path)?;
    db.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
         CREATE TABLE IF NOT EXISTS downloads(
             id TEXT PRIMARY KEY,
             name TEXT NOT NULL,
             mime TEXT NOT NULL DEFAULT '',
             size INTEGER NOT NULL DEFAULT -1,
             downloaded INTEGER NOT NULL DEFAULT 0,
             status TEXT NOT NULL,
             scan TEXT NOT NULL DEFAULT '',
             error TEXT NOT NULL DEFAULT '',
             created INTEGER NOT NULL,
             updated INTEGER NOT NULL,
             url TEXT NOT NULL DEFAULT '',
             selector TEXT NOT NULL DEFAULT ''
         );",
    )?;
    ensure_scan_column(&db)?;
    ensure_url_column(&db)?;
    ensure_selector_column(&db)?;

    // Preserve in-flight downloads as paused on server restart so users can click Resume
    db.execute(
        "UPDATE downloads SET status='paused', error='Paused on server restart' WHERE status IN ('queued','downloading')",
        [],
    )?;
    db.execute(
        "UPDATE downloads SET status='failed', error='Server restarted during security scan' WHERE status='scanning'",
        [],
    )?;

    let state = App {
        jobs: Arc::new(RwLock::new(HashMap::new())),
        db: Arc::new(Mutex::new(db)),
        files,
        scanner: env::var("CLAMD_ADDRESS").ok().filter(|s| !s.is_empty()),
        permits: Arc::new(Semaphore::new(3)),
    };

    let ytdlp_available = find_tool("yt-dlp").is_some();

    let api = Router::new()
        .route(
            "/health",
            get(move || async move {
                Json(serde_json::json!({
                    "status": "ok",
                    "service": "Downloadit Rust API",
                    "features": ["multi-connection", "chunked-downloads", "pause-resume", "disk-space-check", "robust-names", "clamav-scan", "video-extraction"],
                    "ytdlp_available": ytdlp_available
                }))
            }),
        )
        .route("/analyze", post(analyze))
        .route("/downloads", get(list).post(create))
        .route("/downloads/{id}", get(get_job).delete(cancel))
        .route("/downloads/{id}/pause", post(pause))
        .route("/downloads/{id}/resume", post(resume))
        .route("/downloads/{id}/events", get(events))
        .route("/downloads/{id}/file", get(file))
        .with_state(state.clone());

    let app = Router::new()
        .nest("/api/v1", api)
        .fallback_service(ServeDir::new("web").append_index_html_on_directories(true))
        .layer(axum::middleware::map_response(security_headers));

    let addr = env::var("ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("Downloadit Rust server listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn security_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("X-Frame-Options", HeaderValue::from_static("DENY"));
    headers.insert(
        "Referrer-Policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        "Permissions-Policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}

fn ensure_scan_column(db: &Connection) -> rusqlite::Result<()> {
    let has: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('downloads') WHERE name='scan')",
        [],
        |r| r.get(0),
    )?;
    if !has {
        db.execute(
            "ALTER TABLE downloads ADD COLUMN scan TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    Ok(())
}

fn ensure_url_column(db: &Connection) -> rusqlite::Result<()> {
    let has: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('downloads') WHERE name='url')",
        [],
        |r| r.get(0),
    )?;
    if !has {
        db.execute(
            "ALTER TABLE downloads ADD COLUMN url TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    Ok(())
}

fn ensure_selector_column(db: &Connection) -> rusqlite::Result<()> {
    let has: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('downloads') WHERE name='selector')",
        [],
        |r| r.get(0),
    )?;
    if !has {
        db.execute(
            "ALTER TABLE downloads ADD COLUMN selector TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    Ok(())
}

fn err(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrBody {
            error: message.into(),
        }),
    )
        .into_response()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn safe_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let o = v.octets();
            !(v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_broadcast()
                || v.is_unspecified()
                || o[0] == 0
                || o[0] >= 224
                || (o[0] == 100 && (64..=127).contains(&o[1]))
                || (o[0] == 192 && (o[1] == 0 || o[1] == 2))
                || (o[0] == 198 && (o[1] == 18 || o[1] == 19 || (o[1] == 51 && o[2] == 100)))
                || (o[0] == 203 && o[1] == 0 && o[2] == 113))
        }
        IpAddr::V6(v) => {
            let s = v.segments();
            !(v.is_loopback()
                || v.is_unspecified()
                || v.is_unique_local()
                || v.is_unicast_link_local()
                || (s[0] & 0xfe00) != 0x2000
                || (s[0] == 0x2001 && s[1] == 0x0db8)
                || v.to_ipv4_mapped()
                    .is_some_and(|v4| !safe_ip(IpAddr::V4(v4))))
        }
    }
}

async fn validate(raw: &str) -> Result<(Url, Vec<SocketAddr>), String> {
    if raw.len() > 8192 {
        return Err("URL is too long".into());
    }
    let u = Url::parse(raw).map_err(|_| "Enter a valid URL")?;
    if !matches!(u.scheme(), "http" | "https")
        || u.host_str().is_none()
        || !u.username().is_empty()
        || u.password().is_some()
    {
        return Err("Use a public HTTP or HTTPS URL without embedded credentials".into());
    }
    if u.port().is_some_and(|p| p != 80 && p != 443) {
        return Err("Only standard HTTP and HTTPS ports are allowed".into());
    }
    let host = u
        .host_str()
        .unwrap()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".internal")
        || host == "metadata.google.internal"
    {
        return Err("Local network hosts are blocked".into());
    }
    let port = u.port_or_known_default().ok_or("Invalid port")?;
    let ips = match u.host().ok_or("Missing host")? {
        url::Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
        url::Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
        url::Host::Domain(d) => tokio::net::lookup_host((d, port))
            .await
            .map_err(|_| "Could not resolve host")?
            .map(|s| s.ip())
            .collect(),
    };
    if ips.is_empty() || ips.iter().any(|ip| !safe_ip(*ip)) {
        return Err("Host resolves to a private or reserved address".into());
    }
    Ok((
        u,
        ips.into_iter()
            .map(|ip| SocketAddr::new(ip, port))
            .collect(),
    ))
}

async fn get_response(
    mut url: Url,
    mut ips: Vec<SocketAddr>,
    method: reqwest::Method,
    range_header: Option<String>,
) -> Result<(reqwest::Response, Url, Vec<SocketAddr>), String> {
    for hop in 0..=8 {
        let host = url.host_str().ok_or("Missing host")?.to_string();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(12))
            .pool_max_idle_per_host(8)
            .read_timeout(Duration::from_secs(45))
            .tcp_keepalive(Duration::from_secs(30))
            .resolve_to_addrs(&host, &ips)
            .build()
            .map_err(|_| "Network client error")?;
        let mut request = client
            .request(method.clone(), url.clone())
            .header("User-Agent", "Downloadit-Rust/0.2")
            .header("Accept-Encoding", "identity");
        if let Some(r) = &range_header {
            request = request.header(header::RANGE, r);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("Source request failed: {e}"))?;
        if !response.status().is_redirection() {
            return Ok((response, url, ips));
        }
        if hop == 8 {
            return Err("Too many redirects".into());
        }
        let loc = response
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or("Invalid redirect")?;
        url = url.join(loc).map_err(|_| "Invalid redirect URL")?;
        let checked = validate(url.as_str()).await?;
        url = checked.0;
        ips = checked.1;
    }
    Err("Too many redirects".into())
}

// ---------------------------------------------------------------------------
// Tool discovery & Video extraction (adapted from downloadcraft/crates/net/src/video.rs)
// ---------------------------------------------------------------------------

pub fn find_tool(name: &str) -> Option<PathBuf> {
    let exe = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    if let Ok(dir) = env::var("DOWNLOADIT_TOOLS").or_else(|_| env::var("DOWNLOADCRAFT_TOOLS")) {
        let p = PathBuf::from(dir).join(&exe);
        if p.is_file() {
            return Some(p);
        }
    }
    let local = PathBuf::from("tools").join(&exe);
    if local.is_file() {
        return Some(local);
    }
    if let Some(appdata) = env::var_os("APPDATA") {
        let p = PathBuf::from(appdata).join("DownloadCraft").join("tools").join(&exe);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Some(path_var) = env::var_os("PATH") {
        for p in env::split_paths(&path_var) {
            let candidate = p.join(&exe);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

pub fn is_video_host(url: &Url) -> bool {
    let host = url.host_str().unwrap_or("").to_lowercase();
    host.contains("youtube.com")
        || host.contains("youtu.be")
        || host.contains("vimeo.com")
        || host.contains("twitch.tv")
        || host.contains("tiktok.com")
        || host.contains("dailymotion.com")
        || host.contains("twitter.com")
        || host.contains("x.com")
}

pub async fn probe_video(ytdlp: &FsPath, raw_url: &str) -> Result<VideoInfo, String> {
    let mut cmd = tokio::process::Command::new(ytdlp);
    cmd.args([
        "-J",
        "--no-playlist",
        "--flat-playlist",
        "--no-warnings",
        "--skip-download",
        "--",
        raw_url,
    ]);
    #[cfg(windows)]
    {
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let output = tokio::time::timeout(Duration::from_secs(45), cmd.output())
        .await
        .map_err(|_| "yt-dlp took too long to read the video page")?
        .map_err(|e| format!("Failed to run yt-dlp: {e}"))?;

    if !output.status.success() {
        let err_str = String::from_utf8_lossy(&output.stderr);
        let msg = err_str
            .lines()
            .rev()
            .find(|l| l.starts_with("ERROR:"))
            .map(|l| l.trim_start_matches("ERROR:").trim())
            .unwrap_or("yt-dlp could not read the video page");
        return Err(msg.to_string());
    }

    let val: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|_| "Could not parse yt-dlp JSON output")?;

    let title = val
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("video")
        .chars()
        .take(180)
        .collect::<String>();

    let file_name = sanitize_name(&format!("{title}.mp4"));
    let mut qualities = Vec::new();

    let formats = val
        .get("formats")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    let audio_size = formats
        .iter()
        .filter(|f| {
            f.get("acodec")
                .and_then(|v| v.as_str())
                .is_some_and(|c| c != "none")
                && f.get("vcodec")
                    .and_then(|v| v.as_str())
                    .map_or(true, |c| c == "none")
        })
        .filter_map(|f| {
            f.get("filesize")
                .and_then(|v| v.as_u64())
                .or_else(|| f.get("filesize_approx").and_then(|v| v.as_u64()))
        })
        .max();

    let mut heights: Vec<(u64, u64)> = Vec::new();
    for f in formats {
        let Some(h) = f
            .get("height")
            .and_then(|v| v.as_u64())
            .filter(|h| (100..=8640).contains(h))
        else {
            continue;
        };
        if f.get("vcodec")
            .and_then(|v| v.as_str())
            .is_none_or(|c| c == "none")
        {
            continue;
        }
        let fps = f.get("fps").and_then(|v| v.as_u64()).unwrap_or(0);
        if let Some(entry) = heights.iter_mut().find(|(eh, _)| *eh == h) {
            entry.1 = entry.1.max(fps);
        } else {
            heights.push((h, fps));
        }
    }
    heights.sort_by(|a, b| b.cmp(a));

    for (h, fps) in heights.into_iter().take(8) {
        let at: Vec<&serde_json::Value> = formats
            .iter()
            .filter(|f| {
                f.get("height").and_then(|v| v.as_u64()) == Some(h)
                    && f.get("vcodec")
                        .and_then(|v| v.as_str())
                        .is_some_and(|c| c != "none")
            })
            .collect();
        let separate = at.iter().all(|f| {
            f.get("acodec")
                .and_then(|v| v.as_str())
                .is_none_or(|c| c == "none")
        });
        let video_size = at.iter().filter_map(|f| {
            f.get("filesize")
                .and_then(|v| v.as_u64())
                .or_else(|| f.get("filesize_approx").and_then(|v| v.as_u64()))
        }).max();
        let size = match (video_size, separate) {
            (Some(v), true) => Some(v.saturating_add(audio_size.unwrap_or(0))),
            (v, _) => v,
        };
        let label = if fps > 30 {
            format!("{h}p{fps}")
        } else {
            format!("{h}p")
        };
        let selector = format!("bv*[height<={h}]+ba/b[height<={h}]");
        qualities.push(VideoQuality {
            label,
            selector,
            size,
            needs_ffmpeg: separate,
        });
    }

    if audio_size.is_some()
        || formats.iter().any(|f| {
            f.get("acodec")
                .and_then(|v| v.as_str())
                .is_some_and(|c| c != "none")
        })
    {
        qualities.push(VideoQuality {
            label: "Audio only".into(),
            selector: "ba/b".into(),
            size: audio_size,
            needs_ffmpeg: false,
        });
    }

    if qualities.is_empty() {
        qualities.push(VideoQuality {
            label: "Best available".into(),
            selector: "bv*+ba/b".into(),
            size: None,
            needs_ffmpeg: false,
        });
    }

    Ok(VideoInfo {
        title,
        file_name,
        qualities,
    })
}

// ---------------------------------------------------------------------------
// Disk space validation (adapted from downloadcraft/crates/net/src/http.rs)
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn available_space(path: &FsPath) -> std::io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    wide.push(0);
    let mut free_bytes_available: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_free: u64 = 0;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }
    let res = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_bytes_available,
            &mut total_bytes,
            &mut total_free,
        )
    };
    if res != 0 {
        Ok(free_bytes_available)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn available_space(path: &FsPath) -> std::io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    #[repr(C)]
    struct StatVfs {
        f_bsize: std::ffi::c_ulong,
        f_frsize: std::ffi::c_ulong,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        f_files: u64,
        f_ffree: u64,
        f_favail: u64,
        f_fsid: std::ffi::c_ulong,
        f_flag: std::ffi::c_ulong,
        f_namemax: std::ffi::c_ulong,
        _spare: [std::ffi::c_int; 6],
    }
    extern "C" {
        fn statvfs(path: *const std::ffi::c_char, buf: *mut StatVfs) -> std::ffi::c_int;
    }
    let mut stat: StatVfs = unsafe { std::mem::zeroed() };
    let res = unsafe { statvfs(c_path.as_ptr(), &mut stat) };
    if res == 0 {
        let frsize = if stat.f_frsize > 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };
        Ok(stat.f_bavail.saturating_mul(frsize))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(windows, unix)))]
fn available_space(_path: &FsPath) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "unsupported platform",
    ))
}

pub fn check_room(folder: &FsPath, need: u64) -> Result<(), String> {
    match available_space(folder) {
        Ok(free) => {
            if need > free {
                Err(format!(
                    "Not enough room on disk: {} needed, {} free",
                    format_size(need as i64),
                    format_size(free as i64)
                ))
            } else {
                Ok(())
            }
        }
        Err(_) => {
            // A disk whose available space cannot be determined is let through.
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Robust filename sanitization (adapted from downloadcraft/crates/model/src/names.rs)
// ---------------------------------------------------------------------------

pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while let Some(&c) = b.get(i) {
        let hex = |j: usize| b.get(j).and_then(|h| (*h as char).to_digit(16));
        match (c, hex(i + 1), hex(i + 2)) {
            (b'%', Some(hi), Some(lo)) => {
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn cap_name(name: &str) -> String {
    if name.len() <= MAX_NAME {
        return name.to_string();
    }
    let ext = name
        .rsplit_once('.')
        .map(|(_, e)| e)
        .filter(|e| e.len() <= 16 && !e.is_empty())
        .unwrap_or("");
    let room = MAX_NAME.saturating_sub(if ext.is_empty() { 0 } else { ext.len() + 1 });
    let mut end = room.min(name.len());
    while end > 0 && !name.is_char_boundary(end) {
        end -= 1;
    }
    let stem = name
        .get(..end)
        .unwrap_or("")
        .trim_end_matches(|c: char| c == '.' || c.is_whitespace());
    if ext.is_empty() {
        stem.to_string()
    } else {
        format!("{stem}.{ext}")
    }
}

pub fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c: char| c == '.' || c.is_whitespace());
    let mut out = cap_name(trimmed);
    if out.is_empty() || out.chars().all(|c| c == '_') {
        return FALLBACK.to_string();
    }
    let stem = out
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end()
        .to_ascii_uppercase();
    const RESERVED: &[&str] = &["CON", "PRN", "AUX", "NUL"];
    let numbered = (stem.starts_with("COM") || stem.starts_with("LPT"))
        && stem.len() == 4
        && stem.as_bytes().get(3).is_some_and(u8::is_ascii_digit);
    if RESERVED.contains(&stem.as_str()) || numbered {
        out = cap_name(&format!("_{out}"));
    }
    out
}

pub fn from_url(url: &Url) -> String {
    if let Some(mut segs) = url.path_segments() {
        if let Some(last) = segs.next_back().filter(|s| !s.is_empty()) {
            let decoded = percent_decode(last);
            let sanitized = sanitize_name(&decoded);
            if sanitized != FALLBACK {
                return sanitized;
            }
        }
    }
    FALLBACK.to_string()
}

pub fn from_disposition(header: &str) -> Option<String> {
    let mut plain = None;
    for part in header.split(';').map(str::trim) {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        if key == "filename*" {
            // charset'language'percent-encoded
            let mut bits = value.splitn(3, '\'');
            let charset = bits.next().unwrap_or("").to_ascii_lowercase();
            let encoded = bits.nth(1).unwrap_or("");
            if charset == "utf-8" || charset == "us-ascii" {
                let name = sanitize_name(&percent_decode(encoded));
                if name != FALLBACK {
                    return Some(name);
                }
            }
        } else if key == "filename" && plain.is_none() {
            let v = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            plain = Some(sanitize_name(&v.replace("\\\"", "\"")));
        }
    }
    plain.filter(|n| n != FALLBACK)
}

fn filename_from(headers: &reqwest::header::HeaderMap, url: &Url, mime: &str) -> String {
    if let Some(v) = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(n) = from_disposition(v) {
            return n;
        }
    }
    let from_path = from_url(url);
    if from_path != FALLBACK {
        return from_path;
    }
    format!(
        "download{}",
        mime_guess::get_mime_extensions_str(mime)
            .and_then(|a| a.first())
            .map(|s| format!(".{s}"))
            .unwrap_or_default()
    )
}

fn format_size(n: i64) -> String {
    if n < 0 {
        return "Unknown size".into();
    }
    let mut f = n as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut i = 0;
    while f >= 1024. && i < units.len() - 1 {
        f /= 1024.;
        i += 1;
    }
    format!("{f:.1} {}", units[i])
}

fn record_path(part: &FsPath) -> PathBuf {
    let mut name = part.as_os_str().to_owned();
    name.push(".parts");
    PathBuf::from(name)
}

fn split_parts(total: u64, connections: usize) -> Vec<Part> {
    let n = (connections as u64).min(total.div_ceil(MIN_PART)).max(1);
    let size = total.div_ceil(n);
    (0..n)
        .map(|i| {
            let start = i.saturating_mul(size).min(total);
            let end = start.saturating_add(size).min(total);
            Part {
                start,
                end,
                pos: start,
                taken: false,
            }
        })
        .filter(|p| p.end > p.start)
        .collect()
}

fn save_parts_record(part: &FsPath, total: u64, parts: &[Part]) -> std::io::Result<()> {
    let list: Vec<serde_json::Value> = parts
        .iter()
        .map(|p| serde_json::json!([p.start, p.end, p.pos]))
        .collect();
    let path = record_path(part);
    let tmp = path.with_extension("parts.tmp");
    std::fs::write(&tmp, serde_json::json!({"total": total, "parts": list}).to_string())?;
    std::fs::rename(&tmp, &path)
}

fn load_parts_record(part: &FsPath, expected_total: Option<u64>) -> Option<(u64, Vec<Part>)> {
    let text = std::fs::read_to_string(record_path(part)).ok()?;
    if text.len() > 256 * 1024 {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let total = v.get("total")?.as_u64()?;
    if let Some(exp) = expected_total {
        if exp != total {
            return None;
        }
    }
    let list = v.get("parts")?.as_array()?;
    if list.is_empty() || list.len() > MAX_PARTS {
        return None;
    }
    let mut parts = Vec::with_capacity(list.len());
    for item in list {
        let n = |i: usize| item.get(i).and_then(|x| x.as_u64());
        let (start, end, pos) = (n(0)?, n(1)?, n(2)?);
        if !(start <= pos && pos <= end && end <= total) {
            return None;
        }
        parts.push(Part {
            start,
            end,
            pos,
            taken: false,
        });
    }
    parts.sort_by_key(|p| p.start);
    let mut at = 0;
    for p in &parts {
        if p.start != at {
            return None;
        }
        at = p.end;
    }
    if at != total {
        return None;
    }
    Some((total, parts))
}

async fn analyze(Json(input): Json<UrlInput>) -> Response {
    let Ok((url, ips)) = validate(&input.url).await else {
        return err(StatusCode::BAD_REQUEST, "Invalid or unsafe URL");
    };

    // If it's a known video host or yt-dlp is available, attempt video extraction
    if is_video_host(&url) {
        if let Some(ytdlp) = find_tool("yt-dlp") {
            match probe_video(&ytdlp, url.as_str()).await {
                Ok(video) => {
                    let first_size = video.qualities.first().and_then(|q| q.size).map(|s| s as i64).unwrap_or(-1);
                    return Json(serde_json::json!({
                        "kind": "video",
                        "url": input.url,
                        "title": video.title,
                        "name": video.file_name,
                        "mime": "video/mp4",
                        "size": first_size,
                        "size_label": format_size(first_size),
                        "range_supported": true,
                        "connections": 4,
                        "qualities": video.qualities
                    })).into_response();
                }
                Err(e) => {
                    return err(StatusCode::BAD_REQUEST, format!("Video extraction failed: {e}"));
                }
            }
        } else {
            return err(
                StatusCode::BAD_REQUEST,
                "This is a video page. Please install yt-dlp on the server or host system to download videos.",
            );
        }
    }

    let (response, final_url, _) =
        match get_response(url.clone(), ips, reqwest::Method::HEAD, None).await {
            Ok((r, u, _i))
                if [
                    StatusCode::METHOD_NOT_ALLOWED,
                    StatusCode::NOT_IMPLEMENTED,
                    StatusCode::FORBIDDEN,
                ]
                .contains(&r.status()) =>
            {
                drop(r);
                let Ok((u, ips)) = validate(u.as_str()).await else {
                    return err(StatusCode::BAD_REQUEST, "Unsafe redirect");
                };
                match get_response(u, ips, reqwest::Method::GET, Some("bytes=0-0".into())).await {
                    Ok(v) => v,
                    Err(e) => return err(StatusCode::BAD_REQUEST, e),
                }
            }
            Ok(v) => v,
            Err(e) => return err(StatusCode::BAD_REQUEST, e),
        };
    if !response.status().is_success() {
        return err(
            StatusCode::BAD_REQUEST,
            format!("Source returned {}", response.status()),
        );
    }
    let headers = response.headers().clone();
    let mut size = response.content_length().map(|x| x as i64).unwrap_or(-1);
    if let Some(n) = headers
        .get(header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit('/').next())
        .and_then(|v| v.parse::<i64>().ok())
    {
        size = n;
    }
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    // Check if the HTML response is actually an unblocked video page
    if mime.to_lowercase().contains("text/html") {
        if let Some(ytdlp) = find_tool("yt-dlp") {
            if let Ok(video) = probe_video(&ytdlp, final_url.as_str()).await {
                let first_size = video.qualities.first().and_then(|q| q.size).map(|s| s as i64).unwrap_or(-1);
                return Json(serde_json::json!({
                    "kind": "video",
                    "url": input.url,
                    "title": video.title,
                    "name": video.file_name,
                    "mime": "video/mp4",
                    "size": first_size,
                    "size_label": format_size(first_size),
                    "range_supported": true,
                    "connections": 4,
                    "qualities": video.qualities
                })).into_response();
            }
        }
    }

    let name = filename_from(&headers, &final_url, mime);
    let range_supported = headers
        .get(header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_lowercase().contains("bytes"))
        || headers.get(header::CONTENT_RANGE).is_some();

    let result = serde_json::json!({
        "kind": "file",
        "url": input.url,
        "name": name,
        "mime": mime,
        "size": size,
        "size_label": format_size(size),
        "range_supported": range_supported,
        "connections": if range_supported && size >= (MIN_PART as i64) { DEFAULT_CONNECTIONS } else { 1 }
    });
    Json(result).into_response()
}

async fn create(State(app): State<App>, Json(input): Json<CreateInput>) -> Response {
    let Ok((url, ips)) = validate(&input.url).await else {
        return err(StatusCode::BAD_REQUEST, "Invalid or unsafe URL");
    };
    let permit = match app.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return err(StatusCode::TOO_MANY_REQUESTS, "Download queue is full"),
    };
    let id = Uuid::new_v4().to_string();
    let name = input
        .name
        .map(|s| sanitize_name(&s))
        .filter(|s| s != FALLBACK)
        .unwrap_or_else(|| from_url(&url));

    let selector = input.selector.unwrap_or_default();
    let (tx, rx) = watch::channel(DownloadSignal::Run);
    let job = Job {
        id: id.clone(),
        name,
        mime: String::new(),
        size: -1,
        downloaded: 0,
        status: "queued".into(),
        scan: String::new(),
        error: String::new(),
        created: now(),
        updated: now(),
        speed: 0,
        eta: -1,
        segments: Vec::new(),
        url: url.to_string(),
        selector,
        cancel: Some(tx),
    };
    save(&app, &job);
    app.jobs
        .write()
        .await
        .insert(id.clone(), Arc::new(AsyncMutex::new(job.clone())));
    tokio::spawn(run(app.clone(), id, url, ips, rx, permit, false));
    (StatusCode::ACCEPTED, Json(job)).into_response()
}

async fn pause(State(app): State<App>, Path(id): Path<String>) -> Response {
    let Some(j) = job_ref(&app, &id).await else {
        return err(StatusCode::NOT_FOUND, "Not found");
    };
    let mut j = j.lock().await;
    if ["queued", "downloading"].contains(&j.status.as_str()) {
        if let Some(c) = &j.cancel {
            let _ = c.send(DownloadSignal::Pause);
        }
        j.status = "paused".into();
        j.error = String::new();
        j.speed = 0;
        j.eta = -1;
        j.updated = now();
        save(&app, &j);
    }
    Json(j.clone()).into_response()
}

async fn resume(State(app): State<App>, Path(id): Path<String>) -> Response {
    let job_arc = match job_ref(&app, &id).await {
        Some(j) => j,
        None => {
            let row = {
                let Ok(db) = app.db.lock() else {
                    return err(StatusCode::NOT_FOUND, "Not found");
                };
                db.query_row(
                    "SELECT id,name,mime,size,downloaded,status,scan,error,created,updated,url,selector FROM downloads WHERE id=?1",
                    [&id],
                    |r| {
                        Ok(Job {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            mime: r.get(2)?,
                            size: r.get(3)?,
                            downloaded: r.get::<_, i64>(4)? as u64,
                            status: r.get(5)?,
                            scan: r.get(6)?,
                            error: r.get(7)?,
                            created: r.get::<_, i64>(8)? as u64,
                            updated: r.get::<_, i64>(9)? as u64,
                            url: r.get(10)?,
                            selector: r.get(11)?,
                            speed: 0,
                            eta: -1,
                            segments: Vec::new(),
                            cancel: None,
                        })
                    },
                )
                .optional()
            };
            match row {
                Ok(Some(j)) => {
                    let arc = Arc::new(AsyncMutex::new(j));
                    app.jobs.write().await.insert(id.clone(), arc.clone());
                    arc
                }
                _ => return err(StatusCode::NOT_FOUND, "Not found"),
            }
        }
    };

    let (url_str, current_status) = {
        let j = job_arc.lock().await;
        (j.url.clone(), j.status.clone())
    };

    if current_status == "downloading" {
        let snapshot = job_arc.lock().await.clone();
        return Json(snapshot).into_response();
    }
    if !["paused", "failed"].contains(&current_status.as_str()) {
        return err(
            StatusCode::BAD_REQUEST,
            "Only paused or failed downloads can be resumed",
        );
    }
    if url_str.is_empty() {
        return err(StatusCode::BAD_REQUEST, "Download URL missing; cannot resume");
    }

    let Ok((url, ips)) = validate(&url_str).await else {
        return err(StatusCode::BAD_REQUEST, "Invalid or unsafe URL");
    };

    let permit = match app.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return err(StatusCode::TOO_MANY_REQUESTS, "Download queue is full"),
    };

    let (tx, rx) = watch::channel(DownloadSignal::Run);
    {
        let mut j = job_arc.lock().await;
        j.status = "queued".into();
        j.error = String::new();
        j.speed = 0;
        j.eta = -1;
        j.cancel = Some(tx);
        j.updated = now();
        save(&app, &j);
    }

    tokio::spawn(run(app.clone(), id, url, ips, rx, permit, true));
    let snapshot = job_arc.lock().await.clone();
    (StatusCode::ACCEPTED, Json(snapshot)).into_response()
}

struct MultiConnectionShared {
    parts: Mutex<Vec<Part>>,
    done: AtomicU64,
    stopped: AtomicBool,
    paused: AtomicBool,
    cancelled: AtomicBool,
    failed_error: Mutex<Option<String>>,
    active_workers: AtomicUsize,
}

impl MultiConnectionShared {
    fn claim(&self) -> Option<usize> {
        let mut parts = self.parts.lock().unwrap();
        if let Some(i) = parts.iter().position(|p| !p.taken && p.left() > 0) {
            if let Some(p) = parts.get_mut(i) {
                p.taken = true;
            }
            return Some(i);
        }
        // Work-stealing: split the taken part with the largest remaining bytes
        let (i, biggest) = parts
            .iter()
            .enumerate()
            .filter(|(_, p)| p.taken)
            .max_by_key(|(_, p)| p.left())
            .map(|(i, p)| (i, *p))?;

        if biggest.left() < 2 * MIN_PART {
            return None;
        }
        let mid = biggest.pos + biggest.left() / 2;
        if let Some(p) = parts.get_mut(i) {
            p.end = mid;
        }
        parts.push(Part {
            start: mid,
            end: biggest.end,
            pos: mid,
            taken: true,
        });
        Some(parts.len() - 1)
    }

    fn snapshot_segments(&self) -> Vec<Segment> {
        let mut parts = self.parts.lock().unwrap().clone();
        parts.sort_by_key(|p| p.start);
        parts
            .into_iter()
            .map(|p| Segment {
                start: p.start,
                end: p.end,
                pos: p.pos,
            })
            .collect()
    }
}

async fn run(
    app: App,
    id: String,
    url: Url,
    ips: Vec<SocketAddr>,
    mut cancel_rx: watch::Receiver<DownloadSignal>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    is_resume: bool,
) {
    set_status(&app, &id, "downloading", "").await;
    let part_path = app.files.join(format!("{id}.part"));
    let dest_path = app.files.join(format!("{id}.bin"));

    let selector = {
        if let Some(j) = job_ref(&app, &id).await {
            j.lock().await.selector.clone()
        } else {
            String::new()
        }
    };

    let result: Result<Option<()>, String> = async {
        // If this job specifies a video selector or host is a video page and yt-dlp is available, download via yt-dlp
        let is_video = !selector.is_empty() || is_video_host(&url);
        if is_video {
            if let Some(ytdlp) = find_tool("yt-dlp") {
                return run_ytdlp_download(&app, &id, &ytdlp, &url.to_string(), &selector, &part_path, &dest_path, &mut cancel_rx).await;
            } else if !selector.is_empty() {
                return Err("yt-dlp is required for video downloads but is not installed on the system.".into());
            }
        }

        // Direct HTTP Probe
        let (probe_resp, final_url, final_ips) =
            match get_response(url.clone(), ips.clone(), reqwest::Method::HEAD, None).await {
                Ok((r, u, i))
                    if [
                        StatusCode::METHOD_NOT_ALLOWED,
                        StatusCode::NOT_IMPLEMENTED,
                        StatusCode::FORBIDDEN,
                    ]
                    .contains(&r.status()) =>
                {
                    get_response(u, i, reqwest::Method::GET, Some("bytes=0-0".into())).await?
                }
                Ok(v) => v,
                Err(e) => return Err(e),
            };

        if !probe_resp.status().is_success() {
            return Err(format!("Source returned {}", probe_resp.status()));
        }

        let headers = probe_resp.headers().clone();
        let mut total_size = probe_resp.content_length().map(|n| n as i64).unwrap_or(-1);
        if let Some(n) = headers
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|v| v.parse::<i64>().ok())
        {
            total_size = n;
        }

        if total_size > (MAX_BYTES as i64) {
            return Err("File exceeds the 20 GiB limit".into());
        }

        // Check available disk space before starting or allocating
        if total_size > 0 {
            check_room(&app.files, total_size as u64)?;
        }

        let range_supported = headers
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_lowercase().contains("bytes"))
            || headers.get(header::CONTENT_RANGE).is_some();

        let mime = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        if let Some(j) = job_ref(&app, &id).await {
            let mut j = j.lock().await;
            j.mime = mime.clone();
            if j.name == "download" || j.name.is_empty() {
                j.name = filename_from(&headers, &final_url, &mime);
            }
            if total_size > 0 {
                j.size = total_size;
            }
        }

        let mut existing_parts = None;
        if is_resume && part_path.exists() {
            if let Some((rec_total, parts)) = load_parts_record(
                &part_path,
                if total_size > 0 {
                    Some(total_size as u64)
                } else {
                    None
                },
            ) {
                total_size = rec_total as i64;
                existing_parts = Some(parts);
            }
        }

        let can_multiconnect = range_supported && total_size >= (MIN_PART as i64);

        if can_multiconnect {
            let total = total_size as u64;
            let parts = existing_parts.unwrap_or_else(|| split_parts(total, DEFAULT_CONNECTIONS));

            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&part_path)
                .await
                .map_err(|e| format!("Could not open part file: {e}"))?;

            let current_file_len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
            if current_file_len != total {
                file.set_len(total)
                    .await
                    .map_err(|e| format!("Could not allocate file size: {e}"))?;
            }

            let initial_done: u64 = parts.iter().map(|p| p.pos.saturating_sub(p.start)).sum();
            let _ = save_parts_record(&part_path, total, &parts);

            let shared = Arc::new(MultiConnectionShared {
                parts: Mutex::new(parts),
                done: AtomicU64::new(initial_done),
                stopped: AtomicBool::new(false),
                paused: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                failed_error: Mutex::new(None),
                active_workers: AtomicUsize::new(0),
            });

            let file_mutex = Arc::new(AsyncMutex::new(file));
            let worker_count = DEFAULT_CONNECTIONS;

            // Spawn concurrent download workers
            for _ in 0..worker_count {
                let sh = shared.clone();
                let f_mut = file_mutex.clone();
                let w_url = final_url.clone();
                let w_ips = final_ips.clone();

                sh.active_workers.fetch_add(1, Ordering::SeqCst);

                tokio::spawn(async move {
                    loop {
                        if sh.stopped.load(Ordering::Relaxed) {
                            break;
                        }
                        let part_idx = match sh.claim() {
                            Some(i) => i,
                            None => break,
                        };

                        let (p_pos, p_end) = {
                            let parts = sh.parts.lock().unwrap();
                            let Some(p) = parts.get(part_idx) else { break };
                            (p.pos, p.end)
                        };

                        if p_pos >= p_end {
                            continue;
                        }

                        let range_hdr = format!("bytes={}-{}", p_pos, p_end - 1);
                        let client_res = reqwest::Client::builder()
                            .redirect(reqwest::redirect::Policy::none())
                            .connect_timeout(Duration::from_secs(15))
                            .read_timeout(Duration::from_secs(30))
                            .resolve_to_addrs(
                                w_url.host_str().unwrap_or(""),
                                &w_ips,
                            )
                            .build();

                        let client = match client_res {
                            Ok(c) => c,
                            Err(e) => {
                                *sh.failed_error.lock().unwrap() = Some(format!("Client error: {e}"));
                                break;
                            }
                        };

                        let resp = client
                            .get(w_url.clone())
                            .header(header::RANGE, range_hdr)
                            .header("User-Agent", "Downloadit-Rust/0.2")
                            .header("Accept-Encoding", "identity")
                            .send()
                            .await;

                        let mut resp = match resp {
                            Ok(r) if r.status() == StatusCode::PARTIAL_CONTENT => r,
                            Ok(r) => {
                                *sh.failed_error.lock().unwrap() =
                                    Some(format!("Server returned {} for range request", r.status()));
                                break;
                            }
                            Err(e) => {
                                *sh.failed_error.lock().unwrap() = Some(format!("Worker request failed: {e}"));
                                break;
                            }
                        };

                        let mut part_completed = false;
                        while let Some(chunk_res) = resp.chunk().await.transpose() {
                            if sh.stopped.load(Ordering::Relaxed) {
                                break;
                            }
                            let chunk = match chunk_res {
                                Ok(c) => c,
                                Err(e) => {
                                    *sh.failed_error.lock().unwrap() =
                                        Some(format!("Chunk stream error: {e}"));
                                    break;
                                }
                            };

                            let (at, take, last) = {
                                let mut parts = sh.parts.lock().unwrap();
                                let Some(p) = parts.get_mut(part_idx) else { break };
                                let take = (chunk.len() as u64).min(p.left());
                                let at = p.pos;
                                p.pos += take;
                                (at, take, p.pos >= p.end)
                            };

                            if take > 0 {
                                let mut f = f_mut.lock().await;
                                if f.seek(std::io::SeekFrom::Start(at)).await.is_err()
                                    || f.write_all(&chunk[..take as usize]).await.is_err()
                                {
                                    *sh.failed_error.lock().unwrap() =
                                        Some("Disk write error".into());
                                    sh.stopped.store(true, Ordering::Relaxed);
                                    break;
                                }
                                drop(f);
                                sh.done.fetch_add(take, Ordering::Relaxed);
                            }

                            if last {
                                let mut parts = sh.parts.lock().unwrap();
                                if let Some(p) = parts.get_mut(part_idx) {
                                    p.taken = false;
                                }
                                part_completed = true;
                                break;
                            }
                        }

                        if !part_completed && !sh.stopped.load(Ordering::Relaxed) {
                            let mut parts = sh.parts.lock().unwrap();
                            if let Some(p) = parts.get_mut(part_idx) {
                                p.taken = false;
                            }
                        }
                    }
                    sh.active_workers.fetch_sub(1, Ordering::SeqCst);
                });
            }

            // Monitor loop: updates progress, speed, ETA, saves parts record, checks cancellation/pause
            let mut last_tick = Instant::now();
            let mut last_bytes = initial_done;
            let mut last_save = Instant::now();

            while shared.active_workers.load(Ordering::SeqCst) > 0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                    changed = cancel_rx.changed() => {
                        if changed.is_ok() {
                            match *cancel_rx.borrow() {
                                DownloadSignal::Pause => {
                                    shared.paused.store(true, Ordering::Relaxed);
                                    shared.stopped.store(true, Ordering::Relaxed);
                                }
                                DownloadSignal::Cancel => {
                                    shared.cancelled.store(true, Ordering::Relaxed);
                                    shared.stopped.store(true, Ordering::Relaxed);
                                }
                                DownloadSignal::Run => {}
                            }
                        }
                    }
                }

                let current_done = shared.done.load(Ordering::Relaxed);
                let elapsed = last_tick.elapsed();
                if elapsed >= Duration::from_millis(500) {
                    let speed = ((current_done.saturating_sub(last_bytes)) as f64
                        / elapsed.as_secs_f64()) as u64;
                    let eta = if speed > 0 && total > current_done {
                        ((total - current_done) / speed) as i64
                    } else {
                        -1
                    };
                    let segs = shared.snapshot_segments();
                    if let Some(j) = job_ref(&app, &id).await {
                        let mut j = j.lock().await;
                        j.downloaded = current_done;
                        j.speed = speed;
                        j.eta = eta;
                        j.segments = segs;
                        j.updated = now();
                    }
                    last_tick = Instant::now();
                    last_bytes = current_done;
                }

                if last_save.elapsed() >= Duration::from_secs(1) {
                    let parts_snap = shared.parts.lock().unwrap().clone();
                    let _ = save_parts_record(&part_path, total, &parts_snap);
                    if let Some(j) = job_ref(&app, &id).await {
                        save(&app, &*j.lock().await);
                    }
                    last_save = Instant::now();
                }
            }

            // Sync file
            let mut f = file_mutex.lock().await;
            let _ = f.flush().await;
            let _ = f.sync_all().await;
            drop(f);

            let parts_final = shared.parts.lock().unwrap().clone();
            let _ = save_parts_record(&part_path, total, &parts_final);

            if shared.cancelled.load(Ordering::Relaxed) {
                return Ok(None);
            }
            if shared.paused.load(Ordering::Relaxed) {
                return Ok(Some(()));
            }

            let all_finished = parts_final.iter().all(|p| p.pos >= p.end);
            if !all_finished {
                let err = shared
                    .failed_error
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "Download interrupted".into());
                return Err(err);
            }

            let _ = std::fs::remove_file(record_path(&part_path));
        } else {
            // Single-stream fallback (for servers without range support or unknown size)
            let mut start_byte = 0u64;
            let mut open_opts = tokio::fs::OpenOptions::new();
            open_opts.create(true).write(true);

            if is_resume && range_supported && part_path.exists() {
                if let Ok(meta) = tokio::fs::metadata(&part_path).await {
                    start_byte = meta.len();
                    open_opts.append(true);
                } else {
                    open_opts.truncate(true);
                }
            } else {
                open_opts.truncate(true);
            }

            let file = open_opts
                .open(&part_path)
                .await
                .map_err(|e| format!("Could not open file: {e}"))?;
            let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);

            let range_hdr = if start_byte > 0 {
                Some(format!("bytes={start_byte}-"))
            } else {
                None
            };

            let (mut response, _, _) =
                get_response(final_url, final_ips, reqwest::Method::GET, range_hdr).await?;

            if !response.status().is_success() {
                return Err(format!("Source returned {}", response.status()));
            }

            let mut total_downloaded = start_byte;
            let mut last_tick = Instant::now();
            let mut last_bytes = total_downloaded;

            loop {
                tokio::select! {
                    changed = cancel_rx.changed() => {
                        if changed.is_ok() {
                            let sig = *cancel_rx.borrow();
                            match sig {
                                DownloadSignal::Pause => {
                                    let _ = writer.flush().await;
                                    let _ = writer.get_ref().sync_all().await;
                                    return Ok(Some(()));
                                }
                                DownloadSignal::Cancel => {
                                    return Ok(None);
                                }
                                DownloadSignal::Run => {}
                            }
                        }
                    }
                    chunk = response.chunk() => match chunk {
                        Ok(Some(bytes)) => {
                            total_downloaded += bytes.len() as u64;
                            if total_downloaded > MAX_BYTES {
                                return Err("File exceeds the 20 GiB limit".into());
                            }
                            writer.write_all(&bytes).await.map_err(|_| "Disk write failed")?;

                            let elapsed = last_tick.elapsed();
                            if elapsed >= Duration::from_millis(500) {
                                let speed = ((total_downloaded.saturating_sub(last_bytes)) as f64
                                    / elapsed.as_secs_f64()) as u64;
                                let eta = if speed > 0 && total_size > (total_downloaded as i64) {
                                    ((total_size as u64 - total_downloaded) / speed) as i64
                                } else {
                                    -1
                                };
                                if let Some(j) = job_ref(&app, &id).await {
                                    let mut j = j.lock().await;
                                    j.downloaded = total_downloaded;
                                    j.speed = speed;
                                    j.eta = eta;
                                    j.updated = now();
                                    save(&app, &j);
                                }
                                last_tick = Instant::now();
                                last_bytes = total_downloaded;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => return Err(format!("Transfer failed: {e}")),
                    }
                }
            }

            writer.flush().await.map_err(|_| "Disk flush failed")?;
            writer.get_ref().sync_all().await.map_err(|_| "Disk sync failed")?;
        }

        // Inspection and finalization
        let mut prefix = vec![0u8; 512];
        if let Ok(mut f) = tokio::fs::File::open(&part_path).await {
            let n = f.read(&mut prefix).await.unwrap_or(0);
            prefix.truncate(n);
        }

        let snapshot = job_ref(&app, &id).await.unwrap().lock().await.clone();
        inspect(&snapshot.name, &snapshot.mime, &prefix)?;

        tokio::fs::rename(&part_path, &dest_path)
            .await
            .map_err(|_| "File finalize failed")?;

        if let Some(scanner) = &app.scanner {
            set_status(&app, &id, "scanning", "").await;
            if let Err(e) = scan(scanner, &dest_path).await {
                let _ = tokio::fs::remove_file(&dest_path).await;
                return Err(e);
            }
            if let Some(job) = job_ref(&app, &id).await {
                job.lock().await.scan = "clean".into();
            }
        } else if let Some(job) = job_ref(&app, &id).await {
            job.lock().await.scan = "not_scanned".into();
        }

        Ok(Some(()))
    }
    .await;

    match result {
        Ok(Some(())) => {
            if let Some(j) = job_ref(&app, &id).await {
                let mut j = j.lock().await;
                if j.status == "downloading" {
                    j.status = "completed".into();
                    j.speed = 0;
                    j.eta = 0;
                    if j.size <= 0 {
                        if let Ok(meta) = tokio::fs::metadata(&dest_path).await {
                            j.size = meta.len() as i64;
                            j.downloaded = meta.len();
                        }
                    }
                    j.updated = now();
                    save(&app, &j);
                }
            }
        }
        Ok(None) => {
            // Cancelled
            set_status(&app, &id, "cancelled", "").await;
            let _ = tokio::fs::remove_file(&part_path).await;
            let _ = tokio::fs::remove_file(record_path(&part_path)).await;
            let _ = tokio::fs::remove_file(&dest_path).await;
        }
        Err(e) => {
            set_status(&app, &id, "failed", &e).await;
        }
    }
}

async fn run_ytdlp_download(
    app: &App,
    id: &str,
    ytdlp: &FsPath,
    url: &str,
    selector: &str,
    part_path: &FsPath,
    dest_path: &FsPath,
    cancel_rx: &mut watch::Receiver<DownloadSignal>,
) -> Result<Option<()>, String> {
    let selector_arg = if selector.is_empty() { "bv*+ba/b" } else { selector };
    let mut cmd = tokio::process::Command::new(ytdlp);
    cmd.args([
        "-f", selector_arg,
        "-S", "res,vcodec:h264,acodec:m4a",
        "--no-playlist",
        "--newline",
        "--no-colors",
        "--windows-filenames",
        "--continue",
        "--no-mtime",
        "--merge-output-format", "mp4",
        "--concurrent-fragments", "4",
        "--progress-template", "download:DCPROGRESS %(progress.downloaded_bytes)s %(progress.total_bytes)s %(progress.total_bytes_estimate)s %(progress.speed)s",
        "--print", "after_move:DCFILE %(filepath)s",
        "-o", part_path.to_str().unwrap(),
    ]);

    if let Some(ffmpeg) = find_tool("ffmpeg") {
        if let Some(parent) = ffmpeg.parent() {
            cmd.arg("--ffmpeg-location").arg(parent);
        } else {
            cmd.arg("--ffmpeg-location").arg(&ffmpeg);
        }
    }

    cmd.args(["--", url]);

    #[cfg(windows)]
    {
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn().map_err(|e| format!("Could not start yt-dlp: {e}"))?;
    let stdout = child.stdout.take().ok_or("Could not capture yt-dlp stdout")?;
    let mut reader = BufReader::new(stdout).lines();

    let mut moved_file: Option<PathBuf> = None;
    let mut last_tick = Instant::now();

    loop {
        tokio::select! {
            changed = cancel_rx.changed() => {
                if changed.is_ok() {
                    let sig = *cancel_rx.borrow();
                    match sig {
                        DownloadSignal::Pause => {
                            let _ = child.kill().await;
                            return Ok(Some(()));
                        }
                        DownloadSignal::Cancel => {
                            let _ = child.kill().await;
                            return Ok(None);
                        }
                        DownloadSignal::Run => {}
                    }
                }
            }
            line_res = reader.next_line() => {
                match line_res {
                    Ok(Some(line)) => {
                        if let Some(rest) = line.strip_prefix("download:DCPROGRESS ") {
                            let parts: Vec<&str> = rest.split_whitespace().collect();
                            let parse_val = |idx: usize| parts.get(idx).and_then(|s| s.parse::<f64>().ok()).filter(|f| f.is_finite() && *f >= 0.0).map(|f| f as u64);
                            let done = parse_val(0).unwrap_or(0);
                            let total = parse_val(1).or_else(|| parse_val(2));
                            let speed = parse_val(3).unwrap_or(0);

                            if last_tick.elapsed() >= Duration::from_millis(500) {
                                if let Some(j) = job_ref(app, id).await {
                                    let mut j = j.lock().await;
                                    j.downloaded = done;
                                    if let Some(t) = total {
                                        j.size = t as i64;
                                    }
                                    j.speed = speed;
                                    if speed > 0 && total.unwrap_or(0) > done {
                                        j.eta = ((total.unwrap() - done) / speed) as i64;
                                    }
                                    j.updated = now();
                                    save(app, &j);
                                }
                                last_tick = Instant::now();
                            }
                        } else if let Some(path_str) = line.strip_prefix("after_move:DCFILE ") {
                            moved_file = Some(PathBuf::from(path_str.trim()));
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }

    let status = child.wait().await.map_err(|e| format!("yt-dlp process error: {e}"))?;
    if !status.success() {
        return Err("yt-dlp video download failed".into());
    }

    // Locate resulting file
    let source_file = if let Some(f) = moved_file.filter(|p| p.is_file()) {
        f
    } else if part_path.exists() {
        part_path.to_path_buf()
    } else {
        // Look for part_path with any extension appended by yt-dlp (e.g. .mp4, .mkv)
        let dir = part_path.parent().unwrap_or(FsPath::new("."));
        let stem = part_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let found = std::fs::read_dir(dir).ok().and_then(|entries| {
            entries.flatten().find(|e| {
                e.file_name().to_string_lossy().starts_with(stem) && e.path().extension().is_some_and(|ext| ext != "ytdl" && ext != "part" && ext != "m4a" && ext != "opus" && ext != "aac")
            }).map(|e| e.path())
        });
        found.ok_or("Could not locate downloaded video file on disk")?
    };

    tokio::fs::rename(&source_file, dest_path).await.map_err(|e| format!("Could not move completed video: {e}"))?;

    if let Ok(meta) = tokio::fs::metadata(dest_path).await {
        let file_size = meta.len();
        if let Some(job) = job_ref(app, id).await {
            let mut j = job.lock().await;
            j.size = file_size as i64;
            j.downloaded = file_size;
            save(app, &j);
        }
    }

    if let Some(scanner) = &app.scanner {
        set_status(app, id, "scanning", "").await;
        if let Err(e) = scan(scanner, dest_path).await {
            let _ = tokio::fs::remove_file(dest_path).await;
            return Err(e);
        }
        if let Some(job) = job_ref(app, id).await {
            job.lock().await.scan = "clean".into();
        }
    } else if let Some(job) = job_ref(app, id).await {
        job.lock().await.scan = "not_scanned".into();
    }

    Ok(Some(()))
}

fn inspect(name: &str, mime: &str, b: &[u8]) -> Result<(), String> {
    let ext = FsPath::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if [
        "exe", "dll", "com", "scr", "msi", "bat", "cmd", "ps1", "vbs", "js", "jse", "hta", "reg",
        "sh", "app", "dmg", "pkg", "deb", "rpm",
    ]
    .contains(&ext.as_str())
    {
        return Err(format!("Blocked file type: .{ext}"));
    }
    if mime.to_lowercase().contains("text/html")
        || String::from_utf8_lossy(b)
            .trim_start()
            .to_lowercase()
            .starts_with("<!doctype html")
    {
        return Err("URL returned a web page".into());
    }
    if b.starts_with(b"MZ")
        || b.starts_with(b"\x7fELF")
        || b.starts_with(b"#!")
        || b.starts_with(b"\xfe\xed\xfa")
    {
        return Err("Executable or script content is blocked".into());
    }
    Ok(())
}

async fn scan(address: &str, path: &FsPath) -> Result<(), String> {
    let mut s = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|_| "Malware scanner unavailable; download blocked")?;
    s.write_all(b"zINSTREAM\0")
        .await
        .map_err(|_| "Scanner protocol error")?;
    let mut f = tokio::fs::File::open(path)
        .await
        .map_err(|_| "Could not open file for scanning")?;
    let mut b = vec![0; 1024 * 1024];
    loop {
        let n = f.read(&mut b).await.map_err(|_| "File scan read error")?;
        s.write_all(&(n as u32).to_be_bytes())
            .await
            .map_err(|_| "Scanner write error")?;
        if n == 0 {
            break;
        }
        s.write_all(&b[..n])
            .await
            .map_err(|_| "Scanner write error")?;
    }
    let mut out = vec![0; 4096];
    let n = tokio::time::timeout(Duration::from_secs(180), s.read(&mut out))
        .await
        .map_err(|_| "Malware scan timed out")?
        .map_err(|_| "Scanner read error")?;
    let msg = String::from_utf8_lossy(&out[..n]);
    if msg.contains(" OK") {
        Ok(())
    } else if msg.contains(" FOUND") {
        Err("Malware detected; file blocked".into())
    } else {
        Err(format!("Scanner rejected file: {}", msg.trim()))
    }
}

async fn job_ref(app: &App, id: &str) -> Option<Arc<AsyncMutex<Job>>> {
    app.jobs.read().await.get(id).cloned()
}

fn save(app: &App, j: &Job) {
    if let Ok(db) = app.db.lock() {
        let _ = db.execute(
            "INSERT INTO downloads(id,name,mime,size,downloaded,status,scan,error,created,updated,url,selector)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
             ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name,
                 mime=excluded.mime,
                 size=excluded.size,
                 downloaded=excluded.downloaded,
                 status=excluded.status,
                 scan=excluded.scan,
                 error=excluded.error,
                 updated=excluded.updated,
                 url=CASE WHEN excluded.url != '' THEN excluded.url ELSE downloads.url END,
                 selector=CASE WHEN excluded.selector != '' THEN excluded.selector ELSE downloads.selector END",
            params![
                j.id,
                j.name,
                j.mime,
                j.size,
                j.downloaded as i64,
                j.status,
                j.scan,
                j.error,
                j.created as i64,
                j.updated as i64,
                j.url,
                j.selector
            ],
        );
    }
}

async fn set_status(app: &App, id: &str, status: &str, error: &str) {
    if let Some(j) = job_ref(app, id).await {
        let mut j = j.lock().await;
        j.status = status.into();
        j.error = error.into();
        j.speed = 0;
        j.eta = -1;
        j.updated = now();
        save(app, &j);
    }
}

async fn list(State(app): State<App>) -> Response {
    let mut rows = Vec::new();
    if let Ok(db) = app.db.lock() {
        if let Ok(mut stmt) = db.prepare(
            "SELECT id,name,mime,size,downloaded,status,scan,error,created,updated,url,selector
             FROM downloads ORDER BY created DESC LIMIT 200",
        ) {
            if let Ok(iter) = stmt.query_map([], |r| {
                Ok(Job {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    mime: r.get(2)?,
                    size: r.get(3)?,
                    downloaded: r.get::<_, i64>(4)? as u64,
                    status: r.get(5)?,
                    scan: r.get(6)?,
                    error: r.get(7)?,
                    created: r.get::<_, i64>(8)? as u64,
                    updated: r.get::<_, i64>(9)? as u64,
                    url: r.get(10)?,
                    selector: r.get(11)?,
                    speed: 0,
                    eta: -1,
                    segments: Vec::new(),
                    cancel: None,
                })
            }) {
                rows.extend(iter.flatten());
            }
        }
    }
    for item in app.jobs.read().await.values() {
        let j = item.lock().await.clone();
        if let Some(position) = rows.iter().position(|old| old.id == j.id) {
            rows[position] = j;
        } else {
            rows.push(j)
        }
    }
    Json(serde_json::json!({ "downloads": rows })).into_response()
}

async fn get_job(State(app): State<App>, Path(id): Path<String>) -> Response {
    if let Some(j) = job_ref(&app, &id).await {
        let snapshot = j.lock().await.clone();
        return Json(snapshot).into_response();
    }
    let Ok(db) = app.db.lock() else {
        return err(StatusCode::NOT_FOUND, "Not found");
    };
    let row = db
        .query_row(
            "SELECT id,name,mime,size,downloaded,status,scan,error,created,updated,url,selector
             FROM downloads WHERE id=?1",
            [id],
            |r| {
                Ok(Job {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    mime: r.get(2)?,
                    size: r.get(3)?,
                    downloaded: r.get::<_, i64>(4)? as u64,
                    status: r.get(5)?,
                    scan: r.get(6)?,
                    error: r.get(7)?,
                    created: r.get::<_, i64>(8)? as u64,
                    updated: r.get::<_, i64>(9)? as u64,
                    url: r.get(10)?,
                    selector: r.get(11)?,
                    speed: 0,
                    eta: -1,
                    segments: Vec::new(),
                    cancel: None,
                })
            },
        )
        .optional();
    match row {
        Ok(Some(j)) => Json(j).into_response(),
        _ => err(StatusCode::NOT_FOUND, "Not found"),
    }
}

async fn cancel(State(app): State<App>, Path(id): Path<String>) -> Response {
    let Some(j) = job_ref(&app, &id).await else {
        return err(StatusCode::NOT_FOUND, "Not found");
    };
    let mut j = j.lock().await;
    if !["completed", "cancelled"].contains(&j.status.as_str()) {
        if let Some(c) = &j.cancel {
            let _ = c.send(DownloadSignal::Cancel);
        }
        j.status = "cancelled".into();
        j.speed = 0;
        j.eta = -1;
        j.segments.clear();
        j.updated = now();
        save(&app, &j);
    }
    let _ = tokio::fs::remove_file(app.files.join(format!("{id}.part"))).await;
    let _ = tokio::fs::remove_file(record_path(&app.files.join(format!("{id}.part")))).await;
    let _ = tokio::fs::remove_file(app.files.join(format!("{id}.bin"))).await;
    Json(j.clone()).into_response()
}

async fn file(State(app): State<App>, Path(id): Path<String>) -> Response {
    let (name, mime) = if let Some(j) = job_ref(&app, &id).await {
        let j = j.lock().await;
        if j.status != "completed" {
            return err(StatusCode::NOT_FOUND, "File is unavailable");
        }
        (j.name.clone(), j.mime.clone())
    } else {
        let Ok(db) = app.db.lock() else {
            return err(StatusCode::NOT_FOUND, "File not found");
        };
        let stored = db
            .query_row(
                "SELECT name,mime FROM downloads WHERE id=?1 AND status='completed'",
                [&id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional();
        match stored {
            Ok(Some(v)) => v,
            _ => return err(StatusCode::NOT_FOUND, "File not found"),
        }
    };
    let path = app.files.join(format!("{}.bin", id));
    match tokio::fs::File::open(path).await {
        Ok(file) => {
            let mut r = Body::from_stream(ReaderStream::new(file)).into_response();
            let effective_mime = if !mime.is_empty() {
                mime.as_str()
            } else {
                mime_guess::from_path(&name).first_raw().unwrap_or("application/octet-stream")
            };
            r.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(effective_mime)
                    .unwrap_or(HeaderValue::from_static("application/octet-stream")),
            );
            r.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&format!(
                    "attachment; filename=\"{}\"",
                    name.replace('"', "")
                ))
                .unwrap_or(HeaderValue::from_static("attachment")),
            );
            r.headers_mut().insert(
                "X-Content-Type-Options",
                HeaderValue::from_static("nosniff"),
            );
            r
        }
        Err(_) => err(StatusCode::NOT_FOUND, "File not found"),
    }
}

async fn events(State(app): State<App>, Path(id): Path<String>) -> Response {
    if job_ref(&app, &id).await.is_none() {
        return err(StatusCode::NOT_FOUND, "Not found");
    }
    let output = stream! {
        let mut tick = interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            if let Some(j) = job_ref(&app, &id).await {
                let snapshot = j.lock().await.clone();
                let done = ["completed", "failed", "cancelled"].contains(&snapshot.status.as_str());
                yield Ok::<Event, std::convert::Infallible>(
                    Event::default().data(serde_json::to_string(&snapshot).unwrap_or_default())
                );
                if done {
                    break;
                }
            } else {
                break;
            }
        }
    };
    Sse::new(output)
        .keep_alive(KeepAlive::default())
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_name() {
        assert_eq!(sanitize_name("CON.txt"), "_CON.txt");
        assert_eq!(sanitize_name("com1.tar.gz"), "_COM1.tar.gz");
        assert_eq!(sanitize_name("nul"), "_NUL");
        assert_eq!(sanitize_name("   test:file*name?.pdf  "), "test_file_name_.pdf");
        assert_eq!(sanitize_name("...."), "download");
        assert_eq!(sanitize_name("____"), "download");
        assert_eq!(sanitize_name(""), "download");
    }

    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("archive%2Ezip"), "archive.zip");
        assert_eq!(percent_decode("%E6%BC%A2%E5%AD%97.txt"), "漢字.txt");
    }

    #[test]
    fn test_from_disposition() {
        let h1 = "attachment; filename=\"report.pdf\"";
        assert_eq!(from_disposition(h1), Some("report.pdf".into()));

        let h2 = "attachment; filename*=UTF-8''%e2%82%ac%20rates.csv";
        assert_eq!(from_disposition(h2), Some("€ rates.csv".into()));
    }

    #[test]
    fn test_is_video_host() {
        let u1 = Url::parse("https://www.youtube.com/watch?v=dQw4w9WgXcQ").unwrap();
        assert!(is_video_host(&u1));
        let u2 = Url::parse("https://youtu.be/dQw4w9WgXcQ").unwrap();
        assert!(is_video_host(&u2));
        let u3 = Url::parse("https://vimeo.com/12345678").unwrap();
        assert!(is_video_host(&u3));
        let u4 = Url::parse("https://example.org/archive.zip").unwrap();
        assert!(!is_video_host(&u4));
    }
}
