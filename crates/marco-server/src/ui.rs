//! The console (built from `ui/` into `ui/dist`, base `/admin/`), read
//! whole at start from `--ui-dir` or this source tree's build. Serving only
//! the names read here rules out traversal.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::Path as UrlPath;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use http::{header, HeaderValue, StatusCode};

const PLACEHOLDER: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>marco</title></head>\
<body><p>This marco runs without its console. Build it (<code>cd ui &amp;&amp; npm ci &amp;&amp; npm run build</code>), \
or point <code>--ui-dir</code> at a built <code>ui/dist</code>, then restart.</p></body></html>";

/// Same-origin only. Inline style attributes set through the CSSOM (React
/// `style`) aren't governed by style-src.
const CSP: &str =
    "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data: blob:; \
connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

struct File {
    data: axum::body::Bytes,
    mime: HeaderValue,
}

pub struct Ui {
    files: HashMap<String, File>,
}

fn mime(p: &Path) -> &'static str {
    match p.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "woff2" => "font/woff2",
        "json" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

impl Ui {
    pub fn load(dir: Option<&Path>) -> Result<Ui> {
        if let Some(d) = dir {
            let ui = Ui::read(d).with_context(|| format!("--ui-dir {}", d.display()))?;
            anyhow::ensure!(ui.files.contains_key("index.html"), "--ui-dir {}: no index.html", d.display());
            return Ok(ui);
        }
        let dev = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui/dist");
        if dev.join("index.html").is_file() {
            return Ui::read(&dev);
        }
        let mut files = HashMap::new();
        files.insert(
            "index.html".into(),
            File { data: PLACEHOLDER.into(), mime: HeaderValue::from_static("text/html; charset=utf-8") },
        );
        Ok(Ui { files })
    }

    fn read(dir: &Path) -> Result<Ui> {
        let mut files = HashMap::new();
        walk(dir, dir, &mut files)?;
        Ok(Ui { files })
    }

    fn serve(&self, rest: &str) -> Response {
        let (f, cache) = match self.files.get(rest) {
            Some(f) if !rest.is_empty() => (f, rest.starts_with("assets/")),
            _ => match self.files.get("index.html") {
                Some(f) => (f, false),
                None => return StatusCode::NOT_FOUND.into_response(),
            },
        };
        let cc = if cache { "public, max-age=31536000, immutable" } else { "no-cache" };
        (
            [
                (header::CONTENT_TYPE, f.mime.clone()),
                (header::CACHE_CONTROL, HeaderValue::from_static(cc)),
                (header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP)),
                (header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")),
                (header::REFERRER_POLICY, HeaderValue::from_static("no-referrer")),
            ],
            f.data.clone(),
        )
            .into_response()
    }
}

fn walk(root: &Path, dir: &Path, out: &mut HashMap<String, File>) -> Result<()> {
    for e in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = e?.path();
        let meta = std::fs::metadata(&path)?;
        if meta.is_dir() {
            walk(root, &path, out)?;
        } else if meta.is_file() {
            let rel = path
                .strip_prefix(root)?
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let data = std::fs::read(&path)?;
            out.insert(rel, File { data: data.into(), mime: HeaderValue::from_static(mime(&path)) });
        }
    }
    Ok(())
}

pub fn routes(ui: Arc<Ui>) -> Router {
    let a = ui.clone();
    let b = ui.clone();
    Router::new()
        .route("/", get(|| async { Redirect::to("/admin/") }))
        .route("/admin", get(|| async { Redirect::to("/admin/") }))
        .route("/admin/", get(move || async move { a.serve("") }))
        .route("/admin/{*rest}", get(move |UrlPath(rest): UrlPath<String>| async move { b.serve(&rest) }))
}
