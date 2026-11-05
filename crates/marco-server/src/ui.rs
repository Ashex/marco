//! The console and the docs, one build (`ui/` into `ui/dist`), read whole
//! at start from `--ui-dir` or this source tree's build. Serving only the
//! names read here rules out traversal. `/admin/*` and `/docs/*` get
//! index.html, `/assets/*`, `/favicon.svg` and the standalone pages
//! (`/docs/<name>.html`: design and results) are files. The docs and the
//! files are public: the console's page is static, and its data comes from
//! the Admin RPCs behind the token.

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
<body><p>This marco runs without its console and docs. Build them (<code>cd ui &amp;&amp; npm ci &amp;&amp; npm run build</code>), \
or point <code>--ui-dir</code> at a built <code>ui/dist</code>, then restart.</p></body></html>";

/// Same-origin only. Inline style attributes set through the CSSOM (React
/// `style`) aren't governed by style-src.
const CSP: &str =
    "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data: blob:; \
connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

/// The standalone pages carry their styles inline and run no script.
const PAGE_CSP: &str = "default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; \
frame-ancestors 'none'; base-uri 'none'; form-action 'none'; sandbox";

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

    fn page(&self) -> Response {
        match self.files.get("index.html") {
            Some(f) => respond(f, false),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn file(&self, name: &str) -> Response {
        match self.files.get(name) {
            Some(f) => respond(f, name.starts_with("assets/")),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn doc(&self, rest: &str) -> Response {
        if !rest.ends_with(".html") {
            return self.page();
        }
        match self.files.get(&format!("docs/{rest}")) {
            Some(f) => {
                let mut r = respond(f, false);
                r.headers_mut().insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(PAGE_CSP));
                r
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
}

fn respond(f: &File, immutable: bool) -> Response {
    // hashed names under assets/ never change content
    let cc = if immutable { "public, max-age=31536000, immutable" } else { "no-cache" };
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

/// The admin listener's: the console, the docs and their files.
pub fn routes(ui: Arc<Ui>) -> Router {
    let page = |ui: Arc<Ui>| get(move || async move { ui.page() });
    Router::new()
        .route("/", get(|| async { Redirect::to("/admin/") }))
        .route("/admin", get(|| async { Redirect::to("/admin/") }))
        .route("/admin/", page(ui.clone()))
        .route("/admin/{*rest}", page(ui.clone()))
        .merge(docs_routes(ui, true))
}

/// The docs and the files they load, for both listeners. `/docs/_console`
/// tells the docs whether this listener has the console, without a 404 in
/// the browser's console where it doesn't (no page's name starts with `_`).
pub fn docs_routes(ui: Arc<Ui>, console: bool) -> Router {
    let page = |ui: Arc<Ui>| get(move || async move { ui.page() });
    let (a, b, c) = (ui.clone(), ui.clone(), ui.clone());
    Router::new()
        .route("/docs", page(ui.clone()))
        .route("/docs/", page(ui))
        .route("/docs/_console", get(move || async move { axum::Json(serde_json::json!({ "console": console })) }))
        .route("/docs/{*rest}", get(move |UrlPath(rest): UrlPath<String>| async move { c.doc(&rest) }))
        .route(
            "/assets/{*rest}",
            get(move |UrlPath(rest): UrlPath<String>| async move { a.file(&format!("assets/{rest}")) }),
        )
        .route("/favicon.svg", get(move || async move { b.file("favicon.svg") }))
}
