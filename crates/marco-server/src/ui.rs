//! The console, the docs and the landing page, one build (`ui/` into
//! `ui/dist`), built into the binary and read whole at start, or from
//! `--ui-dir` instead. Serving only the names read here rules out traversal.
//! `/admin/*`, `/docs/*` and `/` (the API listener's landing page) get
//! index.html, the docs and `/` with their own title, description and card
//! tags in its head (crawlers don't run the script), from the build's
//! `docs/_pages.json`. `/assets/*`, `/og/*`, `/favicon.svg` and the standalone
//! pages (`/docs/<name>.html`: design and results) are files. The docs and the
//! files are public: the console's page is static, and its data comes from
//! the Admin RPCs behind the token.

mod resolve;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::Path as UrlPath;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use http::{header, HeaderValue, StatusCode};
use rust_embed::RustEmbed;

// A checkout without `npm run build` still compiles; the page then says so.
// Debug builds read the folder from disk, so a UI rebuild needs no cargo build.
#[derive(RustEmbed)]
#[folder = "../../ui/dist"]
#[allow_missing = true]
struct Embedded;

const PLACEHOLDER: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>marco</title></head>\
<body><p>This marco was built without its console and docs. Build them (<code>cd ui &amp;&amp; npm ci &amp;&amp; npm run build</code>) \
and then marco, or point <code>--ui-dir</code> at a built <code>ui/dist</code>, then restart.</p></body></html>";

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

/// How the public pages describe themselves.
#[derive(Clone, Debug, Default)]
pub struct Site {
    /// The API listener's public origin (`https://marco.example.com`): the
    /// cards' og:url and og:image, and the docs' read examples. Unset, the
    /// tags are relative and the examples keep `localhost:7300`.
    pub public_url: Option<String>,
    /// An XRPC host answering `com.atproto.identity.resolveHandle`, for
    /// `/resolve`. Unset, `/resolve` is 404 and the explorer takes DIDs.
    pub handle_resolver: Option<String>,
}

/// What the landing page and a docs page put in their head.
struct Head<'a> {
    title: &'a str,
    description: &'a str,
    path: &'a str,
    kind: &'a str,
}

const SITE_DESCRIPTION: &str = "A graph database for atproto actor-to-actor edges: follows, blocks, list items and any record whose subject is a DID.";
const OG_IMAGE: &str = "/og/marco.png";

pub struct Ui {
    files: HashMap<String, File>,
    /// index.html with each public page's head: "" is `/`, "docs" the
    /// fallback for a slug the build doesn't have, else the docs slug.
    pages: HashMap<String, axum::body::Bytes>,
    resolver: Option<Arc<resolve::Resolver>>,
}

#[derive(serde::Deserialize)]
struct PageMeta {
    slug: String,
    title: String,
    summary: String,
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
        Ui::load_with(dir, Site::default())
    }

    pub fn load_with(dir: Option<&Path>, site: Site) -> Result<Ui> {
        let files = Ui::files(dir)?;
        let pages = heads(&files, &site)?;
        let resolver = site.handle_resolver.as_deref().map(|u| Arc::new(resolve::Resolver::new(u)));
        Ok(Ui { files, pages, resolver })
    }

    fn files(dir: Option<&Path>) -> Result<HashMap<String, File>> {
        if let Some(d) = dir {
            let mut files = HashMap::new();
            walk(d, d, &mut files).with_context(|| format!("--ui-dir {}", d.display()))?;
            anyhow::ensure!(files.contains_key("index.html"), "--ui-dir {}: no index.html", d.display());
            return Ok(files);
        }
        let mut files = HashMap::new();
        for name in Embedded::iter() {
            if let Some(f) = Embedded::get(&name) {
                let mime = HeaderValue::from_static(mime(Path::new(name.as_ref())));
                files.insert(name.into_owned(), File { data: f.data.into_owned().into(), mime });
            }
        }
        files.entry("index.html".into()).or_insert_with(|| File {
            data: PLACEHOLDER.into(),
            mime: HeaderValue::from_static("text/html; charset=utf-8"),
        });
        Ok(files)
    }

    fn page(&self) -> Response {
        match self.files.get("index.html") {
            Some(f) => respond(f, false),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    /// `key` as in [`Ui::pages`].
    fn public_page(&self, key: &str) -> Response {
        let (Some(f), Some(html)) =
            (self.files.get("index.html"), self.pages.get(key).or_else(|| self.pages.get("docs")))
        else {
            return self.page();
        };
        respond(&File { data: html.clone(), mime: f.mime.clone() }, false)
    }

    fn file(&self, name: &str) -> Response {
        match self.files.get(name) {
            Some(f) => respond(f, name.starts_with("assets/")),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn doc(&self, rest: &str) -> Response {
        if !rest.ends_with(".html") {
            let slug = rest.trim_end_matches('/');
            return self.public_page(if slug.is_empty() { "overview" } else { slug });
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

/// index.html once per public page, its `<title>` swapped for that page's
/// head (or the head added before `</head>`).
fn heads(files: &HashMap<String, File>, site: &Site) -> Result<HashMap<String, axum::body::Bytes>> {
    let Some(index) = files.get("index.html") else { return Ok(HashMap::new()) };
    let index = std::str::from_utf8(&index.data).context("index.html isn't UTF-8")?;
    let docs: Vec<PageMeta> = match files.get("docs/_pages.json") {
        Some(f) => serde_json::from_slice(&f.data).context("docs/_pages.json")?,
        None => Vec::new(),
    };
    let fill = |h: Head| -> axum::body::Bytes {
        let tags = head_tags(site, &h);
        let html = if index.contains("<title>marco</title>") {
            index.replacen("<title>marco</title>", &tags, 1)
        } else {
            index.replacen("</head>", &format!("{tags}</head>"), 1)
        };
        html.into()
    };
    let mut out = HashMap::new();
    out.insert(
        String::new(),
        fill(Head {
            title: "marco: a graph database for atproto",
            description: SITE_DESCRIPTION,
            path: "/",
            kind: "website",
        }),
    );
    out.insert(
        "docs".into(),
        fill(Head { title: "marco docs", description: SITE_DESCRIPTION, path: "/docs", kind: "website" }),
    );
    for p in &docs {
        let title = format!("{} · marco docs", p.title);
        let path = if p.slug == "overview" { "/docs".to_string() } else { format!("/docs/{}", p.slug) };
        out.insert(p.slug.clone(), fill(Head { title: &title, description: &p.summary, path: &path, kind: "article" }));
    }
    Ok(out)
}

fn head_tags(site: &Site, h: &Head) -> String {
    let origin = site.public_url.as_deref().unwrap_or("");
    let (title, desc) = (esc(h.title), esc(h.description));
    let url = esc(&format!("{origin}{}", h.path));
    let image = esc(&format!("{origin}{OG_IMAGE}"));
    let mut t = format!(
        "<title>{title}</title>\n\
<meta name=\"description\" content=\"{desc}\" />\n\
<meta property=\"og:site_name\" content=\"marco\" />\n\
<meta property=\"og:type\" content=\"{kind}\" />\n\
<meta property=\"og:title\" content=\"{title}\" />\n\
<meta property=\"og:description\" content=\"{desc}\" />\n\
<meta property=\"og:url\" content=\"{url}\" />\n\
<meta property=\"og:image\" content=\"{image}\" />\n\
<meta property=\"og:image:width\" content=\"1200\" />\n\
<meta property=\"og:image:height\" content=\"630\" />\n\
<meta property=\"og:image:alt\" content=\"marco: a graph database for atproto\" />\n\
<meta name=\"twitter:card\" content=\"summary_large_image\" />\n\
<meta name=\"twitter:title\" content=\"{title}\" />\n\
<meta name=\"twitter:description\" content=\"{desc}\" />\n\
<meta name=\"twitter:image\" content=\"{image}\" />\n",
        kind = h.kind,
    );
    if !origin.is_empty() {
        t.push_str(&format!(
            "<link rel=\"canonical\" href=\"{url}\" />\n<meta name=\"marco-public-url\" content=\"{}\" />\n",
            esc(origin)
        ));
    }
    t
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('"', "&quot;").replace('<', "&lt;").replace('>', "&gt;")
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

/// The docs and the files they load, for both listeners, and on the one
/// without the console (the API listener) the landing page at `/` and
/// `/resolve`. `/docs/_console` tells the docs whether this listener has the
/// console, without a 404 in the browser's console where it doesn't (no
/// page's name starts with `_`).
pub fn docs_routes(ui: Arc<Ui>, console: bool) -> Router {
    let page = |ui: Arc<Ui>, key: &'static str| get(move || async move { ui.public_page(key) });
    let (a, b, c, d) = (ui.clone(), ui.clone(), ui.clone(), ui.clone());
    let mut r = Router::new()
        .route("/docs", page(ui.clone(), "overview"))
        .route("/docs/", page(ui.clone(), "overview"))
        .route("/docs/_console", get(move || async move { axum::Json(serde_json::json!({ "console": console })) }))
        .route("/docs/{*rest}", get(move |UrlPath(rest): UrlPath<String>| async move { c.doc(&rest) }))
        .route(
            "/assets/{*rest}",
            get(move |UrlPath(rest): UrlPath<String>| async move { a.file(&format!("assets/{rest}")) }),
        )
        .route("/og/{*rest}", get(move |UrlPath(rest): UrlPath<String>| async move { d.file(&format!("og/{rest}")) }))
        .route("/favicon.svg", get(move || async move { b.file("favicon.svg") }));
    if !console {
        r = r.route("/", page(ui.clone(), ""));
        if let Some(res) = ui.resolver.clone() {
            r = r.route("/resolve", get(move |q| async move { res.handle(q).await }));
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(index: &str) -> HashMap<String, File> {
        let html = HeaderValue::from_static("text/html; charset=utf-8");
        let mut f = HashMap::new();
        f.insert("index.html".into(), File { data: index.to_string().into(), mime: html.clone() });
        let pages = r#"[{"slug":"ingest","title":"Writing \"and\" ingest","summary":"Writes."}]"#;
        f.insert("docs/_pages.json".into(), File { data: pages.into(), mime: html });
        f
    }

    #[test]
    fn heads_without_a_public_url_are_relative() {
        let p = heads(&files("<html><head><title>marco</title></head></html>"), &Site::default()).unwrap();
        let ingest = std::str::from_utf8(&p["ingest"]).unwrap();
        assert!(ingest.contains("<title>Writing &quot;and&quot; ingest · marco docs</title>"), "{ingest}");
        assert!(ingest.contains(r#"<meta property="og:url" content="/docs/ingest" />"#));
        assert!(ingest.contains(r#"<meta property="og:image" content="/og/marco.png" />"#));
        assert!(!ingest.contains("marco-public-url") && !ingest.contains("canonical"));
        assert_eq!(ingest.matches("<title>").count(), 1);
        assert!(p.contains_key("") && p.contains_key("docs"));
    }

    #[test]
    fn heads_go_before_head_end_without_the_title() {
        let site = Site { public_url: Some("https://m.example".into()), handle_resolver: None };
        let p = heads(&files("<html><head></head><body></body></html>"), &site).unwrap();
        let home = std::str::from_utf8(&p[""]).unwrap();
        assert!(home.starts_with("<html><head><title>marco: a graph database for atproto</title>"), "{home}");
        assert!(home.contains(r#"<link rel="canonical" href="https://m.example/" />"#));
        assert!(home.ends_with("</head><body></body></html>"));
    }
}
