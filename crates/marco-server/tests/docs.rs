//! The docs are public on both listeners, with each page's card tags in its
//! head; the landing page is the API listener's; the console's data stays
//! behind the token.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;

use marco_server::app::{App, Config};
use marco_server::auth::AuthConfig;

const TOKEN: &str = "test-token-0123456789";
const INDEX: &str = "<!doctype html><title>marco</title><script src=\"/assets/app-1.js\"></script>";
const PAGES: &str = r#"[{"slug":"overview","title":"Overview","summary":"A graph database."},{"slug":"query-api","title":"Query API","summary":"Reads & <writes>."}]"#;

/// One HTTP/1.1 GET: (status, lower-cased head, body).
fn get(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> (u16, String, String) {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    let mut req = format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let status = out[9..12].parse().unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    (status, head.to_lowercase(), body.to_string())
}

fn post(addr: SocketAddr, path: &str) -> u16 {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{{}}"
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out[9..12].parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docs_are_public() {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap");
    marco_server::synth::write(&snap, 50, 2).unwrap();
    let ui_dir = dir.path().join("dist");
    std::fs::create_dir_all(ui_dir.join("assets")).unwrap();
    std::fs::write(ui_dir.join("index.html"), INDEX).unwrap();
    std::fs::write(ui_dir.join("assets/app-1.js"), "console.log(1)").unwrap();
    std::fs::write(ui_dir.join("favicon.svg"), "<svg/>").unwrap();
    std::fs::create_dir_all(ui_dir.join("docs")).unwrap();
    std::fs::write(ui_dir.join("docs/design.html"), "<!doctype html><style>p{}</style><p>design").unwrap();
    std::fs::write(ui_dir.join("docs/_pages.json"), PAGES).unwrap();
    std::fs::create_dir_all(ui_dir.join("og")).unwrap();
    std::fs::write(ui_dir.join("og/marco.png"), "png").unwrap();

    let auth = AuthConfig { token: Some(TOKEN.into()), ..Default::default() };
    let app = App::open(Config { auth, ..Config::new(snap) }).unwrap();
    let site = marco_server::ui::Site { public_url: Some("https://marco.example.com".into()), handle_resolver: None };
    let ui = Arc::new(marco_server::ui::Ui::load_with(Some(&ui_dir), site).unwrap());
    let (api, admin) = marco_server::serve(app, ui).await.unwrap();

    for addr in [api, admin] {
        for (path, title) in [
            ("/docs", "Overview · marco docs"),
            ("/docs/", "Overview · marco docs"),
            ("/docs/overview", "Overview · marco docs"),
            ("/docs/query-api", "Query API · marco docs"),
            ("/docs/query-api/", "Query API · marco docs"),
            ("/docs/nope", "marco docs"),
        ] {
            let (status, head, body) = get(addr, path, &[]);
            assert_eq!(status, 200, "{addr}{path}");
            assert!(head.contains("content-type: text/html"), "{path}: {head}");
            assert!(head.contains("content-security-policy: default-src 'none'"), "{path}");
            assert!(body.starts_with("<!doctype html><title>"), "{path}: {body}");
            assert!(body.ends_with("<script src=\"/assets/app-1.js\"></script>"), "{path}");
            assert!(body.contains(&format!("<title>{title}</title>")), "{path}: {body}");
            assert!(body.contains(r#"<meta name="twitter:card" content="summary_large_image" />"#), "{path}");
            assert!(body.contains(r#"<meta property="og:image" content="https://marco.example.com/og/marco.png" />"#));
            assert!(body.contains(r#"<meta name="marco-public-url" content="https://marco.example.com" />"#));
        }
        let (_, _, body) = get(addr, "/docs/query-api", &[]);
        assert!(body.contains(r#"<meta property="og:url" content="https://marco.example.com/docs/query-api" />"#));
        assert!(body.contains(r#"<meta name="description" content="Reads &amp; &lt;writes&gt;." />"#), "{body}");
        assert!(body.contains(r#"<meta property="og:type" content="article" />"#));
        assert_eq!(get(addr, "/og/marco.png", &[]).0, 200);
        let (status, head, body) = get(addr, "/assets/app-1.js", &[]);
        assert_eq!((status, body.as_str()), (200, "console.log(1)"));
        assert!(head.contains("immutable"));
        assert_eq!(get(addr, "/favicon.svg", &[]).0, 200);
        assert_eq!(get(addr, "/assets/nope.js", &[]).0, 404);
        assert_eq!(get(addr, "/assets/../index.html", &[]).0, 404);
        let (status, head, body) = get(addr, "/docs/design.html", &[]);
        assert_eq!(status, 200);
        assert!(body.ends_with("<p>design"));
        assert!(head.contains("style-src 'unsafe-inline'") && head.contains("sandbox"), "{head}");
        assert_eq!(get(addr, "/docs/nope.html", &[]).0, 404);
    }

    // the landing page is the API listener's; the admin listener's / is the console
    let (status, _, body) = get(api, "/", &[]);
    assert_eq!(status, 200);
    assert!(body.contains(r#"<meta property="og:url" content="https://marco.example.com/" />"#), "{body}");
    assert!(body.contains(r#"<meta property="og:type" content="website" />"#));
    let (status, head, _) = get(admin, "/", &[]);
    assert_eq!(status, 303);
    assert!(head.contains("location: /admin/"), "{head}");
    // no --handle-resolver, no /resolve
    assert_eq!(get(api, "/resolve?handle=alice.test", &[]).0, 404);
    // the console's pages carry no card tags
    assert_eq!(get(admin, "/admin/", &[]).2, INDEX);

    assert_eq!(get(api, "/docs/_console", &[]).2, r#"{"console":false}"#);
    assert_eq!(get(admin, "/docs/_console", &[]).2, r#"{"console":true}"#);

    // the console is on the admin listener only, and its data needs the token
    assert_eq!(get(api, "/admin/", &[]).0, 404);
    assert_eq!(get(admin, "/admin/", &[]).0, 200);
    assert_eq!(get(admin, "/admin/storage", &[]).2, INDEX);
    assert_eq!(post(admin, "/marco.v1.Admin/GetOverview"), 401);
    assert_eq!(get(admin, "/metrics", &[]).0, 401);
    let bearer = format!("Bearer {TOKEN}");
    assert_eq!(get(admin, "/metrics", &[("authorization", &bearer)]).0, 200);
    // the bench OTLP sink is marco-load's, not the server's
    assert_eq!(post(admin, "/v1/traces"), 404);
}
