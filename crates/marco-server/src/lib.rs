//! marco's server. The API listener serves marco.v1.Graph. The admin
//! listener (loopback by default) serves Graph too, marco.v1.Admin, the ops
//! routes (/metrics, /debug/pprof/*, /debug/tracing) and the console at
//! /admin.

pub mod admin_svc;
pub mod app;
pub mod auth;
pub mod graph_svc;
pub mod ingest;
pub mod liststats;
pub mod metrics;
pub mod series;
pub mod synth;
pub mod sys;
pub mod ui;
pub mod writer;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use marco_proto::connect::marco::v1::{AdminExt, GraphExt};

use crate::app::App;
use crate::auth::Listener;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMMIT: &str = match option_env!("MARCO_COMMIT") {
    Some(c) => c,
    None => "dev",
};

fn connect(app: &Arc<App>, admin: bool) -> axum::Router {
    let mut r = Arc::new(graph_svc::GraphApi(app.clone(), !admin)).register(connectrpc::Router::new());
    if admin {
        r = Arc::new(admin_svc::AdminApi(app.clone())).register(r);
    }
    let svc = r.into_axum_service();
    let mut router = axum::Router::new().route_service("/marco.v1.Graph/{*method}", svc.clone());
    if admin {
        router = router.route_service("/marco.v1.Admin/{*method}", svc);
    }
    router
}

/// 200 once every expected source has caught up since start, else 503 and why.
async fn readyz(axum::extract::State(app): axum::extract::State<Arc<App>>) -> (http::StatusCode, String) {
    match app.readiness() {
        (true, _) => (http::StatusCode::OK, "ready\n".into()),
        (false, why) => (http::StatusCode::SERVICE_UNAVAILABLE, format!("{why}\n")),
    }
}

pub fn api_router(app: &Arc<App>) -> axum::Router {
    connect(app, false)
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .route("/readyz", axum::routing::get(readyz).with_state(app.clone()))
        .layer(axum::middleware::from_fn_with_state((app.clone(), Listener::Api), auth::gate))
        .layer(axum::middleware::from_fn(metrics::time_rpcs))
}

pub fn admin_router(app: &Arc<App>, ui: Arc<ui::Ui>) -> axum::Router {
    let refresh = {
        let app = app.clone();
        Arc::new(move || app.graph().refresh_gauges()) as marco_ops::Refresh
    };
    connect(app, true)
        .merge(marco_ops::router(Some(refresh)))
        .merge(ui::routes(ui))
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .route("/readyz", axum::routing::get(readyz).with_state(app.clone()))
        .layer(axum::middleware::from_fn_with_state((app.clone(), Listener::Admin), auth::gate))
        .layer(axum::middleware::from_fn(metrics::time_rpcs))
}

/// Binds both listeners and serves them on the current runtime.
pub async fn serve(app: Arc<App>, ui: Arc<ui::Ui>) -> Result<(SocketAddr, SocketAddr)> {
    metrics::init(VERSION, COMMIT);
    let api = tokio::net::TcpListener::bind(&app.cfg.api_listen)
        .await
        .with_context(|| format!("bind --listen {}", app.cfg.api_listen))?;
    let admin = tokio::net::TcpListener::bind(&app.cfg.admin_listen)
        .await
        .with_context(|| format!("bind --admin-listen {}", app.cfg.admin_listen))?;
    let (a1, a2) = (api.local_addr()?, admin.local_addr()?);
    let r1 = api_router(&app).into_make_service_with_connect_info::<SocketAddr>();
    let r2 = admin_router(&app, ui).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(api, r1).await });
    tokio::spawn(async move { axum::serve(admin, r2).await });
    series::spawn(app);
    Ok((a1, a2))
}
