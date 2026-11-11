//! The kill -9 test: `marco serve` follows a fake source replaying a
//! capture, gets SIGKILLed at random moments and restarted, and at the end
//! its state for every URI and DID the capture touched must equal the
//! capture applied in this process to the same starting snapshot.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use connectrpc::client::{ClientConfig, HttpClient};
use futures::StreamExt;
use marco_core::relation::parse_uri;
use marco_core::Relations;
use marco_ingest::firehose::{self, Frame};
use marco_ingest::{jetstream, Change, Op, Proto, Source};
use marco_proto::connect::marco::v1::AdminClient;
use marco_proto::v1::{CheckpointRequest, GetActorRequest, ResolveUriRequest};

use crate::capture::Capture;
use crate::{copy_dir, fake, free_port, open_app, wait_cursor, Rng, TOKEN};

pub struct Args {
    pub snapshot: PathBuf,
    pub capture: PathBuf,
    pub marco: PathBuf,
    pub work: PathBuf,
    pub kills: u32,
    pub rate: u64,
    pub min_ms: u64,
    pub max_ms: u64,
    pub seed: u64,
    pub catch_up: Duration,
    pub serve_args: Vec<String>,
    /// A counter on marco's /metrics that moves at each segment write.
    pub segment_metric: Option<String>,
    /// Also kill during shard rewrites.
    pub rewrite: bool,
    /// Also kill during the compaction after a checkpoint.
    pub compaction: bool,
}

/// The URIs and DIDs a capture touches.
fn touched(cap: &Capture) -> (BTreeSet<String>, BTreeSet<String>) {
    let rels = Relations::bsky_default();
    let (mut uris, mut dids) = (BTreeSet::new(), BTreeSet::new());
    for m in &cap.msgs {
        let ev = match cap.proto {
            Proto::Jetstream => jetstream::decode(&m.bytes, &rels).ok(),
            Proto::JetstreamV2 => unreachable!("the harness has no Jetstream v2 mode"),
            Proto::Firehose => match firehose::decode(&m.bytes, &rels) {
                Ok(Frame::Event(ev)) => Some(ev),
                _ => None,
            },
        };
        match ev.map(|e| e.change) {
            Some(Change::Ops(ops)) => {
                for op in ops {
                    let uri = match op {
                        Op::Add { uri, subject, list, .. } => {
                            dids.insert(subject);
                            // a list node reads like an actor
                            dids.extend(list);
                            uri
                        }
                        Op::Delete { uri, .. } => uri,
                    };
                    if let Some((d, _, _)) = parse_uri(&uri) {
                        dids.insert(d.to_string());
                    }
                    uris.insert(uri);
                }
            }
            Some(Change::Status { did, .. }) => {
                dids.insert(did);
            }
            _ => {}
        }
    }
    (uris, dids)
}

fn admin(addr: SocketAddr) -> AdminClient<HttpClient> {
    let c = ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_default_header(
        http::header::AUTHORIZATION,
        format!("Bearer {TOKEN}").parse::<http::HeaderValue>().unwrap(),
    );
    AdminClient::new(HttpClient::plaintext(), c)
}

async fn uri_state(c: &AdminClient<HttpClient>, uri: &str) -> String {
    match c.resolve_uri(ResolveUriRequest { uri: uri.into(), ..Default::default() }).await {
        Ok(r) => {
            let r = r.into_owned();
            format!(
                "rel={} subject={} pair_records={} in_contains={} subject_in={}",
                r.rel, r.subject, r.pair_records, r.in_contains, r.subject_in_count
            )
        }
        Err(e) => format!("error {:?}", e.code),
    }
}

async fn did_state(c: &AdminClient<HttpClient>, did: &str) -> String {
    match c.get_actor(GetActorRequest { actor: did.into(), ..Default::default() }).await {
        Ok(r) => {
            let r = r.into_owned();
            let mut s = format!("status={}", r.status);
            for x in &r.rels {
                s.push_str(&format!(" {}:out={},in={},mutual={},first={}", x.rel, x.out, x.r#in, x.mutual, x.first_us));
            }
            s
        }
        Err(e) => format!("error {:?}", e.code),
    }
}

/// Every key whose state differs: (key, expected, got).
async fn compare(
    want: SocketAddr,
    got: SocketAddr,
    uris: &BTreeSet<String>,
    dids: &BTreeSet<String>,
) -> Vec<(String, String, String)> {
    let (w, g) = (admin(want), admin(got));
    let keys: Vec<(bool, &String)> = uris.iter().map(|u| (true, u)).chain(dids.iter().map(|d| (false, d))).collect();
    futures::stream::iter(keys)
        .map(|(is_uri, k)| {
            let (w, g) = (&w, &g);
            async move {
                let (a, b) = if is_uri {
                    (uri_state(w, k).await, uri_state(g, k).await)
                } else {
                    (did_state(w, k).await, did_state(g, k).await)
                };
                (a != b).then(|| (k.clone(), a, b))
            }
        })
        .buffer_unordered(32)
        .filter_map(|x| async move { x })
        .collect()
        .await
}

/// SIGKILL, after checking the pid is ours and is marco.
fn kill9(pid: u32) -> Result<()> {
    if pid == 0 || pid == 1 {
        bail!("refusing to kill pid {pid}");
    }
    let out = std::process::Command::new("ps").args(["-o", "comm=", "-p", &pid.to_string()]).output()?;
    let comm = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if comm != "marco" {
        bail!("pid {pid} is {comm:?}, not marco");
    }
    if unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) } != 0 {
        bail!("kill {pid}: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

struct Marco {
    child: tokio::process::Child,
    admin: SocketAddr,
}

fn spawn(a: &Args, snap: &Path, src_url: &str, token: &Path, log: &Path) -> Result<Marco> {
    let (api, admin) = (free_port()?, free_port()?);
    let logf = std::fs::OpenOptions::new().create(true).append(true).open(log)?;
    let flag = if src_url.contains("/xrpc/") { "--firehose" } else { "--jetstream" };
    let child = tokio::process::Command::new(&a.marco)
        .arg("serve")
        .arg("--snapshot")
        .arg(snap)
        .args(["--listen", &api.to_string(), "--admin-listen", &admin.to_string()])
        .arg("--admin-token-file")
        .arg(token)
        .args([flag, src_url])
        .args(&a.serve_args)
        .stdout(logf.try_clone()?)
        .stderr(logf)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", a.marco.display()))?;
    Ok(Marco { child, admin })
}

/// When in a run its kill comes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum When {
    /// A random time in [min-ms, max-ms].
    Random,
    /// Just after the segment counter moves.
    AfterSegment,
    /// 30 ms before the next segment write, by the last two's interval.
    BeforeSegment,
    /// Within 300 ms of starting a checkpoint (Admin.Checkpoint).
    DuringRewrite,
    /// Within 500 ms of a checkpoint's commit, when the compaction it starts runs.
    DuringCompaction,
}

/// A metric's value from /metrics text: `name` with no labels, or `name{labels}` as written.
fn metric(body: &str, name: &str) -> Option<f64> {
    let key = format!("{name} ");
    body.lines().find_map(|l| l.strip_prefix(&key)).and_then(|v| v.trim().parse::<f64>().ok())
}

/// marco's applied cursor for `source` and how far behind its event time
/// it was (µs); None before it has applied anything.
fn progress(body: &str, source: &str) -> Option<(u64, i64)> {
    let get = |name: &str| metric(body, &format!("{name}{{source=\"{source}\"}}"));
    let c = get("marco_ingest_cursor").filter(|&c| c > 0.0)?;
    Some((c as u64, get("marco_ingest_behind_microseconds").unwrap_or(f64::MAX) as i64))
}

pub async fn run(a: Args) -> Result<bool> {
    let cap = Arc::new(Capture::load(&a.capture)?);
    let (uris, dids) = touched(&cap);
    println!(
        "capture {}: {} messages, {} URIs and {} DIDs touched",
        a.capture.display(),
        cap.msgs.len(),
        uris.len(),
        dids.len()
    );
    std::fs::create_dir_all(&a.work)?;
    let (got_snap, want_snap) = (a.work.join("got"), a.work.join("want"));
    for d in [&got_snap, &want_snap] {
        if d.exists() {
            std::fs::remove_dir_all(d)?;
        }
        copy_dir(&a.snapshot, d)?;
    }
    let token = a.work.join("token");
    std::fs::write(&token, TOKEN)?;
    let log = a.work.join("marco.log");
    let _ = std::fs::remove_file(&log);

    let fk = fake::start(cap.clone(), "127.0.0.1:0", a.rate).await?;
    let url = match cap.proto {
        Proto::Jetstream => format!("ws://{}/subscribe", fk.addr),
        Proto::JetstreamV2 => unreachable!("the harness has no Jetstream v2 mode"),
        Proto::Firehose => format!("ws://{}/xrpc/com.atproto.sync.subscribeRepos", fk.addr),
    };
    let src = Source::new(cap.proto, &url)?;
    let mut rng = Rng(a.seed.max(1));
    let t0 = Instant::now();
    let mut high = 0u64;
    let (mut readies, mut readyzs) = (Vec::new(), Vec::new());
    let mut modes = vec![When::Random];
    if a.segment_metric.is_some() {
        modes.extend([When::AfterSegment, When::BeforeSegment]);
    }
    if a.rewrite {
        modes.push(When::DuringRewrite);
    }
    if a.compaction {
        modes.push(When::DuringCompaction);
    }
    for i in 0..=a.kills {
        let last_run = i == a.kills;
        let mode = modes[i as usize % modes.len()];
        let mut m = spawn(&a, &got_snap, &url, &token, &log)?;
        let started = Instant::now();
        let random = Duration::from_millis(a.min_ms + rng.below(a.max_ms.saturating_sub(a.min_ms) + 1));
        // A segment comes only when there are writes, so once a capture is
        // used up a segment-timed run falls back to a kill at 20 s.
        let limit = match (last_run, mode) {
            (true, _) => a.catch_up,
            (false, When::Random) => random,
            (false, _) => Duration::from_secs(20),
        };
        let (mut ready, mut readyz, mut cursor) = (None, None, None);
        let (mut segs, mut seg_times): (Option<f64>, Vec<Instant>) = (None, Vec::new());
        let mut rewrite_at: Option<Instant> = None;
        let mut ckpts: Option<f64> = None;
        let mut kill_at: Option<Instant> = None;
        while started.elapsed() < limit && kill_at.is_none_or(|k| Instant::now() < k) {
            if let Some(st) = m.child.try_wait()? {
                bail!("marco exited on its own ({st}) in run {}; see {}", i + 1, log.display());
            }
            let body = crate::http_get(m.admin, "/metrics", Some(TOKEN)).await.unwrap_or_default();
            if readyz.is_none() && crate::http_get(m.admin, "/readyz", Some(TOKEN)).await.is_ok() {
                readyz = Some(started.elapsed());
            }
            if let Some((c, behind_us)) = progress(&body, &src.name) {
                cursor = Some(c);
                if ready.is_none() && c >= high && (behind_us < 1_000_000 || !fk.restamped()) {
                    ready = Some(started.elapsed());
                }
                if last_run && c >= fk.last_cursor() && (ready.is_some() && readyz.is_some() || !fk.restamped()) {
                    break;
                }
            }
            if !last_run {
                match mode {
                    When::Random => {}
                    When::AfterSegment | When::BeforeSegment => {
                        let name = a.segment_metric.as_deref().unwrap_or_default();
                        if let Some(n) = metric(&body, name) {
                            if segs.is_some_and(|p| n > p) {
                                seg_times.push(Instant::now());
                            }
                            segs = Some(n);
                        }
                        if mode == When::AfterSegment && !seg_times.is_empty() {
                            kill_at = Some(Instant::now());
                        }
                        if mode == When::BeforeSegment && seg_times.len() >= 2 && kill_at.is_none() {
                            let [x, y] = [seg_times[seg_times.len() - 2], seg_times[seg_times.len() - 1]];
                            kill_at = Some((y + (y - x)).checked_sub(Duration::from_millis(30)).unwrap_or(y));
                        }
                    }
                    When::DuringRewrite => {
                        // the admin listener answers once open is done, which on a cold engine takes a while
                        if rewrite_at.is_none()
                            && !body.is_empty()
                            && started.elapsed() >= random.min(Duration::from_millis(1500))
                        {
                            admin(m.admin).checkpoint(CheckpointRequest::default()).await.context("Checkpoint")?;
                            rewrite_at = Some(Instant::now());
                            kill_at = Some(Instant::now() + Duration::from_millis(rng.below(300)));
                        }
                    }
                    When::DuringCompaction => {
                        let done = metric(&body, "marco_checkpoint_seconds_count");
                        // the admin listener answers once open is done, which on a cold engine takes a while
                        if rewrite_at.is_none()
                            && !body.is_empty()
                            && started.elapsed() >= random.min(Duration::from_millis(1500))
                        {
                            admin(m.admin).checkpoint(CheckpointRequest::default()).await.context("Checkpoint")?;
                            rewrite_at = Some(Instant::now());
                            ckpts = done;
                        } else if rewrite_at.is_some() && kill_at.is_none() && done > ckpts {
                            kill_at = Some(Instant::now() + Duration::from_millis(rng.below(500)));
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(if mode == When::Random || last_run { 20 } else { 3 })).await;
        }
        let done = cursor.map_or(0, |c| fk.through(c));
        let ms = |r: Option<Duration>| r.map_or("-".to_string(), |r| format!("{:.0} ms", r.as_secs_f64() * 1e3));
        let ready_s = format!("{} (/readyz {})", ms(ready), ms(readyz));
        if let Some(r) = ready {
            readies.push(r.as_secs_f64());
        }
        if let Some(r) = readyz {
            readyzs.push(r.as_secs_f64());
        }
        if last_run {
            if cursor.is_none_or(|c| c < fk.last_cursor()) {
                bail!("marco didn't reach the capture's end in {:?}; see {}", a.catch_up, log.display());
            }
            println!("final run: caught up {ready_s}, applied through {done}/{} messages", cap.msgs.len());
            return finish(&a, &cap, fk, &uris, &dids, &want_snap, m, t0, [readies, readyzs]).await;
        }
        let mode = if mode != When::Random && kill_at.is_none() { When::Random } else { mode };
        kill9(m.child.id().context("no pid")?)?;
        m.child.wait().await?;
        high = high.max(cursor.unwrap_or(0));
        println!(
            "kill {:>3} {:<14} after {:>5} ms: caught up {ready_s}, applied through {done}/{} messages",
            i + 1,
            format!("{mode:?}"),
            started.elapsed().as_millis(),
            cap.msgs.len()
        );
    }
    unreachable!()
}

#[allow(clippy::too_many_arguments)]
async fn finish(
    a: &Args,
    cap: &Arc<Capture>,
    fk: Arc<fake::Fake>,
    uris: &BTreeSet<String>,
    dids: &BTreeSet<String>,
    want_snap: &Path,
    m: Marco,
    t0: Instant,
    readies: [Vec<f64>; 2],
) -> Result<bool> {
    println!(
        "caught up after {} kills in {:.1} s ({} connections to the fake, {} messages sent)",
        a.kills,
        t0.elapsed().as_secs_f64(),
        fk.stats.connections.load(std::sync::atomic::Ordering::Relaxed),
        fk.stats.sent.load(std::sync::atomic::Ordering::Relaxed)
    );
    for (what, mut v) in
        ["caught up (cursor past the pre-kill high-water, behind < 1 s)", "/readyz 200"].into_iter().zip(readies)
    {
        v.sort_by(f64::total_cmp);
        if v.is_empty() {
            println!("restart to {what}: never before a kill");
            continue;
        }
        let q = |p: f64| v[((v.len() - 1) as f64 * p) as usize] * 1e3;
        println!(
            "restart to {what}, {} of {} runs: p50 {:.0} ms, p90 {:.0} ms, max {:.0} ms",
            v.len(),
            a.kills + 1,
            q(0.5),
            q(0.9),
            q(1.0)
        );
    }
    let url = match cap.proto {
        Proto::Jetstream => "/subscribe",
        Proto::JetstreamV2 => unreachable!("the harness has no Jetstream v2 mode"),
        Proto::Firehose => "/xrpc/com.atproto.sync.subscribeRepos",
    };
    // its own fake, at full speed: the expected state is the whole capture, once
    let wf = fake::start(cap.clone(), "127.0.0.1:0", 0).await?;
    let want_src = Source::new(cap.proto, &format!("ws://{}{url}", wf.addr))?;
    let want = open_app(want_snap)?;
    // From the capture's start: a new source would otherwise start at now,
    // past a recorded capture's times.
    want.apply_from(&want_src.name, "1", &[]).await?;
    let rels = want.graph().relations().clone();
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    let (st, _) = marco_ingest::run(want_src, rels, want.clone(), Default::default(), stopped);
    wait_cursor(&st, wf.last_cursor(), a.catch_up).await?;
    let ui = Arc::new(marco_server::ui::Ui::load(None)?);
    let (_, want_admin) = marco_server::serve(want.clone(), ui).await?;

    let diffs = compare(want_admin, m.admin, uris, dids).await;
    drop(m);
    for (k, w, g) in diffs.iter().take(50) {
        println!("DIFF {k}\n  want {w}\n  got  {g}");
    }
    println!("{} keys compared, {} differ", uris.len() + dids.len(), diffs.len());
    Ok(diffs.is_empty())
}
