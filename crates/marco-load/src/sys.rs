//! Process stats from /proc.

fn status_kb(field: &str) -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with(field))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// VmHWM: the resident peak since the last reset.
pub fn peak_rss_gb() -> f64 {
    status_kb("VmHWM:").map_or(0.0, |kb| kb as f64 / 1e6)
}

pub fn rss_gb() -> f64 {
    status_kb("VmRSS:").map_or(0.0, |kb| kb as f64 / 1e6)
}

/// Resets VmHWM to the current RSS, so each phase reports its own peak.
pub fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// User + system CPU seconds of this process so far.
pub fn cpu_secs() -> f64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/stat") else { return 0.0 };
    // the fields after the parenthesised command name; utime and stime are the 12th and 13th
    let f: Vec<&str> = s.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    let tick = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    (tick(11) + tick(12)) / 100.0
}

/// Raises the soft open-file limit to the hard one, and returns it.
pub fn raise_nofile() -> u64 {
    let mut r = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: getrlimit and setrlimit only read and write `r`.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) != 0 {
            return 0;
        }
        if r.rlim_cur < r.rlim_max {
            let want = libc::rlimit { rlim_cur: r.rlim_max, rlim_max: r.rlim_max };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &want) == 0 {
                r.rlim_cur = r.rlim_max;
            }
        }
    }
    r.rlim_cur
}
