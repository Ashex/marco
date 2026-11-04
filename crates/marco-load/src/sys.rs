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
