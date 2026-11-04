//! Process and page-cache figures from /proc and mincore(2).

pub struct Proc {
    pub rss_bytes: u64,
    pub cpu_seconds: f64,
    pub threads: u32,
}

pub fn proc_self() -> Proc {
    let page = page_size();
    let rss_bytes = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |p| p * page);
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // the fields after the command name, which may hold spaces and parens
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let tick = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let num = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    // utime and stime are fields 14 and 15 of stat, 12 and 13 after the name and state
    let cpu_seconds = (num(11) + num(12)) / tick;
    let threads = num(17) as u32;
    Proc { rss_bytes, cpu_seconds, threads }
}

pub fn mem_total() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            let l = s.lines().find(|l| l.starts_with("MemTotal:"))?;
            l.split_whitespace().nth(1)?.parse::<u64>().ok()
        })
        .map_or(0, |kb| kb * 1024)
}

fn page_size() -> u64 {
    (unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).max(4096) as u64
}

/// Bytes of a mapping that are in the page cache.
pub fn resident(bytes: &[u8]) -> u64 {
    if bytes.is_empty() {
        return 0;
    }
    let page = page_size() as usize;
    let start = bytes.as_ptr() as usize & !(page - 1);
    let len = bytes.as_ptr() as usize + bytes.len() - start;
    let mut vec = vec![0u8; len.div_ceil(page)];
    let r = unsafe { libc::mincore(start as *mut libc::c_void, len, vec.as_mut_ptr()) };
    if r != 0 {
        return 0;
    }
    vec.iter().filter(|&&b| b & 1 == 1).count() as u64 * page as u64
}
