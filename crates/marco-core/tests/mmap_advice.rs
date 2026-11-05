//! `set_mmap_random` marks snapshot mappings MADV_RANDOM (VmFlags `rr` in smaps); off, they aren't.

use marco_core::{snapshot::set_mmap_random, Graph, Relations};

fn flags_of(file: &str) -> Vec<String> {
    let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
    let mut out = Vec::new();
    let mut inside = false;
    for line in smaps.lines() {
        if line.contains(file) {
            inside = true;
        } else if inside && line.starts_with("VmFlags:") {
            out.push(line.to_string());
            inside = false;
        }
    }
    out
}

#[cfg(target_os = "linux")]
#[test]
fn random_advice_reaches_the_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut g = Graph::new(Relations::bsky_default());
    let a = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let b = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    g.add(&format!("at://{a}/app.bsky.graph.follow/3jwdwj2ctlk26"), b).unwrap();
    g.write_snapshot(&dir.path().join("plain.marco")).unwrap();
    g.write_snapshot(&dir.path().join("random.marco")).unwrap();

    set_mmap_random(false);
    let plain = Graph::open(&dir.path().join("plain.marco")).unwrap();
    set_mmap_random(true);
    let random = Graph::open(&dir.path().join("random.marco")).unwrap();
    set_mmap_random(false);

    let (p, r) = (flags_of("plain.marco"), flags_of("random.marco"));
    assert!(!p.is_empty() && !r.is_empty(), "both files are mapped");
    assert!(p.iter().all(|f| !f.split_whitespace().any(|x| x == "rr")), "{p:?}");
    assert!(r.iter().all(|f| f.split_whitespace().any(|x| x == "rr")), "{r:?}");
    drop((plain, random));
}
