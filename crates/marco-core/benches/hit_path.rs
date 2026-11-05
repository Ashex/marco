//! The hit path: one page lookup in a pass whose pages are all in the cache
//! (the phase gate is 15 ns), and a whole one-page pass on a fast path.
//!
//!     cargo bench -p marco-core --bench hit_path

#[path = "../tests/support/mod.rs"]
mod support;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use marco_core::read::{Hold, ReadCx};
use marco_core::store::engine::Engine;
use marco_core::store::io::Plain;
use marco_core::store::page::PageId;

const PAGES: u32 = 1 << 14;

fn hit_path(c: &mut Criterion) {
    let d = tempfile::tempdir().unwrap();
    let path = support::write_tagged(d.path(), "f.marco", 1, PAGES);
    let e = Engine::with_config(support::config(256 << 20, None, Arc::new(Plain))).unwrap();
    let f = e.register(&path).unwrap();
    let ids: Vec<PageId> = (0..PAGES).map(|p| PageId::new(f.id(), p)).collect();
    for c in ids.chunks(1024) {
        e.read_blocking(&mut Hold::new(), |cx| {
            for id in c {
                cx.page(*id)?;
            }
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(e.stats().fetched, PAGES as u64);

    let guard = e.pin();
    let cx = ReadCx::new(&e, &guard);
    let mut i = 0usize;
    c.bench_function("page lookup, hit, random page", |b| {
        b.iter(|| {
            i = (i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) % PAGES as usize;
            black_box(cx.page(black_box(ids[i])).is_ok())
        })
    });
    let mut i = 0usize;
    c.bench_function("page lookup, hit, same page", |b| b.iter(|| black_box(cx.page(black_box(ids[i & 7])).is_ok())));
    drop(cx);
    drop(guard);

    c.bench_function("one-page pass, fast path", |b| {
        b.iter(|| {
            i = (i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) % PAGES as usize;
            black_box(e.pass(&mut Hold::none(), |cx| cx.page(ids[i]).map(|p| p[0])).is_ok())
        })
    });
}

criterion_group!(benches, hit_path);
criterion_main!(benches);
