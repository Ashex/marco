//! FORMAT 6's codecs against FORMAT 5's, on random lists: every read a
//! FORMAT 5 list answers, the FORMAT 6 one answers the same, in one round,
//! within the page bounds of storage.html §8.
//!
//! Lists are packed several to a column, as in a shard, and every column
//! page is registered as its own engine file. A pass over an engine missing
//! all of them names the pages a read needs; an engine holding only those
//! pages must then answer it in one pass.

use std::collections::BTreeSet;

use marco_core::format6::{Entry, Packer, Space, SpacePart, WHOLE};
use marco_core::inset::{self, InList, PagedIn};
use marco_core::outlog::{self, OutBytes, OutList, OutSpaces, PagedOut, SplitCols, BLOCK};
use marco_core::read::{Engine, Hold, ReadCx, R};
use marco_core::rkey::NON_TID;
use marco_core::snapshot::{AlignedPage, Bytes};
use marco_core::store::page::{FileId, PageId, PAGE};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

const SIZES: [usize; 12] = [1, 2, 8, 9, 127, 128, 129, 300, 4095, 4096, 4097, 9000];

/// Sorted unique rkeys, mostly TIDs with gaps of every size, some hashed
/// (non-TID), and subjects drawn from a universe small enough to repeat.
fn random_list(r: &mut Rng, n: usize) -> Vec<(u64, u32)> {
    let universe = match r.below(4) {
        0 => 3,
        1 => (n as u64 / 4).max(2),
        2 => n as u64 * 4,
        _ => u32::MAX as u64,
    };
    let mut keys = BTreeSet::new();
    let mut k = 0x0d00_0000_0000_0000u64 + r.below(1 << 40);
    let hashed = if r.below(3) == 0 { (n as u64).div_ceil(10) } else { 0 };
    while keys.len() < n - (hashed as usize).min(n - 1) {
        k += match r.below(5) {
            0 => 1,
            1 => r.below(1000) + 1,
            2 => r.below(1 << 20) + 1,
            3 => r.below(1 << 40) + 1,
            _ => r.below(1 << 50) + 1,
        };
        keys.insert(k);
    }
    while keys.len() < n {
        keys.insert(NON_TID | r.below(NON_TID));
    }
    keys.into_iter().map(|k| (k, r.below(universe) as u32)).collect()
}

fn random_set(r: &mut Rng, n: usize) -> Vec<u32> {
    let universe = if r.below(2) == 0 { n as u64 * 2 } else { u32::MAX as u64 };
    let mut s = BTreeSet::new();
    while s.len() < n {
        s.insert(r.below(universe) as u32);
    }
    s.into_iter().collect()
}

/// Every column page as its own engine file: `pages[i]` is space page i of
/// column `cols[i]`. `keep` says which pages the engine holds; the rest are
/// empty files, so reading them misses.
struct Pages {
    cols: Vec<Vec<AlignedPage>>,
}

impl Pages {
    fn spaces(&self) -> Vec<Space> {
        let mut file = 0u32;
        self.cols
            .iter()
            .map(|c| {
                let parts = (0..c.len() as u32)
                    .map(|i| {
                        file += 1;
                        SpacePart { first: i, pages: 1, file: FileId(file - 1), file_page: 0 }
                    })
                    .collect();
                Space::new(parts).unwrap()
            })
            .collect()
    }

    fn engine(&self, keep: impl Fn(PageId) -> bool) -> Engine {
        let e = Engine::new();
        let mut file = 0u32;
        for c in &self.cols {
            for p in c {
                let id = PageId::new(FileId(file), 0);
                let bytes = if keep(id) { p.0.to_vec() } else { Vec::new() };
                assert_eq!(e.add(format!("page-{file}"), Bytes::Owned(bytes)), FileId(file));
                file += 1;
            }
        }
        e
    }
}

/// Runs `f` on an engine missing every page, then on one holding exactly
/// the pages that pass named, which must answer in one pass. Returns the
/// answer and the pages read.
fn one_round<T>(pages: &Pages, f: impl Fn(&ReadCx<'_>) -> R<T>) -> (T, usize) {
    let none = pages.engine(|_| false);
    let need: BTreeSet<PageId> = match none.pass(&mut Hold::none(), &f) {
        Ok(t) => return (t, 0),
        Err(m) => m.pages().iter().collect(),
    };
    let some = pages.engine(|p| need.contains(&p));
    let t = some
        .pass(&mut Hold::none(), &f)
        .unwrap_or_else(|m| panic!("a second round: {:?} after {:?}", m.pages().iter().collect::<Vec<_>>(), need));
    (t, need.len())
}

/// Lists placed into shared columns, as a shard holds them.
struct Out {
    lists: Vec<Vec<(u64, u32)>>,
    entries: Vec<Entry>,
    head: Vec<u8>,
    pages: Pages,
    spaces: Vec<Space>,
    subj_index: usize,
}

impl Out {
    fn build(lists: Vec<Vec<(u64, u32)>>, subj_index: usize) -> Out {
        let (mut small, mut subj, mut rkey, mut bysubj) =
            (Packer::new(0), Packer::new(0), Packer::new(0), Packer::new(0));
        let mut head = Vec::new();
        let mut entries = Vec::new();
        let mut bytes = OutBytes::default();
        let mut buf = Vec::new();
        for l in &lists {
            if l.len() <= WHOLE {
                buf.clear();
                outlog::encode(l, subj_index, &mut buf, &mut bytes);
                entries.push(Entry::Small(small.put(&buf) as u32));
            } else {
                entries.push(Entry::Head(head.len() as u32));
                let cols = SplitCols { subj: &mut subj, rkey: &mut rkey, bysubj: &mut bysubj };
                outlog::encode_split(l, subj_index, cols, &mut head, &mut bytes).unwrap();
            }
        }
        assert_eq!(bytes.header + bytes.subjects + bytes.rkeys + bytes.bysubj + bytes.skip, {
            small.bytes + subj.bytes + rkey.bytes + bysubj.bytes + head.len() as u64
        });
        let pages = Pages { cols: [small, subj, rkey, bysubj].iter_mut().map(Packer::take_all).collect() };
        let spaces = pages.spaces();
        Out { lists, entries, head, pages, spaces, subj_index }
    }

    fn open<'g>(&'g self, cx: &ReadCx<'g>, i: usize) -> R<PagedOut<'g>> {
        let sp = OutSpaces { subj: &self.spaces[1], rkey: &self.spaces[2], bysubj: &self.spaces[3] };
        PagedOut::open(cx, self.entries[i], &self.spaces[0], &self.head, sp, self.subj_index)
    }
}

fn f5_out(l: &[(u64, u32)], subj_index: usize) -> Vec<u8> {
    let mut b = Vec::new();
    outlog::encode(l, subj_index, &mut b, &mut OutBytes::default());
    b
}

fn check_out(o: &Out, i: usize, r: &mut Rng) {
    let list = &o.lists[i];
    let n = list.len();
    let f5b = f5_out(list, o.subj_index);
    let f5 = OutList::parse(&f5b, o.subj_index);
    let big = n > WHOLE;
    let bysubj = big && n >= o.subj_index;
    let nb = n.div_ceil(BLOCK);

    let (got, pages) = one_round(&o.pages, |cx| o.open(cx, i)?.to_vec(cx));
    assert_eq!(got, f5.to_vec(), "to_vec, n {n}");
    assert!(pages <= if big { 2 * nb + 2 } else { 1 });
    let (got, _) = one_round(&o.pages, |cx| {
        let l = o.open(cx, i)?;
        Ok((l.len(), l.distinct(), l.blocks()))
    });
    assert_eq!(got, (f5.len(), f5.distinct(), nb));
    let (got, _) = one_round(&o.pages, |cx| {
        let mut v = Vec::new();
        o.open(cx, i)?.subjects_sorted(cx, &mut v)?;
        Ok(v)
    });
    let mut want = Vec::new();
    f5.subjects_sorted(&mut want);
    assert_eq!(got, want, "subjects_sorted");

    let mut probes: Vec<usize> = (0..12).map(|_| r.below(n as u64) as usize).collect();
    probes.extend([0, n - 1]);
    for &p in &probes {
        let (k, s) = list[p];
        let (got, pages) = one_round(&o.pages, |cx| o.open(cx, i)?.find_key(cx, k));
        assert_eq!(got, f5.find_key(k));
        assert_eq!(got, Some(p));
        assert!(pages <= 1, "find_key read {pages} pages");
        for miss in [k.wrapping_add(1), k.wrapping_sub(1), k ^ NON_TID] {
            assert_eq!(one_round(&o.pages, |cx| o.open(cx, i)?.find_key(cx, miss)).0, f5.find_key(miss));
        }
        let (got, pages) = one_round(&o.pages, |cx| {
            let l = o.open(cx, i)?;
            let (s, k) = (l.subject(cx, p), l.key_at(cx, p));
            Ok((s?, k?))
        });
        assert_eq!(got, (f5.subject(p), f5.key_at(p)));
        assert!(pages <= 2);
        for subject in [s, s.wrapping_add(1), u32::MAX, 0] {
            let stop = 1 + r.below(4) as usize;
            for asc in [false, true] {
                let collect = |cx: &ReadCx<'_>, limit: usize| -> R<Vec<usize>> {
                    let mut v = Vec::new();
                    let push = |p: usize| {
                        v.push(p);
                        v.len() < limit
                    };
                    let l = o.open(cx, i)?;
                    if asc {
                        l.for_subject_asc(cx, subject, push)?;
                    } else {
                        l.for_subject(cx, subject, push)?;
                    }
                    Ok(v)
                };
                let mut want = Vec::new();
                let push = |p: usize| {
                    want.push(p);
                    true
                };
                if asc {
                    f5.for_subject_asc(subject, push);
                } else {
                    f5.for_subject(subject, push);
                }
                let (got, pages) = one_round(&o.pages, |cx| collect(cx, usize::MAX));
                assert_eq!(got, want, "for_subject asc {asc} n {n}");
                if bysubj && want.len() <= 64 {
                    assert!(pages <= 2, "Has on a list with by-subject blocks read {pages} pages");
                }
                if big && !bysubj {
                    assert!(pages <= nb.div_ceil(8) + 1, "a subject scan read {pages} pages");
                }
                let (got, _) = one_round(&o.pages, |cx| collect(cx, stop));
                assert_eq!(got, want[..stop.min(want.len())], "for_subject stops early");
            }
        }
    }
    for b in 0..nb {
        let (got, pages) = one_round(&o.pages, |cx| {
            let l = o.open(cx, i)?;
            let mut keys = [0u64; BLOCK];
            let (k, s) = (l.block_keys(cx, b, &mut keys), l.block_subjects(cx, b));
            Ok((keys[..k?].to_vec(), s?.to_vec()))
        });
        let mut keys = [0u64; BLOCK];
        let k = f5.block_keys(b, &mut keys);
        assert_eq!(got, (keys[..k].to_vec(), f5.block_subjects(b).to_vec()), "block {b}");
        assert!(pages <= 2);
    }
    for _ in 0..6 {
        let before = match r.below(4) {
            0 => None,
            1 => Some(list[r.below(n as u64) as usize].0),
            2 => Some(list[r.below(n as u64) as usize].0 + 1),
            _ => Some(list[0].0),
        };
        let limit = match r.below(3) {
            0 => 1 + r.below(10) as usize,
            1 => 1 + r.below(300) as usize,
            _ => n + 5,
        };
        let mut want = Vec::new();
        f5.for_each_desc(before, |k, s| {
            want.push((k, s));
            want.len() < limit
        });
        let (got, pages) = one_round(&o.pages, |cx| {
            let mut v = Vec::new();
            o.open(cx, i)?.for_each_desc(cx, before, limit, |k, s| {
                v.push((k, s));
                v.len() < limit
            })?;
            Ok(v)
        });
        assert_eq!(got, want, "for_each_desc before {before:?} limit {limit}");
        if big {
            assert!(pages <= 2 * (limit.div_ceil(BLOCK) + 1), "ListOut limit {limit} read {pages} pages");
        }
    }
}

#[test]
fn out_lists_answer_as_format_5_does() {
    let mut r = Rng(0x9e37_79b9_7f4a_7c15);
    for subj_index in [4096, 129, 2] {
        for round in 0..3 {
            let mut lists: Vec<Vec<(u64, u32)>> = SIZES.iter().map(|&n| random_list(&mut r, n)).collect();
            for _ in 0..8 {
                let n = 1 + r.below(if round == 0 { 200 } else { 2000 }) as usize;
                lists.push(random_list(&mut r, n));
            }
            for i in (1..lists.len()).rev() {
                lists.swap(i, r.below(i as u64 + 1) as usize);
            }
            let o = Out::build(lists, subj_index);
            for i in 0..o.lists.len() {
                check_out(&o, i, &mut r);
            }
        }
    }
}

#[test]
fn out_list_edges() {
    // a last block of one record has no rkey bytes; keys at the u64 extremes
    let mut r = Rng(7);
    let tid = |i: u64| 0x0d00_0000_0000_0000u64 + i * 1_234_567;
    let lists = vec![
        (0..129u64).map(|i| (tid(i), 5)).collect::<Vec<_>>(),
        (0..257u64).map(|i| (tid(i), (i % 2) as u32)).collect(),
        (0..200u64).map(|i| (i, u32::MAX - i as u32)).chain([(NON_TID | 1, 0), (u64::MAX, u32::MAX)]).collect(),
        vec![(1, 5), (NON_TID | 3, 5)],
    ];
    for subj_index in [4096, 2] {
        let o = Out::build(lists.clone(), subj_index);
        for i in 0..o.lists.len() {
            check_out(&o, i, &mut r);
        }
    }
}

struct In {
    sets: Vec<Vec<u32>>,
    entries: Vec<Entry>,
    head: Vec<u8>,
    pages: Pages,
    spaces: Vec<Space>,
}

impl In {
    fn build(sets: Vec<Vec<u32>>) -> In {
        let (mut small, mut blocks) = (Packer::new(0), Packer::new(0));
        let mut head = Vec::new();
        let mut entries = Vec::new();
        let mut buf = Vec::new();
        for s in &sets {
            if s.len() <= WHOLE {
                buf.clear();
                inset::encode(s, &mut buf);
                entries.push(Entry::Small(small.put(&buf) as u32));
            } else {
                entries.push(Entry::Head(head.len() as u32));
                inset::encode_split(s, &mut blocks, &mut head).unwrap();
            }
        }
        let pages = Pages { cols: vec![small.take_all(), blocks.take_all()] };
        let spaces = pages.spaces();
        In { sets, entries, head, pages, spaces }
    }

    fn open<'g>(&'g self, cx: &ReadCx<'g>, i: usize) -> R<PagedIn<'g>> {
        PagedIn::open(cx, self.entries[i], &self.spaces[0], &self.head, &self.spaces[1])
    }
}

fn check_in(s: &In, i: usize, r: &mut Rng) {
    let ids = &s.sets[i];
    let n = ids.len();
    let mut f5b = Vec::new();
    inset::encode(ids, &mut f5b);
    let f5 = InList::parse(&f5b);
    let nb = n.div_ceil(BLOCK);
    let (got, pages) = one_round(&s.pages, |cx| {
        let l = s.open(cx, i)?;
        Ok((l.len(), l.to_vec(cx)?))
    });
    assert_eq!(got, (f5.len(), f5.to_vec()));
    assert!(pages <= nb.max(1));
    let mut xs: Vec<u32> = (0..16).map(|_| ids[r.below(n as u64) as usize]).collect();
    xs.extend(xs.clone().iter().map(|x| x.wrapping_add(1)));
    xs.extend([0, u32::MAX, ids[0], ids[n - 1]]);
    for x in xs {
        let (got, pages) = one_round(&s.pages, |cx| s.open(cx, i)?.contains(cx, x));
        assert_eq!(got, f5.contains(x), "contains {x}");
        assert!(pages <= 1, "contains read {pages} pages");
        let cap = if r.below(2) == 0 { 10 } else { 400 };
        let limit = 1 + r.below(cap) as usize;
        for after in [Some(x), None] {
            let mut want = Vec::new();
            f5.for_each_after(after, |v| {
                want.push(v);
                want.len() < limit
            });
            let (got, pages) = one_round(&s.pages, |cx| {
                let mut v = Vec::new();
                s.open(cx, i)?.for_each_after(cx, after, limit, |x| {
                    v.push(x);
                    v.len() < limit
                })?;
                Ok(v)
            });
            assert_eq!(got, want, "for_each_after {after:?} limit {limit}");
            assert!(pages <= limit.div_ceil(BLOCK) + 2, "ListIn limit {limit} read {pages} pages");
        }
    }
    for _ in 0..4 {
        let mut probes: Vec<u32> = (0..r.below(300) + 1)
            .map(|_| if r.below(2) == 0 { ids[r.below(n as u64) as usize] } else { r.below(u32::MAX as u64) as u32 })
            .collect();
        probes.sort_unstable();
        probes.dedup();
        let limit = if r.below(3) == 0 { 1 + r.below(5) as usize } else { usize::MAX };
        let mut want = Vec::new();
        f5.intersect_sorted(&probes, limit, &mut want);
        let (got, pages) = one_round(&s.pages, |cx| {
            let mut v = Vec::new();
            s.open(cx, i)?.intersect_sorted(cx, &probes, limit, &mut v)?;
            Ok(v)
        });
        assert_eq!(got, want, "intersect_sorted");
        assert!(pages <= probes.len().min(nb).max(1));
    }
}

#[test]
fn in_sets_answer_as_format_5_does() {
    let mut r = Rng(0x2545_f491_4f6c_dd1d);
    for round in 0..4 {
        let mut sets: Vec<Vec<u32>> = SIZES.iter().map(|&n| random_set(&mut r, n)).collect();
        for _ in 0..10 {
            let n = 1 + r.below(if round == 0 { 200 } else { 3000 }) as usize;
            sets.push(random_set(&mut r, n));
        }
        sets.push(vec![0]);
        sets.push(vec![u32::MAX - 1]);
        sets.push((0..129).collect());
        sets.push((0..257).map(|i| u32::MAX - 300 + i).collect());
        for i in (1..sets.len()).rev() {
            sets.swap(i, r.below(i as u64 + 1) as usize);
        }
        let s = In::build(sets);
        for i in 0..s.sets.len() {
            check_in(&s, i, &mut r);
        }
    }
}

#[test]
fn blocks_never_cross_pages() {
    let mut r = Rng(99);
    let mut p = Packer::new(0);
    let mut placed = Vec::new();
    for _ in 0..5000 {
        let len = match r.below(4) {
            0 => 0,
            1 => r.below(64) as usize + 1,
            2 => r.below(1800) as usize + 1,
            _ => PAGE - r.below(3) as usize,
        };
        let b: Vec<u8> = (0..len).map(|_| r.below(256) as u8).collect();
        let at = p.put(&b);
        assert!(b.is_empty() || at / PAGE as u64 == (at + len as u64 - 1) / PAGE as u64, "{len} at {at}");
        placed.push((at, b));
    }
    let pages = p.take_all();
    for (at, b) in placed {
        if b.is_empty() {
            continue;
        }
        let pg = &pages[(at / PAGE as u64) as usize].0;
        let off = (at % PAGE as u64) as usize;
        assert_eq!(&pg[off..off + b.len()], &b[..]);
    }
}

/// The paged writer on a real filesystem (the target dir, not a tmpfs):
/// O_DIRECT when the filesystem takes it, and either way the same file.
#[test]
fn the_paged_writer_writes_direct_where_it_can() {
    use marco_core::snapshot::{FileKind, PagedFooter, PagedWriter};
    let d = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let p = d.path().join("f.marco");
    let mut w = PagedWriter::with_salt(&p, FileKind::Base, 42).unwrap();
    eprintln!("O_DIRECT: {}", w.is_direct());
    let pages: Vec<AlignedPage> = (0..700u32)
        .map(|i| {
            let mut p = AlignedPage::ZERO;
            p.0[..4].copy_from_slice(&i.to_le_bytes());
            p
        })
        .collect();
    w.pinned("a", &[1, 2, 3]).unwrap();
    w.begin_paged("b", 0).unwrap();
    w.write_pages(&pages[..3]).unwrap();
    w.write_pages(&pages[3..]).unwrap();
    w.end_paged().unwrap();
    w.pinned("c", &vec![9u8; 5 * PAGE + 77]).unwrap();
    w.paged("d", 0, &[4u8; 10]).unwrap();
    let footer = w.finish("shard", &()).unwrap();
    let file = std::fs::read(&p).unwrap();
    assert_eq!(PagedFooter::read(&file).unwrap(), footer);
    footer.verify(&file).unwrap();
    let b = footer.section("b").unwrap();
    for i in [0u32, 2, 3, 699] {
        let off = b.off as usize + i as usize * PAGE;
        assert_eq!(&file[off..off + 4], &i.to_le_bytes());
    }
    assert_eq!(footer.pinned(&file, "c").unwrap(), &vec![9u8; 5 * PAGE + 77][..]);
}

#[test]
fn bytes_used_counts_each_decoded_block_once_per_pass() {
    // these spaces are built bare, so every page counts as Structure::Other
    let total = |u: [u64; 7]| u.iter().sum::<u64>();
    let mut r = Rng(0x5eed);
    let (big, small) = (random_set(&mut r, 9000), random_set(&mut r, 40));
    let s = In::build(vec![big.clone(), small.clone()]);
    let (mut blocks, mut head) = (Packer::new(0), Vec::new());
    inset::encode_split(&big, &mut blocks, &mut head).unwrap();
    let mut small_bytes = Vec::new();
    inset::encode(&small, &mut small_bytes);
    let e = s.pages.engine(|_| true);
    let used = |f: &dyn Fn(&ReadCx<'_>) -> R<()>| {
        let g = e.pin();
        let cx = ReadCx::new(&e, &g);
        f(&cx).unwrap();
        cx.bytes_used()
    };
    let u = used(&|cx| {
        for _ in 0..2 {
            s.open(cx, 0)?.to_vec(cx)?;
            s.open(cx, 0)?.contains(cx, big[4000])?;
        }
        Ok(())
    });
    assert_eq!(total(u), blocks.bytes, "every block once, though decoded twice or more");
    let u = used(&|cx| s.open(cx, 1)?.to_vec(cx).map(|_| ()));
    assert_eq!(total(u), small_bytes.len() as u64);
    let u = total(used(&|cx| s.open(cx, 0)?.contains(cx, big[4000] + 1).map(|_| ())));
    assert!(u > 0 && u < 200, "one block: {u}");
    let u = used(&|cx| match s.open(cx, 0)? {
        PagedIn::Split(l) => {
            use marco_core::read::Blocks;
            l.blocks().need(cx, 0..big.len().div_ceil(BLOCK))
        }
        PagedIn::Whole(_) => unreachable!(),
    });
    assert_eq!(u, [0; 7], "need() reads blocks without using them");

    let list = random_list(&mut r, 5000);
    let o = Out::build(vec![list.clone()], 64);
    let (mut subj, mut rkey, mut bysubj) = (Packer::new(0), Packer::new(0), Packer::new(0));
    let cols = SplitCols { subj: &mut subj, rkey: &mut rkey, bysubj: &mut bysubj };
    outlog::encode_split(&list, 64, cols, &mut Vec::new(), &mut OutBytes::default()).unwrap();
    let e = o.pages.engine(|_| true);
    let g = e.pin();
    let cx = ReadCx::new(&e, &g);
    o.open(&cx, 0).unwrap().to_vec(&cx).unwrap();
    assert_eq!(total(cx.bytes_used()), subj.bytes + rkey.bytes);
    o.open(&cx, 0).unwrap().subjects_sorted(&cx, &mut Vec::new()).unwrap();
    assert_eq!(total(cx.bytes_used()), subj.bytes + rkey.bytes + bysubj.bytes);
    drop(cx);
    drop(g);

    // the engine adds a request's clean passes, not a job's or a missing one
    assert_eq!(e.stats().bytes_used, [0; 7]);
    e.pass(&mut Hold::none(), |cx| o.open(cx, 0)?.to_vec(cx).map(|_| ())).unwrap();
    let after = e.stats().bytes_used;
    assert_eq!(total(after), subj.bytes + rkey.bytes);
    e.pass(&mut Hold::new(), |cx| o.open(cx, 0)?.to_vec(cx).map(|_| ())).unwrap();
    assert_eq!(e.stats().bytes_used, after, "a job's pass isn't a request's");
    let none = o.pages.engine(|_| false);
    assert!(none.pass(&mut Hold::none(), |cx| o.open(cx, 0)?.to_vec(cx).map(|_| ())).is_err());
    assert_eq!(none.stats().bytes_used, [0; 7], "a pass that missed decoded nothing it serves");
}
