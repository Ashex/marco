//! One repo's CAR into build input rows: the commit names the MST root, and
//! the walk visits only subtrees whose key range can hold a scanned
//! collection, so a repo's posts and likes are never decoded.

use std::collections::{BTreeMap, HashMap};

use vlatproto::cbor::ValueRef;
use vlatproto::cid::Cid;
use vlatproto::mst::{self, Entry};

use crate::build::own_list;
use crate::rows::put_row;

/// A scanned relation: its collection and where the record keeps its subject and list.
pub struct Wanted {
    /// `<collection>/`, the MST key prefix of its records.
    pub prefix: Vec<u8>,
    pub subject: String,
    pub list: Option<String>,
}

impl Wanted {
    pub fn of(rels: &marco_core::Relations) -> Vec<Wanted> {
        rels.user()
            .into_iter()
            .map(|id| {
                let r = rels.get(id);
                Wanted {
                    prefix: format!("{}/", r.collection).into_bytes(),
                    subject: r.subject.clone(),
                    list: r.list.clone(),
                }
            })
            .collect()
    }
}

/// What one repo gave: rows per scanned relation, in the order of [`Wanted::of`].
#[derive(Default)]
pub struct RepoRows {
    pub rows: Vec<Vec<u8>>,
    pub records: Vec<u64>,
    /// Records of a scanned collection left out, by reason.
    pub skipped: BTreeMap<&'static str, u64>,
    /// MST nodes decoded: the walk skips the rest.
    pub nodes: u64,
}

/// Why a whole repo was skipped.
pub type Bad = &'static str;

pub(crate) fn path_of<'a>(record: &ValueRef<'a>, path: &str) -> Option<&'a str> {
    path.split('.').try_fold(record, |v, k| v.get(k))?.as_str()
}

/// The keys between exclusive bounds `lo` and `hi` can include one with `prefix`:
/// those keys are in [prefix, prefix with its '/' turned into '0').
fn may_hold(lo: Option<&[u8]>, hi: Option<&[u8]>, prefix: &[u8]) -> bool {
    let mut end = prefix.to_vec();
    *end.last_mut().unwrap() = b'0';
    hi.is_none_or(|h| h > prefix) && lo.is_none_or(|l| l < &end[..])
}

pub fn extract(did: &str, car: &[u8], wanted: &[Wanted]) -> Result<RepoRows, Bad> {
    let (roots, blocks) = vlatproto::car::read_car(car).map_err(|_| "bad_car")?;
    let blocks: HashMap<Cid, &[u8]> = blocks.into_iter().collect();
    let commit = roots.first().and_then(|c| blocks.get(c)).ok_or("no_commit")?;
    let commit = ValueRef::decode(commit).map_err(|_| "bad_commit")?;
    if commit.get("did").and_then(|d| d.as_str()) != Some(did) {
        return Err("did_mismatch");
    }
    let Some(ValueRef::Link(root)) = commit.get("data") else { return Err("bad_commit") };
    let mut out = RepoRows {
        rows: vec![Vec::new(); wanted.len()],
        records: vec![0; wanted.len()],
        skipped: BTreeMap::new(),
        nodes: 0,
    };
    let mut walk = Walk { blocks: &blocks, wanted, nodes: 0 };
    walk.node(*root, None, None, 0, &mut |key, val| {
        let Some(w) = wanted.iter().position(|w| key.starts_with(&w.prefix)) else { return };
        let mut skip = |why| *out.skipped.entry(why).or_default() += 1;
        let rkey = &key[wanted[w].prefix.len()..];
        let Some(data) = blocks.get(&val) else { return skip("missing_block") };
        if !vlatproto::car::block_matches(&val, data) {
            return skip("bad_block");
        }
        let Ok(rec) = ValueRef::decode(data) else { return skip("bad_record") };
        let Some(subject) = path_of(&rec, &wanted[w].subject) else { return skip("no_subject") };
        let list = match &wanted[w].list {
            None => None,
            Some(p) => match path_of(&rec, p) {
                None => return skip("no_list"),
                Some(l) => match own_list(did.as_bytes(), l.as_bytes()) {
                    Some(l) => Some(l),
                    // the live path keeps an item only on a list in its own repo
                    None if marco_core::relation::parse_uri(l).is_some() => return skip("cross_repo"),
                    None => return skip("bad_list"),
                },
            },
        };
        put_row(&mut out.rows[w], did.as_bytes(), rkey, subject.as_bytes(), list.map(str::as_bytes), 0, 1);
        out.records[w] += 1;
    })?;
    out.nodes = walk.nodes;
    Ok(out)
}

struct Walk<'a> {
    blocks: &'a HashMap<Cid, &'a [u8]>,
    wanted: &'a [Wanted],
    nodes: u64,
}

impl Walk<'_> {
    /// Calls `f` with every entry of the subtree at `cid` whose keys lie
    /// between `lo` and `hi`, skipping children that can't hold a wanted key.
    fn node(
        &mut self,
        cid: Cid,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        depth: usize,
        f: &mut dyn FnMut(&[u8], Cid),
    ) -> Result<(), Bad> {
        if depth > mst::MAX_DEPTH {
            return Err("mst_too_deep");
        }
        let data = self.blocks.get(&cid).ok_or("missing_node")?;
        let node = mst::decode_node(data, cid).map_err(|_| "bad_node")?;
        self.nodes += 1;
        let key = |e: &Entry| match e {
            Entry::Value { key, .. } => Some(key.clone()),
            Entry::Child { .. } => None,
        };
        let mut prev: Option<std::sync::Arc<[u8]>> = None;
        for (i, e) in node.entries.iter().enumerate() {
            match e {
                Entry::Value { key, val } => {
                    f(key, *val);
                    prev = Some(key.clone());
                }
                Entry::Child { cid: Some(c), .. } => {
                    let next = node.entries[i + 1..].iter().find_map(key);
                    let (l, h) = (prev.as_deref().or(lo), next.as_deref().or(hi));
                    if self.wanted.iter().any(|w| may_hold(l, h, &w.prefix)) {
                        self.node(*c, l, h, depth + 1, f)?;
                    }
                }
                Entry::Child { cid: None, .. } => return Err("bad_node"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use vlatproto::cbor::Value;

    /// A repo's CAR as a PDS serves it: the commit, every MST node, the records.
    pub fn car(did: &str, records: &[(String, Value)]) -> Vec<u8> {
        let mut tree = mst::Tree::new();
        let mut blocks: Vec<(Cid, Vec<u8>)> = Vec::new();
        for (key, rec) in records {
            let b = rec.to_cbor();
            let c = Cid::dag_cbor(&b);
            tree.insert(key.as_bytes(), c).unwrap();
            blocks.push((c, b));
        }
        let mut nodes = Vec::new();
        let root = tree.write_diff_blocks(&mut nodes).unwrap();
        NODES.set(nodes.len() as u64);
        let commit = map(vec![
            ("data".into(), Value::Link(root)),
            ("did".into(), Value::Text(did.into())),
            ("prev".into(), Value::Null),
            ("rev".into(), Value::Text("3lbmzx5qng32v".into())),
            ("sig".into(), Value::Bytes(vec![0; 64])),
            ("version".into(), Value::Int(3)),
        ])
        .to_cbor();
        let cc = Cid::dag_cbor(&commit);
        let mut out = Vec::new();
        vlatproto::car::write_header(&mut out, &cc);
        vlatproto::car::write_block(&mut out, &cc, &commit);
        for (c, b) in nodes.iter().chain(&blocks) {
            vlatproto::car::write_block(&mut out, c, b);
        }
        out
    }

    thread_local! {
        /// The MST nodes of the last CAR [`car`] made.
        pub static NODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    pub fn record(ty: &str, fields: &[(&str, &str)]) -> Value {
        let mut m: Vec<(String, Value)> = vec![("$type".into(), Value::Text(ty.into()))];
        m.extend(fields.iter().map(|(k, v)| (k.to_string(), Value::Text(v.to_string()))));
        m.push(("createdAt".into(), Value::Text("2024-01-01T00:00:00.000Z".into())));
        map(m)
    }

    fn map(mut m: Vec<(String, Value)>) -> Value {
        m.sort_by(|a, b| vlatproto::cbor::key_cmp(&a.0, &b.0));
        Value::Map(m)
    }

    fn rows(raw: &[u8], cols: usize) -> Vec<Vec<String>> {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), zstd::bulk::compress(raw, 1).unwrap()).unwrap();
        let mut r = crate::rows::Rows::open(tmp.path()).unwrap();
        let mut c = vec![Vec::new(); cols];
        let mut out = Vec::new();
        while r.next(&mut c).unwrap() {
            assert_eq!((r.i64().unwrap(), r.u8().unwrap()), (0, 1));
            out.push(c.iter().map(|x| String::from_utf8(x.clone()).unwrap()).collect());
        }
        out
    }

    #[test]
    fn keeps_the_relations_records_and_skips_the_rest() {
        let me = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let them = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let mut recs = Vec::new();
        // enough posts and likes that the tree has internal nodes to prune
        for i in 0..3000 {
            let k = vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + i * 1000, 0).to_string();
            recs.push((format!("app.bsky.feed.post/{k}"), record("app.bsky.feed.post", &[("text", "hi")])));
            recs.push((format!("app.bsky.feed.like/{k}"), record("app.bsky.feed.like", &[("subject", them)])));
        }
        for i in 0..50 {
            let k = vlatproto::tid::Tid::from_parts(1_710_000_000_000_000 + i * 1000, 0).to_string();
            recs.push((format!("app.bsky.graph.follow/{k}"), record("app.bsky.graph.follow", &[("subject", them)])));
        }
        recs.push(("app.bsky.graph.block/3kaaaaaaaaa22".into(), record("app.bsky.graph.block", &[("subject", them)])));
        recs.push(("app.bsky.graph.block/3kaaaaaaaaa23".into(), record("app.bsky.graph.block", &[])));
        let own = format!("at://{me}/app.bsky.graph.list/l1");
        let theirs = format!("at://{them}/app.bsky.graph.list/l1");
        recs.push((
            "app.bsky.graph.listitem/3kaaaaaaaaa24".into(),
            record("app.bsky.graph.listitem", &[("subject", them), ("list", &own)]),
        ));
        recs.push((
            "app.bsky.graph.listitem/3kaaaaaaaaa25".into(),
            record("app.bsky.graph.listitem", &[("subject", them), ("list", &theirs)]),
        ));
        recs.push((
            "app.bsky.graph.listblock/3kaaaaaaaaa26".into(),
            record("app.bsky.graph.listblock", &[("subject", &theirs)]),
        ));
        let car = car(me, &recs);
        let wanted = Wanted::of(&marco_core::Relations::bsky_default());
        let r = extract(me, &car, &wanted).unwrap();
        assert_eq!(r.records, [50, 1, 1, 1]);
        let all = NODES.get();
        assert!(all > 100 && r.nodes * 4 < all, "decoded {} of {all} nodes", r.nodes);
        assert_eq!(r.skipped, BTreeMap::from([("no_subject", 1), ("cross_repo", 1)]));
        let f = rows(&r.rows[0], 3);
        assert_eq!(f.len(), 50);
        assert!(f.iter().all(|x| x[0] == me && x[2] == them));
        assert_eq!(rows(&r.rows[1], 3), [[me, "3kaaaaaaaaa22", them]]);
        assert_eq!(rows(&r.rows[2], 4), [[me, "3kaaaaaaaaa24", them, own.as_str()]]);
        assert_eq!(
            rows(&r.rows[3], 3),
            [[me, "3kaaaaaaaaa26", theirs.as_str()]],
            "a listblock names a list in any repo"
        );
        assert_eq!(extract(them, &car, &wanted).err(), Some("did_mismatch"));
        assert_eq!(extract(me, &car[..car.len() / 2], &wanted).err(), Some("bad_car"));
    }

    #[test]
    fn prunes_by_key_range() {
        let p = b"app.bsky.graph.follow/";
        assert!(may_hold(None, None, p));
        assert!(may_hold(Some(b"app.bsky.feed.post/x"), Some(b"app.bsky.graph.follow/3k"), p));
        assert!(!may_hold(Some(b"app.bsky.feed.like/x"), Some(b"app.bsky.feed.post/x"), p));
        assert!(!may_hold(Some(b"app.bsky.graph.follow0"), None, p));
        assert!(!may_hold(None, Some(b"app.bsky.graph.follow/"), p));
        assert!(may_hold(Some(b"app.bsky.graph.follow/zz"), None, p));
    }
}
