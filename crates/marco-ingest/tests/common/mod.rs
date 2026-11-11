//! A sink in memory, shared by the consumer tests.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use marco_ingest::{Op, Sink, Status};

#[derive(Default)]
pub struct Mem {
    pub edges: Mutex<BTreeMap<String, String>>,
    pub statuses: Mutex<HashMap<String, Status>>,
    pub cursors: Mutex<HashMap<String, String>>,
    /// (cursor, what) per sink call, in order.
    pub calls: Mutex<Vec<(u64, String)>>,
}

impl Sink for Mem {
    async fn apply_from(&self, source: &str, cursor: &str, ops: &[Op]) -> anyhow::Result<()> {
        let mut e = self.edges.lock().unwrap();
        for op in ops {
            match op {
                Op::Add { uri, subject, .. } => {
                    e.entry(uri.clone()).or_insert_with(|| subject.clone());
                }
                Op::Delete { uri, .. } => {
                    e.remove(uri);
                }
            }
        }
        self.cursors.lock().unwrap().insert(source.into(), cursor.into());
        self.calls.lock().unwrap().push((cursor.parse().unwrap(), format!("{} ops", ops.len())));
        Ok(())
    }

    async fn status_from(
        &self,
        source: &str,
        cursor: &str,
        did: &str,
        s: Option<Status>,
        _: i64,
    ) -> anyhow::Result<u64> {
        match s {
            Some(s) => self.statuses.lock().unwrap().insert(did.into(), s),
            None => self.statuses.lock().unwrap().remove(did),
        };
        self.cursors.lock().unwrap().insert(source.into(), cursor.into());
        self.calls.lock().unwrap().push((cursor.parse().unwrap(), format!("status {did} {s:?}")));
        Ok(0)
    }

    fn source_cursor(&self, source: &str) -> Option<String> {
        self.cursors.lock().unwrap().get(source).cloned()
    }

    fn has_status(&self, did: &str) -> bool {
        self.statuses.lock().unwrap().contains_key(did)
    }
}

pub fn now_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as u64
}

pub const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
pub const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
pub const C: &str = "did:plc:cccccccccccccccccccccccc";

impl Mem {
    pub fn source_cursor_num(&self) -> u64 {
        self.cursors.lock().unwrap().values().next().map_or(0, |c| c.parse().unwrap())
    }
}
