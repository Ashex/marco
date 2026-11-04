//! Relations come from config: any record whose subject is a DID.
//!
//! ```toml
//! [[relation]]
//! name = "follow"
//! collection = "app.bsky.graph.follow"
//! subject = "subject"
//! ```

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub type RelId = u8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub collection: String,
    /// Dotted path to the subject DID in the record.
    pub subject: String,
}

impl Relation {
    pub fn subject_of<'a>(&self, record: &'a serde_json::Value) -> Option<&'a str> {
        self.subject.split('.').try_fold(record, |v, k| v.get(k))?.as_str()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Relations {
    #[serde(rename = "relation")]
    pub list: Vec<Relation>,
}

impl Relations {
    pub fn new(list: Vec<Relation>) -> Result<Relations> {
        let r = Relations { list };
        r.validate()?;
        Ok(r)
    }

    pub fn parse_toml(s: &str) -> Result<Relations> {
        let r: Relations = toml::from_str(s).context("relations config")?;
        r.validate()?;
        Ok(r)
    }

    /// follow and block, the relations an appview needs first.
    pub fn bsky_default() -> Relations {
        let rel = |name: &str, collection: &str| Relation {
            name: name.into(),
            collection: collection.into(),
            subject: "subject".into(),
        };
        Relations { list: vec![rel("follow", "app.bsky.graph.follow"), rel("block", "app.bsky.graph.block")] }
    }

    fn validate(&self) -> Result<()> {
        if self.list.len() > RelId::MAX as usize {
            bail!("at most {} relations", RelId::MAX);
        }
        for (i, r) in self.list.iter().enumerate() {
            if !vlatproto::syntax::valid_nsid(&r.collection) {
                bail!("relation {}: {:?} isn't an NSID", r.name, r.collection);
            }
            if r.subject.is_empty() || r.name.is_empty() {
                bail!("relation {i}: name and subject are required");
            }
            if self.list[..i].iter().any(|o| o.name == r.name || o.collection == r.collection) {
                bail!("relation {}: name and collection must be unique", r.name);
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn get(&self, rel: RelId) -> &Relation {
        &self.list[rel as usize]
    }

    pub fn by_collection(&self, collection: &str) -> Option<RelId> {
        self.list.iter().position(|r| r.collection == collection).map(|i| i as RelId)
    }

    pub fn by_name(&self, name: &str) -> Option<RelId> {
        self.list.iter().position(|r| r.name == name).map(|i| i as RelId)
    }
}

/// `at://{did}/{collection}/{rkey}`, split.
pub fn parse_uri(uri: &str) -> Option<(&str, &str, &str)> {
    let rest = uri.strip_prefix("at://")?;
    let mut it = rest.splitn(3, '/');
    let (did, coll, rkey) = (it.next()?, it.next()?, it.next()?);
    (vlatproto::syntax::valid_did(did) && vlatproto::syntax::valid_rkey(rkey) && !coll.is_empty())
        .then_some((did, coll, rkey))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_paths() {
        let r = Relations::parse_toml(
            r#"
[[relation]]
name = "follow"
collection = "app.bsky.graph.follow"
subject = "subject"

[[relation]]
name = "verify"
collection = "app.bsky.graph.verification"
subject = "subject"

[[relation]]
name = "nested"
collection = "com.example.thing"
subject = "target.did"
"#,
        )
        .unwrap();
        assert_eq!(r.by_collection("app.bsky.graph.verification"), Some(1));
        let rec = serde_json::json!({"target": {"did": "did:plc:x"}});
        assert_eq!(r.get(2).subject_of(&rec), Some("did:plc:x"));
        assert_eq!(r.get(0).subject_of(&rec), None);
        assert!(Relations::parse_toml("[[relation]]\nname='a'\ncollection='nope'\nsubject='s'").is_err());
    }

    #[test]
    fn uris() {
        assert_eq!(
            parse_uri("at://did:plc:abc/app.bsky.graph.follow/3lab2xyz7qk2c"),
            Some(("did:plc:abc", "app.bsky.graph.follow", "3lab2xyz7qk2c"))
        );
        assert_eq!(parse_uri("at://did:plc:abc/app.bsky.graph.follow"), None);
        assert_eq!(parse_uri("at://handle.test/app.bsky.graph.follow/x"), None);
    }
}
