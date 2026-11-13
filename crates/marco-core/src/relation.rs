//! Relations come from config: any record whose subject is a DID.
//!
//! ```toml
//! [[relation]]
//! name = "follow"
//! collection = "app.bsky.graph.follow"
//! subject = "subject"
//!
//! [[relation]]
//! name = "listitem"
//! collection = "app.bsky.graph.listitem"
//! subject = "subject"
//! list = "list"
//! ```
//!
//! A relation with `subject_kind = "list"` names a list instead of an
//! account: its subject is the list's AT URI, in any repo, and the edge goes
//! from the actor to the list's node (`listblock`: blocking everyone on a
//! moderation list).
//!
//! A relation with `newest_in = true` also keeps each subject's records
//! newest first, in a companion `<name>.by`: the subject's followers in the
//! order they followed. Its records are the relation's, held by subject.
//!
//! A relation with `list` keeps edges from that record (a list, in the
//! actor's own repo) to the subject: the list is a node in the directory,
//! keyed by its AT URI. A delete names only the actor and rkey, so each such
//! relation gets a companion, `<name>.owner`, holding the records as actor
//! -> list. Companions are added here, never configured.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub type RelId = u8;

/// What a relation's subject names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubjectKind {
    /// An account, by DID.
    #[default]
    Did,
    /// A list, by its record's AT URI: a node in the directory.
    List,
}

impl SubjectKind {
    fn is_did(&self) -> bool {
        *self == SubjectKind::Did
    }

    /// Whether `s` is a subject of this kind.
    pub fn admits(self, s: &str) -> bool {
        match self {
            SubjectKind::Did => crate::did::classify(s).is_some(),
            SubjectKind::List => s.starts_with("at://") && parse_uri(s).is_some(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub collection: String,
    /// Dotted path to the subject DID in the record.
    pub subject: String,
    /// Dotted path to an AT URI in the record, of a record in the same repo:
    /// edges go from that node rather than from the actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<String>,
    /// Set on a companion: the list relation whose records it holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_of: Option<String>,
    #[serde(default, skip_serializing_if = "SubjectKind::is_did")]
    pub subject_kind: SubjectKind,
    /// Keep each subject's records newest first too, in a `<name>.by` companion.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub newest_in: bool,
    /// Set on a `.by` companion: the relation whose records it holds by subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse_of: Option<String>,
}

impl Relation {
    pub fn subject_of<'a>(&self, record: &'a serde_json::Value) -> Option<&'a str> {
        path_of(&self.subject, record)
    }

    pub fn list_of<'a>(&self, record: &'a serde_json::Value) -> Option<&'a str> {
        path_of(self.list.as_deref()?, record)
    }

    /// Whether `s` is the kind of subject this relation names.
    pub fn valid_subject(&self, s: &str) -> bool {
        self.subject_kind.admits(s)
    }
}

fn path_of<'a>(path: &str, record: &'a serde_json::Value) -> Option<&'a str> {
    path.split('.').try_fold(record, |v, k| v.get(k))?.as_str()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Relations {
    #[serde(rename = "relation")]
    pub list: Vec<Relation>,
}

impl Relations {
    /// The configured relations, each list relation followed by its
    /// companion. Companions in `list` (a snapshot's) are made again.
    pub fn new(list: Vec<Relation>) -> Result<Relations> {
        let user: Vec<Relation> = list.into_iter().filter(|r| r.owner_of.is_none() && r.reverse_of.is_none()).collect();
        let r = Relations { list: user };
        r.validate()?;
        let mut all = Vec::with_capacity(r.list.len() + 1);
        for x in r.list {
            let owner = x.list.as_ref().map(|path| Relation {
                name: format!("{}.owner", x.name),
                collection: x.collection.clone(),
                subject: path.clone(),
                list: None,
                owner_of: Some(x.name.clone()),
                subject_kind: SubjectKind::List,
                newest_in: false,
                reverse_of: None,
            });
            all.push(x);
            all.extend(owner);
        }
        // last, so adding one moves no other relation's id
        let reverse: Vec<Relation> = all
            .iter()
            .filter(|x| x.newest_in)
            .map(|x| Relation {
                name: format!("{}.by", x.name),
                collection: x.collection.clone(),
                subject: x.subject.clone(),
                list: None,
                owner_of: None,
                subject_kind: SubjectKind::Did,
                newest_in: false,
                reverse_of: Some(x.name.clone()),
            })
            .collect();
        all.extend(reverse);
        if all.len() > RelId::MAX as usize {
            bail!("at most {} relations, companions included", RelId::MAX);
        }
        let r = Relations { list: all };
        if r.list.iter().enumerate().any(|(i, x)| r.list[..i].iter().any(|o| o.name == x.name)) {
            bail!("relation names must be unique, companions' `<name>.owner` included");
        }
        Ok(r)
    }

    pub fn parse_toml(s: &str) -> Result<Relations> {
        let r: Relations = toml::from_str(s).context("relations config")?;
        Relations::new(r.list)
    }

    /// follow, block, listitem and listblock, the relations an appview needs
    /// first. listblock comes last, so the others keep their ids.
    pub fn bsky_default() -> Relations {
        let rel = |name: &str, collection: &str, list: Option<&str>, subject_kind| Relation {
            name: name.into(),
            collection: collection.into(),
            subject: "subject".into(),
            list: list.map(Into::into),
            owner_of: None,
            subject_kind,
            newest_in: false,
            reverse_of: None,
        };
        // followers newest first, as getFollowers lists them
        let follow = Relation { newest_in: true, ..rel("follow", "app.bsky.graph.follow", None, SubjectKind::Did) };
        Relations::new(vec![
            follow,
            rel("block", "app.bsky.graph.block", None, SubjectKind::Did),
            rel("listitem", "app.bsky.graph.listitem", Some("list"), SubjectKind::Did),
            rel("listblock", "app.bsky.graph.listblock", None, SubjectKind::List),
        ])
        .expect("default relations")
    }

    fn validate(&self) -> Result<()> {
        for (i, r) in self.list.iter().enumerate() {
            if !vlatproto::syntax::valid_nsid(&r.collection) {
                bail!("relation {}: {:?} isn't an NSID", r.name, r.collection);
            }
            if r.subject.is_empty() || r.name.is_empty() || r.list.as_ref().is_some_and(|l| l.is_empty()) {
                bail!("relation {i}: name and subject are required, and list can't be empty");
            }
            if r.newest_in && (r.list.is_some() || r.subject_kind == SubjectKind::List) {
                bail!("relation {}: newest_in is for relations between accounts", r.name);
            }
            if r.list.is_some() && r.subject_kind == SubjectKind::List {
                bail!("relation {}: a list relation's subject is an account", r.name);
            }
            if r.name.contains('.') {
                bail!("relation {}: names can't hold a '.'", r.name);
            }
            if self.list[..i].iter().any(|o| o.name == r.name || o.collection == r.collection) {
                bail!("relation {}: name and collection must be unique", r.name);
            }
        }
        Ok(())
    }

    /// The relations a caller names: every one but the companions.
    pub fn user(&self) -> Vec<RelId> {
        (0..self.list.len() as RelId).filter(|&r| !self.is_owner(r) && !self.is_reverse(r)).collect()
    }

    pub fn is_reverse(&self, rel: RelId) -> bool {
        self.list[rel as usize].reverse_of.is_some()
    }

    /// A relation's `.by` companion, which holds its records by subject, newest first.
    pub fn reverse_rel(&self, rel: RelId) -> Option<RelId> {
        if !self.list[rel as usize].newest_in {
            return None;
        }
        let name = &self.list[rel as usize].name;
        self.list.iter().position(|r| r.reverse_of.as_ref() == Some(name)).map(|i| i as RelId)
    }

    /// The relations between actors: neither list relations, nor companions,
    /// nor relations naming a list.
    pub fn actor_rels(&self) -> Vec<RelId> {
        (0..self.list.len() as RelId)
            .filter(|&r| {
                !self.is_list(r)
                    && !self.is_owner(r)
                    && !self.is_reverse(r)
                    && self.list[r as usize].subject_kind == SubjectKind::Did
            })
            .collect()
    }

    pub fn is_list(&self, rel: RelId) -> bool {
        self.list[rel as usize].list.is_some()
    }

    pub fn is_owner(&self, rel: RelId) -> bool {
        self.list[rel as usize].owner_of.is_some()
    }

    /// A list relation's companion, which holds its records by actor.
    pub fn owner_rel(&self, rel: RelId) -> Option<RelId> {
        if !self.is_list(rel) {
            return None;
        }
        let name = &self.list[rel as usize].name;
        self.list.iter().position(|r| r.owner_of.as_ref() == Some(name)).map(|i| i as RelId)
    }

    /// A companion's list relation.
    pub fn list_rel(&self, owner: RelId) -> Option<RelId> {
        let name = self.list[owner as usize].owner_of.as_ref()?;
        self.by_name(name)
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
        self.list
            .iter()
            .position(|r| r.collection == collection && r.owner_of.is_none() && r.reverse_of.is_none())
            .map(|i| i as RelId)
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
    let did_ok = crate::did::pack_plc(did).is_some() || vlatproto::syntax::valid_did(did);
    (did_ok && vlatproto::syntax::valid_rkey(rkey) && !coll.is_empty()).then_some((did, coll, rkey))
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
    fn list_companions() {
        let r = Relations::bsky_default();
        let names: Vec<&str> = r.list.iter().map(|x| x.name.as_str()).collect();
        assert_eq!(names, ["follow", "block", "listitem", "listitem.owner", "listblock", "follow.by"]);
        assert_eq!(r.reverse_rel(0), Some(5), "followers newest first");
        assert_eq!(r.reverse_rel(1), None);
        assert_eq!(r.by_collection("app.bsky.graph.follow"), Some(0), "the relation, not its companion");
        assert_eq!(r.by_collection("app.bsky.graph.listitem"), Some(2));
        assert_eq!(r.owner_rel(2), Some(3));
        assert_eq!(r.list_rel(3), Some(2));
        assert_eq!(r.owner_rel(0), None);
        assert_eq!(r.user(), vec![0, 1, 2, 4]);
        assert_eq!(r.actor_rels(), vec![0, 1], "a listblock goes from an actor to a list, not to an actor");
        assert_eq!(r.get(4).subject_kind, SubjectKind::List, "listblock names a list");
        assert!(r.get(4).valid_subject("at://did:plc:x/app.bsky.graph.list/l") && !r.get(4).valid_subject("did:plc:x"));
        assert!(r.get(0).valid_subject("did:plc:x") && !r.get(0).valid_subject("at://did:plc:x/app.bsky.graph.list/l"));
        let again = Relations::new(r.list.clone()).unwrap();
        assert_eq!(again.list, r.list);
        let rec = serde_json::json!({"list": "at://did:plc:x/app.bsky.graph.list/l", "subject": "did:plc:y"});
        assert_eq!(r.get(2).list_of(&rec), Some("at://did:plc:x/app.bsky.graph.list/l"));
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
