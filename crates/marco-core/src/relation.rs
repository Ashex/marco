//! Relations come from config: any record whose subject is a DID, or the
//! AT URI of a record.
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
//!
//! [[relation]]
//! name = "listblock"
//! collection = "app.bsky.graph.listblock"
//! subject = "subject"
//! subject_collection = "app.bsky.graph.list"
//!
//! [[relation]]
//! name = "modlist"
//! collection = "app.bsky.graph.list"
//! subject = "$self"
//! when = { purpose = "app.bsky.graph.defs#modlist" }
//! ```
//!
//! A relation with `list` keeps edges from that record (a list, in the
//! actor's own repo) to the subject: the list is a node in the directory,
//! keyed by its AT URI. A delete names only the actor and rkey, so each such
//! relation gets a companion, `<name>.owner`, holding the records as actor
//! -> list. Companions are added here, never configured.
//!
//! A relation with `subject_collection` takes a record's AT URI in that
//! collection as the subject, and `subject = "$self"` the record's own URI:
//! either is a node keyed by its URI, as a list is. `when` makes a record an
//! edge only while the paths it names hold its values.
//!
//! [`Relation::edge`] is the one rule for what a record is: every decoder
//! (JSON or CBOR) calls it, and the write API, the add path and the build
//! check an op's subject and list with the same [`Relation::check_subject`]
//! and [`Relation::check_list`].

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub type RelId = u8;

/// The subject path that names the record's own URI.
pub const SELF: &str = "$self";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub collection: String,
    /// Dotted path to the subject in the record, or [`SELF`].
    pub subject: String,
    /// The subject is the AT URI of a record in this collection, not a DID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_collection: Option<String>,
    /// Dotted path to an AT URI in the record, of a record in the same repo:
    /// edges go from that node rather than from the actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<String>,
    /// Set on a companion: the list relation whose records it holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_of: Option<String>,
    /// Dotted paths and the string each must hold for a record to be an edge.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub when: BTreeMap<String, String>,
}

/// A record as an edge: its subject (a DID, or a record's AT URI) and, for a
/// list relation, its list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Edge<'a> {
    pub subject: &'a str,
    pub list: Option<&'a str>,
}

/// Why a record isn't an edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    /// A `when` path doesn't hold its value: a record the relation doesn't keep.
    Unmatched,
    NoSubject,
    /// Not a DID, not a record in `subject_collection`, or under `$self` not
    /// the record's own URI.
    BadSubject,
    NoList,
    /// A list that isn't an AT URI.
    BadList,
    /// A list in another repo.
    CrossRepo,
}

impl Skip {
    pub fn as_str(self) -> &'static str {
        match self {
            Skip::Unmatched => "unmatched",
            Skip::NoSubject => "no_subject",
            Skip::BadSubject => "bad_subject",
            Skip::NoList => "no_list",
            Skip::BadList => "bad_list",
            Skip::CrossRepo => "cross_repo",
        }
    }
}

impl Relation {
    /// Whether the subject is the record's own URI.
    pub fn is_self(&self) -> bool {
        self.subject == SELF
    }

    /// The collection a subject URI is in, for a relation whose subjects are
    /// records: `subject_collection`, or under `$self` the relation's own.
    /// None when subjects are DIDs.
    pub fn subject_records(&self) -> Option<&str> {
        if self.is_self() {
            return Some(&self.collection);
        }
        self.subject_collection.as_deref()
    }

    /// What a subject has to be, for an error.
    pub fn subject_desc(&self) -> String {
        if self.is_self() {
            return "the record's own URI".into();
        }
        match &self.subject_collection {
            Some(c) => format!("the AT URI of a record in {c}"),
            None => "a DID".into(),
        }
    }

    /// Whether `subject` can be record `uri`'s subject.
    pub fn check_subject(&self, uri: &str, subject: &str) -> Result<(), Skip> {
        let ok = if self.is_self() {
            subject == uri && parse_uri(uri).is_some()
        } else if let Some(c) = &self.subject_collection {
            parse_uri(subject).is_some_and(|(_, sc, _)| sc == c)
        } else {
            crate::did::classify(subject).is_some()
        };
        if ok {
            Ok(())
        } else {
            Err(Skip::BadSubject)
        }
    }

    /// A list relation's list for record `uri`: an AT URI in the record's own
    /// repo (the appview refuses the rest too). Ok(None) for other relations.
    pub fn check_list<'a>(&self, uri: &str, list: Option<&'a str>) -> Result<Option<&'a str>, Skip> {
        if self.list.is_none() {
            return Ok(None);
        }
        let l = list.ok_or(Skip::NoList)?;
        let (owner, _, _) = parse_uri(l).ok_or(Skip::BadList)?;
        if parse_uri(uri).map(|x| x.0) != Some(owner) {
            return Err(Skip::CrossRepo);
        }
        Ok(Some(l))
    }

    /// Record `uri` as an edge, or why it isn't one. `get` reads a dotted
    /// path from the record, whatever its encoding. `when` comes first, so a
    /// record the relation doesn't keep is `Unmatched` whatever else it holds,
    /// then the list, then the subject.
    pub fn edge<'a>(&self, uri: &'a str, get: impl Fn(&str) -> Option<&'a str>) -> Result<Edge<'a>, Skip> {
        if self.when.iter().any(|(path, want)| get(path) != Some(want.as_str())) {
            return Err(Skip::Unmatched);
        }
        let list = self.check_list(uri, self.list.as_deref().and_then(&get))?;
        let subject = if self.is_self() { uri } else { get(&self.subject).ok_or(Skip::NoSubject)? };
        self.check_subject(uri, subject)?;
        Ok(Edge { subject, list })
    }
}

/// A dotted path's string in a JSON record.
pub fn json_path<'a>(record: &'a serde_json::Value, path: &str) -> Option<&'a str> {
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
        let user: Vec<Relation> = list.into_iter().filter(|r| r.owner_of.is_none()).collect();
        let r = Relations { list: user };
        r.validate()?;
        let mut all = Vec::with_capacity(r.list.len() + 1);
        for x in r.list {
            let owner = x.list.as_ref().map(|path| Relation {
                name: format!("{}.owner", x.name),
                collection: x.collection.clone(),
                subject: path.clone(),
                subject_collection: None,
                list: None,
                owner_of: Some(x.name.clone()),
                when: BTreeMap::new(),
            });
            all.push(x);
            all.extend(owner);
        }
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

    /// follow, block and listitem, the relations an appview needs first.
    pub fn bsky_default() -> Relations {
        let rel = |name: &str, collection: &str, list: Option<&str>| Relation {
            name: name.into(),
            collection: collection.into(),
            subject: "subject".into(),
            subject_collection: None,
            list: list.map(Into::into),
            owner_of: None,
            when: BTreeMap::new(),
        };
        Relations::new(vec![
            rel("follow", "app.bsky.graph.follow", None),
            rel("block", "app.bsky.graph.block", None),
            rel("listitem", "app.bsky.graph.listitem", Some("list")),
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
            if r.name.contains('.') {
                bail!("relation {}: names can't hold a '.'", r.name);
            }
            if self.list[..i].iter().any(|o| o.name == r.name || o.collection == r.collection) {
                bail!("relation {}: name and collection must be unique", r.name);
            }
            if let Some(c) = &r.subject_collection {
                if !vlatproto::syntax::valid_nsid(c) {
                    bail!("relation {}: subject_collection {c:?} isn't an NSID", r.name);
                }
                if r.is_self() {
                    bail!(
                        "relation {}: a $self subject is in the relation's own collection: no subject_collection",
                        r.name
                    );
                }
            }
            if r.list.is_some() && r.subject_records().is_some() {
                bail!("relation {}: a list relation's subjects are DIDs: no subject_collection or $self", r.name);
            }
            if r.when.keys().any(|p| p.is_empty()) {
                bail!("relation {}: a when path can't be empty", r.name);
            }
        }
        Ok(())
    }

    /// The relations a caller names: every one but the companions.
    pub fn user(&self) -> Vec<RelId> {
        (0..self.list.len() as RelId).filter(|&r| self.list[r as usize].owner_of.is_none()).collect()
    }

    /// The relations between actors: no list relations, companions or
    /// relations whose subjects are records.
    pub fn actor_rels(&self) -> Vec<RelId> {
        (0..self.list.len() as RelId)
            .filter(|&r| !self.is_list(r) && !self.is_owner(r) && !self.is_uri_subject(r))
            .collect()
    }

    pub fn is_list(&self, rel: RelId) -> bool {
        self.list[rel as usize].list.is_some()
    }

    /// Whether the relation's subjects are records' AT URIs
    /// (`subject_collection` or `$self`).
    pub fn is_uri_subject(&self, rel: RelId) -> bool {
        self.list[rel as usize].subject_records().is_some()
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
        self.list.iter().position(|r| r.collection == collection && r.owner_of.is_none()).map(|i| i as RelId)
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

    /// `rel`'s edge from a JSON record, owned.
    fn edge(rel: &Relation, uri: &str, rec: serde_json::Value) -> Result<(String, Option<String>), Skip> {
        rel.edge(uri, |p| json_path(&rec, p)).map(|e| (e.subject.to_string(), e.list.map(str::to_string)))
    }

    fn to(subject: &str, list: Option<&str>) -> Result<(String, Option<String>), Skip> {
        Ok((subject.to_string(), list.map(str::to_string)))
    }

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
        assert_eq!(json_path(&rec, &r.get(2).subject), Some("did:plc:x"));
        assert_eq!(json_path(&rec, &r.get(0).subject), None);
        let uri = "at://did:plc:a/com.example.thing/k";
        assert_eq!(edge(r.get(2), uri, rec.clone()), to("did:plc:x", None));
        assert_eq!(edge(r.get(0), uri, rec), Err(Skip::NoSubject));
        assert_eq!(edge(r.get(0), uri, serde_json::json!({"subject": "bob.test"})), Err(Skip::BadSubject));
        assert!(Relations::parse_toml("[[relation]]\nname='a'\ncollection='nope'\nsubject='s'").is_err());
    }

    #[test]
    fn list_companions() {
        let r = Relations::bsky_default();
        let names: Vec<&str> = r.list.iter().map(|x| x.name.as_str()).collect();
        assert_eq!(names, ["follow", "block", "listitem", "listitem.owner"]);
        assert_eq!(r.by_collection("app.bsky.graph.listitem"), Some(2));
        assert_eq!(r.owner_rel(2), Some(3));
        assert_eq!(r.list_rel(3), Some(2));
        assert_eq!(r.owner_rel(0), None);
        assert_eq!(r.user(), vec![0, 1, 2]);
        assert_eq!(r.actor_rels(), vec![0, 1]);
        let again = Relations::new(r.list.clone()).unwrap();
        assert_eq!(again.list, r.list);
        let uri = "at://did:plc:x/app.bsky.graph.listitem/k";
        let item = |rec| edge(r.get(2), uri, rec);
        let l = "at://did:plc:x/app.bsky.graph.list/l";
        assert_eq!(item(serde_json::json!({"list": l, "subject": "did:plc:y"})), to("did:plc:y", Some(l)));
        let theirs = "at://did:plc:z/app.bsky.graph.list/l";
        assert_eq!(item(serde_json::json!({"list": theirs, "subject": "did:plc:y"})), Err(Skip::CrossRepo));
        assert_eq!(item(serde_json::json!({"list": "l", "subject": "did:plc:y"})), Err(Skip::BadList));
        assert_eq!(item(serde_json::json!({"subject": "did:plc:y"})), Err(Skip::NoList));
        assert_eq!(item(serde_json::json!({"list": theirs})), Err(Skip::CrossRepo), "the list before the subject");
    }

    /// The defaults, then a relation whose subject is a list, then one that
    /// holds a list's own URI while its purpose is a mod list.
    const LISTS: &str = r#"
[[relation]]
name = "follow"
collection = "app.bsky.graph.follow"
subject = "subject"

[[relation]]
name = "block"
collection = "app.bsky.graph.block"
subject = "subject"

[[relation]]
name = "listitem"
collection = "app.bsky.graph.listitem"
subject = "subject"
list = "list"

[[relation]]
name = "listblock"
collection = "app.bsky.graph.listblock"
subject = "subject"
subject_collection = "app.bsky.graph.list"

[[relation]]
name = "modlist"
collection = "app.bsky.graph.list"
subject = "$self"
when = { purpose = "app.bsky.graph.defs#modlist" }
"#;

    #[test]
    fn list_subjects_and_purpose() {
        let r = Relations::parse_toml(LISTS).unwrap();
        let names: Vec<&str> = r.list.iter().map(|x| x.name.as_str()).collect();
        assert_eq!(names, ["follow", "block", "listitem", "listitem.owner", "listblock", "modlist"]);
        assert_eq!(r.actor_rels(), vec![0, 1], "no list relation, companion or URI subject");
        assert_eq!((r.is_uri_subject(4), r.is_uri_subject(5), r.is_uri_subject(3)), (true, true, false));
        assert_eq!(r.get(4).subject_records(), Some("app.bsky.graph.list"));
        assert_eq!(r.get(5).subject_records(), Some("app.bsky.graph.list"));
        assert_eq!(Relations::new(r.list.clone()).unwrap().list, r.list, "a snapshot's relations make the same");

        let lb = r.get(4);
        let uri = "at://did:plc:v/app.bsky.graph.listblock/3k";
        let list = "at://did:plc:o/app.bsky.graph.list/3l";
        let block = |rec| edge(lb, uri, rec);
        assert_eq!(block(serde_json::json!({"subject": list})), to(list, None));
        let follow = "at://did:plc:o/app.bsky.graph.follow/3l";
        assert_eq!(block(serde_json::json!({"subject": follow})), Err(Skip::BadSubject), "another collection");
        assert_eq!(block(serde_json::json!({"subject": "did:plc:o"})), Err(Skip::BadSubject), "a DID");
        assert_eq!(block(serde_json::json!({})), Err(Skip::NoSubject));
        assert_eq!(lb.check_subject(uri, list), Ok(()));
        assert_eq!(lb.subject_desc(), "the AT URI of a record in app.bsky.graph.list");

        let ml = r.get(5);
        let own = "at://did:plc:o/app.bsky.graph.list/3l";
        let purpose = |p: &str| edge(ml, own, serde_json::json!({"name": "n", "purpose": p}));
        assert_eq!(purpose("app.bsky.graph.defs#modlist"), to(own, None));
        assert_eq!(purpose("app.bsky.graph.defs#curatelist"), Err(Skip::Unmatched));
        assert_eq!(edge(ml, own, serde_json::json!({"name": "n"})), Err(Skip::Unmatched));
        assert_eq!(edge(ml, own, serde_json::json!({"purpose": 3})), Err(Skip::Unmatched), "only a string matches");
        assert_eq!(ml.check_subject(own, own), Ok(()));
        assert_eq!(ml.check_subject(own, "at://did:plc:o/app.bsky.graph.list/3m"), Err(Skip::BadSubject));
        let handle = "at://handle.test/app.bsky.graph.list/3l";
        assert_eq!(ml.check_subject(handle, handle), Err(Skip::BadSubject), "not a record URI");
        assert_eq!(ml.subject_desc(), "the record's own URI");

        // a nested path, and several: every one has to hold
        let nested = Relations::parse_toml(
            r#"
[[relation]]
name = "starred"
collection = "com.example.star"
subject = "subject"
when = { "kind.name" = "gold", visible = "yes" }
"#,
        )
        .unwrap();
        let s = nested.get(0);
        let uri = "at://did:plc:a/com.example.star/k";
        let rec = serde_json::json!({"subject": "did:plc:b", "kind": {"name": "gold"}, "visible": "yes"});
        assert_eq!(edge(s, uri, rec), to("did:plc:b", None));
        let rec = serde_json::json!({"subject": "did:plc:b", "kind": {"name": "gold"}, "visible": "no"});
        assert_eq!(edge(s, uri, rec), Err(Skip::Unmatched));
        let rec = serde_json::json!({"kind": {"name": "silver"}});
        assert_eq!(edge(s, uri, rec), Err(Skip::Unmatched), "when before the subject");
    }

    #[test]
    fn refuses_list_subjects_it_cant_hold() {
        let one = |extra: &str| {
            Relations::parse_toml(&format!(
                "[[relation]]\nname = 'x'\ncollection = 'com.example.x'\nsubject = 'subject'\n{extra}"
            ))
        };
        assert!(one("").is_ok());
        assert!(one("subject_collection = 'not an nsid'").is_err());
        assert!(
            one("list = 'list'\nsubject_collection = 'app.bsky.graph.list'").is_err(),
            "a list relation's subjects are DIDs"
        );
        assert!(one("when = { '' = 'v' }").is_err());
        assert!(one("when = { a.b = 'v' }").is_err(), "an unquoted dotted key is a nested table");
        let own = |extra: &str| {
            Relations::parse_toml(&format!(
                "[[relation]]\nname = 'x'\ncollection = 'com.example.x'\nsubject = '$self'\n{extra}"
            ))
        };
        assert!(own("").is_ok());
        assert!(own("subject_collection = 'com.example.x'").is_err());
        assert!(own("list = 'list'").is_err());
    }

    /// A config without the new keys writes the relations it always did, so
    /// a snapshot's directory meta and a scan's relations.toml don't change,
    /// and an existing snapshot's meta reads back as the same relations.
    #[test]
    fn the_defaults_serialize_as_before() {
        let r = Relations::bsky_default();
        let meta = r#"[{"name":"follow","collection":"app.bsky.graph.follow","subject":"subject"},{"name":"block","collection":"app.bsky.graph.block","subject":"subject"},{"name":"listitem","collection":"app.bsky.graph.listitem","subject":"subject","list":"list"},{"name":"listitem.owner","collection":"app.bsky.graph.listitem","subject":"list","owner_of":"listitem"}]"#;
        assert_eq!(serde_json::to_string(&r.list).unwrap(), meta);
        let read: Vec<Relation> = serde_json::from_str(meta).unwrap();
        assert_eq!(Relations::new(read).unwrap().list, r.list);
        let lists = Relations::parse_toml(LISTS).unwrap();
        let toml = toml::to_string(&lists).unwrap();
        assert_eq!(Relations::parse_toml(&toml).unwrap().list, lists.list, "{toml}");
        let json = serde_json::to_string(&lists.list).unwrap();
        assert!(json.contains(r#""subject_collection":"app.bsky.graph.list""#), "{json}");
        assert!(json.contains(r#""when":{"purpose":"app.bsky.graph.defs#modlist"}"#), "{json}");
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
