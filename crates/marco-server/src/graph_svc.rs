//! marco.v1.Graph over the served graph.

use std::sync::Arc;

use connectrpc::{ConnectError, InboundStream, RequestContext, Response, ServiceRequest, ServiceResult};
use futures::StreamExt;
use marco_core::{Dir, Graph, ListRef, RelId};
use marco_proto::connect::marco::v1 as svc;
use marco_proto::v1::__buffa::oneof::{write_op, write_result};
use marco_proto::v1::*;

use crate::app::{App, Op, Outcome, MAX_BATCH};

/// The Graph service; `gate` holds reads until marco is ready (the API
/// listener; the console's copy on the admin listener answers regardless).
pub struct GraphApi(pub Arc<App>, pub bool);

pub fn bad(m: impl Into<String>) -> ConnectError {
    ConnectError::invalid_argument(m)
}

pub fn rel_id(g: &Graph, name: &str) -> Result<RelId, ConnectError> {
    if name.is_empty() {
        return Err(bad("rel is required"));
    }
    g.relations().by_name(name).ok_or_else(|| bad(format!("no relation {name:?}")))
}

pub fn rel_ids(g: &Graph, names: &[String]) -> Result<Vec<RelId>, ConnectError> {
    if names.is_empty() {
        return Ok((0..g.relations().len() as RelId).collect());
    }
    names.iter().map(|n| rel_id(g, n)).collect()
}

pub fn dir_of(d: &buffa::EnumValue<marco_proto::v1::Dir>) -> Result<Dir, ConnectError> {
    match d.as_known() {
        Some(marco_proto::v1::Dir::DIR_OUT) => Ok(Dir::Out),
        Some(marco_proto::v1::Dir::DIR_IN) => Ok(Dir::In),
        _ => Err(bad("dir must be DIR_OUT or DIR_IN")),
    }
}

pub fn dir_pb(d: Dir) -> buffa::EnumValue<marco_proto::v1::Dir> {
    match d {
        Dir::Out => marco_proto::v1::Dir::DIR_OUT.into(),
        Dir::In => marco_proto::v1::Dir::DIR_IN.into(),
    }
}

fn limit(l: u32, def: u32, max: u32) -> usize {
    (if l == 0 { def } else { l.min(max) }) as usize
}

fn ops_of(ops: &[WriteOp]) -> Result<Vec<Op>, ConnectError> {
    if ops.len() > MAX_BATCH {
        return Err(bad(format!("at most {MAX_BATCH} ops a message")));
    }
    ops.iter()
        .enumerate()
        .map(|(i, o)| match &o.op {
            Some(write_op::Op::Add(a)) => {
                Ok(Op::Add { uri: a.uri.clone(), subject: a.subject.clone(), time_us: a.time_us })
            }
            Some(write_op::Op::Delete(d)) => Ok(Op::Delete { uri: d.uri.clone(), time_us: d.time_us }),
            None => Err(bad(format!("op {i} is neither add nor delete"))),
        })
        .collect()
}

fn result_pb(o: Outcome) -> WriteResult {
    let outcome = match o {
        Outcome::Added => write_result::Outcome::Added(true),
        Outcome::Exists => write_result::Outcome::Exists(true),
        Outcome::Deleted => write_result::Outcome::Deleted(true),
        Outcome::Removed(s) => write_result::Outcome::Removed(s),
        Outcome::NotFound => write_result::Outcome::NotFound(true),
        Outcome::Error(e) => write_result::Outcome::Error(e),
    };
    WriteResult { outcome: Some(outcome), ..Default::default() }
}

impl GraphApi {
    fn writable(&self) -> Result<(), ConnectError> {
        if self.0.cfg.read_only {
            return Err(ConnectError::failed_precondition("this marco is --read-only"));
        }
        Ok(())
    }

    async fn write_ops(&self, ops: &[WriteOp], cursor: &str) -> Result<Vec<WriteResult>, ConnectError> {
        self.writable()?;
        let ops = ops_of(ops)?;
        App::validate(&self.0.graph(), &ops).map_err(bad)?;
        let out = self.0.apply(&ops, cursor).await.map_err(|e| ConnectError::unavailable(format!("{e:#}")))?;
        Ok(out.into_iter().map(result_pb).collect())
    }

    fn readable(&self) -> Result<(), ConnectError> {
        if self.1 {
            self.0.read_gate().map_err(ConnectError::unavailable)?;
        }
        Ok(())
    }
}

#[allow(refining_impl_trait)]
impl svc::Graph for GraphApi {
    async fn has(&self, _: RequestContext, req: ServiceRequest<'_, HasRequest>) -> ServiceResult<HasResponse> {
        self.readable()?;
        let g = self.0.graph();
        let rel = rel_id(&g, req.rel)?;
        let hidden = g.hidden(req.from) || g.hidden(req.to);
        let uri = if hidden { None } else { g.has(rel, req.from, req.to) };
        Response::ok(HasResponse { uri: uri.unwrap_or_default(), ..Default::default() })
    }

    async fn relationships(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, RelationshipsRequest>,
    ) -> ServiceResult<RelationshipsResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.others.len() > 100 {
            return Err(bad("at most 100 others"));
        }
        let g = self.0.graph();
        let rels = rel_ids(&g, &req.rels)?;
        let others: Vec<&str> = req.others.iter().map(String::as_str).collect();
        let rows = g.relationships(&req.actor, &others, &rels);
        let actor_hidden = g.hidden(&req.actor);
        let relationships = req
            .others
            .iter()
            .zip(rows)
            .map(|(o, per)| {
                let hide = actor_hidden || g.hidden(o);
                Relationship {
                    other: o.clone(),
                    rels: rels
                        .iter()
                        .zip(per)
                        .map(|(&r, x)| RelationshipEdge {
                            rel: g.relations().get(r).name.clone(),
                            out_uri: if hide { String::new() } else { x.out.unwrap_or_default() },
                            in_uri: if hide { String::new() } else { x.inn.unwrap_or_default() },
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();
        Response::ok(RelationshipsResponse { relationships, ..Default::default() })
    }

    async fn list_out(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, ListOutRequest>,
    ) -> ServiceResult<ListOutResponse> {
        self.readable()?;
        let g = self.0.graph();
        let rel = rel_id(&g, req.rel)?;
        if g.hidden(req.actor) {
            return Response::ok(ListOutResponse::default());
        }
        let cursor = (!req.cursor.is_empty()).then_some(req.cursor);
        let (items, next) = g.out_list(rel, req.actor, limit(req.limit, 50, 1000), cursor);
        let edges = items
            .into_iter()
            .filter(|(_, s)| !g.hidden(s))
            .map(|(uri, subject)| OutEdge { uri, subject, ..Default::default() })
            .collect();
        Response::ok(ListOutResponse { edges, cursor: next.unwrap_or_default(), ..Default::default() })
    }

    async fn list_in(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, ListInRequest>,
    ) -> ServiceResult<ListInResponse> {
        self.readable()?;
        let g = self.0.graph();
        let rel = rel_id(&g, req.rel)?;
        if g.hidden(req.subject) {
            return Response::ok(ListInResponse::default());
        }
        let cursor = (!req.cursor.is_empty()).then_some(req.cursor);
        let (actors, next) = g.in_list(rel, req.subject, limit(req.limit, 50, 1000), cursor);
        let actors = actors.into_iter().filter(|a| !g.hidden(a)).collect();
        Response::ok(ListInResponse { actors, cursor: next.unwrap_or_default(), ..Default::default() })
    }

    async fn counts(&self, _: RequestContext, req: ServiceRequest<'_, CountsRequest>) -> ServiceResult<CountsResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.actors.len() > 100 {
            return Err(bad("at most 100 actors"));
        }
        let g = self.0.graph();
        let rels = rel_ids(&g, &req.rels)?;
        let counts = req
            .actors
            .iter()
            .map(|a| {
                let hide = g.hidden(a);
                ActorCounts {
                    actor: a.clone(),
                    rels: rels
                        .iter()
                        .map(|&r| {
                            let (o, i) = if hide { (0, 0) } else { g.counts(r, a) };
                            RelCounts {
                                rel: g.relations().get(r).name.clone(),
                                out_records: o,
                                in_actors: i,
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();
        Response::ok(CountsResponse { counts, ..Default::default() })
    }

    async fn intersect(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, IntersectRequest>,
    ) -> ServiceResult<IntersectResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let g = self.0.graph();
        let side = |s: &ListSide| -> Result<Option<ListRef>, ConnectError> {
            let rel = rel_id(&g, &s.rel)?;
            let dir = dir_of(&s.dir)?;
            if g.hidden(&s.actor) {
                return Ok(None);
            }
            Ok(g.id_of(&s.actor).map(|actor| ListRef { rel, dir, actor }))
        };
        let (a, b) = (side(&req.a)?, side(&req.b)?);
        let len = |s: Option<ListRef>| {
            s.map_or(0, |s| match s.dir {
                Dir::Out => g.out_count(s.rel, s.actor),
                Dir::In => g.in_count(s.rel, s.actor),
            })
        };
        let actors = match (a, b) {
            (Some(x), Some(y)) => g
                .intersect(x, y, limit(req.limit, 100, 10_000))
                .into_iter()
                .filter_map(|id| g.did_of(id))
                .filter(|d| !g.hidden(d))
                .collect(),
            _ => Vec::new(),
        };
        Response::ok(IntersectResponse { actors, a_len: len(a), b_len: len(b), ..Default::default() })
    }

    async fn filter(&self, _: RequestContext, req: ServiceRequest<'_, FilterRequest>) -> ServiceResult<FilterResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.actors.len() > 1000 {
            return Err(bad("at most 1000 actors"));
        }
        let g = self.0.graph();
        let drops: Vec<(RelId, Dir)> =
            req.drop.iter().map(|d| Ok((rel_id(&g, &d.rel)?, dir_of(&d.dir)?))).collect::<Result<_, ConnectError>>()?;
        let viewer = g.id_of(&req.viewer);
        let ids: Vec<Option<u32>> = req.actors.iter().map(|a| g.id_of(a)).collect();
        let kept: std::collections::HashSet<u32> = match viewer {
            Some(v) => g.filter_id(v, &ids.iter().flatten().copied().collect::<Vec<_>>(), &drops).into_iter().collect(),
            None => ids.iter().flatten().copied().collect(),
        };
        let mut actors = Vec::new();
        let mut dropped = Vec::new();
        for (a, id) in req.actors.iter().zip(&ids) {
            if g.hidden(a) {
                dropped.push(Dropped { actor: a.clone(), rel: "status".into(), ..Default::default() });
                continue;
            }
            match id {
                Some(x) if !kept.contains(x) => {
                    let v = viewer.unwrap_or_default();
                    let hit = drops.iter().find(|&&(r, d)| match d {
                        Dir::Out => g.has_id(r, v, *x).is_some(),
                        Dir::In => g.in_contains(r, v, *x),
                    });
                    let (r, d) = hit.copied().unwrap_or(drops[0]);
                    dropped.push(Dropped {
                        actor: a.clone(),
                        rel: g.relations().get(r).name.clone(),
                        dir: dir_pb(d),
                        ..Default::default()
                    });
                }
                _ => actors.push(a.clone()),
            }
        }
        Response::ok(FilterResponse { actors, dropped, ..Default::default() })
    }

    async fn write(&self, _: RequestContext, mut reqs: InboundStream<WriteRequest>) -> ServiceResult<WriteResponse> {
        self.writable()?;
        let mut results = Vec::new();
        let mut cursor = String::new();
        while let Some(m) = reqs.next().await {
            let m = m?.to_owned_message();
            results.extend(self.write_ops(&m.ops, &m.cursor).await?);
            if !m.cursor.is_empty() {
                cursor = m.cursor;
            }
        }
        Response::ok(WriteResponse { results, cursor, ..Default::default() })
    }

    async fn write_batch(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, WriteBatchRequest>,
    ) -> ServiceResult<WriteBatchResponse> {
        let req = req.to_owned_message();
        let results = self.write_ops(&req.ops, &req.cursor).await?;
        Response::ok(WriteBatchResponse { results, cursor: req.cursor, ..Default::default() })
    }
}
