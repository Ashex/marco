///Shorthand for `OwnedView<HasRequestView<'static>>`.
pub type OwnedHasRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::HasRequestView<'static>,
>;
///Shorthand for `OwnedView<HasResponseView<'static>>`.
pub type OwnedHasResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::HasResponseView<'static>,
>;
///Shorthand for `OwnedView<RelationshipsRequestView<'static>>`.
pub type OwnedRelationshipsRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::RelationshipsRequestView<'static>,
>;
///Shorthand for `OwnedView<RelationshipsResponseView<'static>>`.
pub type OwnedRelationshipsResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::RelationshipsResponseView<'static>,
>;
///Shorthand for `OwnedView<ListOutRequestView<'static>>`.
pub type OwnedListOutRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListOutRequestView<'static>,
>;
///Shorthand for `OwnedView<ListOutResponseView<'static>>`.
pub type OwnedListOutResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListOutResponseView<'static>,
>;
///Shorthand for `OwnedView<ListInRequestView<'static>>`.
pub type OwnedListInRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListInRequestView<'static>,
>;
///Shorthand for `OwnedView<ListInResponseView<'static>>`.
pub type OwnedListInResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListInResponseView<'static>,
>;
///Shorthand for `OwnedView<CountsRequestView<'static>>`.
pub type OwnedCountsRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::CountsRequestView<'static>,
>;
///Shorthand for `OwnedView<CountsResponseView<'static>>`.
pub type OwnedCountsResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::CountsResponseView<'static>,
>;
///Shorthand for `OwnedView<IntersectRequestView<'static>>`.
pub type OwnedIntersectRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::IntersectRequestView<'static>,
>;
///Shorthand for `OwnedView<IntersectResponseView<'static>>`.
pub type OwnedIntersectResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::IntersectResponseView<'static>,
>;
///Shorthand for `OwnedView<FilterRequestView<'static>>`.
pub type OwnedFilterRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::FilterRequestView<'static>,
>;
///Shorthand for `OwnedView<FilterResponseView<'static>>`.
pub type OwnedFilterResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::FilterResponseView<'static>,
>;
///Shorthand for `OwnedView<WriteRequestView<'static>>`.
pub type OwnedWriteRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteRequestView<'static>,
>;
///Shorthand for `OwnedView<WriteResponseView<'static>>`.
pub type OwnedWriteResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteResponseView<'static>,
>;
///Shorthand for `OwnedView<WriteBatchRequestView<'static>>`.
pub type OwnedWriteBatchRequestView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteBatchRequestView<'static>,
>;
///Shorthand for `OwnedView<WriteBatchResponseView<'static>>`.
pub type OwnedWriteBatchResponseView = ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteBatchResponseView<'static>,
>;
impl ::connectrpc::Encodable<crate::proto::marco::v1::HasResponse>
for crate::proto::marco::v1::__buffa::view::HasResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::HasResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::HasResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::RelationshipsResponse>
for crate::proto::marco::v1::__buffa::view::RelationshipsResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::RelationshipsResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::RelationshipsResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::ListOutResponse>
for crate::proto::marco::v1::__buffa::view::ListOutResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::ListOutResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListOutResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::ListInResponse>
for crate::proto::marco::v1::__buffa::view::ListInResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::ListInResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::ListInResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::CountsResponse>
for crate::proto::marco::v1::__buffa::view::CountsResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::CountsResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::CountsResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::IntersectResponse>
for crate::proto::marco::v1::__buffa::view::IntersectResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::IntersectResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::IntersectResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::FilterResponse>
for crate::proto::marco::v1::__buffa::view::FilterResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::FilterResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::FilterResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::WriteResponse>
for crate::proto::marco::v1::__buffa::view::WriteResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::WriteResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::WriteBatchResponse>
for crate::proto::marco::v1::__buffa::view::WriteBatchResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::marco::v1::WriteBatchResponse>
for ::buffa::view::OwnedView<
    crate::proto::marco::v1::__buffa::view::WriteBatchResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
/// Full service name for this service.
pub const GRAPH_SERVICE_NAME: &str = "marco.v1.Graph";
/// Static [`Spec`](::connectrpc::Spec) for the `Has` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_HAS_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Has",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `Relationships` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_RELATIONSHIPS_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Relationships",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `ListOut` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_LIST_OUT_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/ListOut",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `ListIn` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_LIST_IN_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/ListIn",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `Counts` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_COUNTS_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Counts",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `Intersect` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_INTERSECT_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Intersect",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `Filter` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_FILTER_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Filter",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the `Write` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_WRITE_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/Write",
        ::connectrpc::StreamType::ClientStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the `WriteBatch` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GRAPH_WRITE_BATCH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/marco.v1.Graph/WriteBatch",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// The graph API: actor-to-actor edges. Reads are open. Writes need the admin
/// token (`Authorization: Bearer <token>`).
///
/// Relations are named in config ("follow", "block", "listitem"). A rel field
/// takes a name; an empty rels list means every relation.
///
/// Lists: a list relation (listitem) keeps edges from a list to its members,
/// and an actor field there takes the list's AT URI. ListOut of a list is its
/// members, newest first; ListIn of a DID is the lists it's on; Has(list, did)
/// is the listitem's URI. Its companion, "listitem.owner", holds the records by
/// owner: ListOut(listitem.owner, owner) is the owner's listitems with their lists.
/// Has(list, did) returns the member's oldest listitem. An add of a key that
/// holds another subject or list replaces the record (an update).
///
/// # Implementing handlers
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically.
///
/// **Unary and server-streaming requests** arrive as
/// [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy
/// view of the request plus its body, valid for the duration of the call.
/// Fields are read directly (`request.name` is a `&str` into the decoded
/// buffer) and the borrow may be held across `.await` points. Anything
/// that must outlive the call — `tokio::spawn`, channels, server state,
/// or data captured by a returned response stream — takes owned data:
/// call `request.to_owned_message()` (or copy the specific fields)
/// first.
///
/// **Client-streaming and bidi requests** arrive as
/// [`InboundStream<Req>`](::connectrpc::InboundStream) — a
/// `ServiceStream` of [`StreamMessage`](::connectrpc::StreamMessage)s.
/// Each item owns its decoded buffer and is `Send + 'static`, so items
/// can be buffered or moved into spawned tasks; read fields zero-copy
/// through the generated accessor methods (`item.name()`) or `.view()`,
/// convert with `.to_owned_message()`, or yield an item back unchanged —
/// `StreamMessage<M>` implements `Encodable<M>`.
///
/// Request types resolved through `extern_path` (e.g. well-known types
/// from another crate) use the same wrappers; the crate that owns the
/// type must be generated with buffa ≥ 0.9.0 and views enabled so the
/// backing `HasMessageView` impl exists.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime and
/// the request's lifetime (unary methods use `use<'a, Self>` and may
/// borrow from `&self`), so stream items must be `'static` and cannot
/// borrow from the request. To stream view-encoded data, encode each
/// item inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait Graph: Send + Sync + 'static {
    /// The newest record of `from` naming `to`, as its URI, or empty. For a
    /// list relation, the member's oldest listitem.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn has<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, crate::proto::marco::v1::HasRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::HasResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Per other and relation: the actor's record naming the other, and the
    /// other's naming the actor (getRelationships). At most 100 others.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn relationships<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::marco::v1::RelationshipsRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::RelationshipsResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// An actor's records, newest first. The cursor is an rkey: pass the last
    /// response's to get the records older than it.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn list_out<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::marco::v1::ListOutRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::ListOutResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// The actors naming a subject, in id order. The cursor is a DID.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn list_in<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, crate::proto::marco::v1::ListInRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::ListInResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Out records and in actors per actor and relation. At most 100 actors.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn counts<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, crate::proto::marco::v1::CountsRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::CountsResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// The actors in both sides (mutuals, "followed by people you follow"), or
    /// in the first and not the second. A side can be a list.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn intersect<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::marco::v1::IntersectRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::IntersectResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// `actors` without those a drop rule hits for `viewer`.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn filter<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, crate::proto::marco::v1::FilterRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::FilterResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Ops from a pipeline, many messages on one stream, answered once at the end.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):
    /// it owns its buffer, is `Send + 'static`, and exposes zero-copy
    /// accessor methods (`item.name()`), `.view()`, and
    /// `.to_owned_message()`.
    fn write<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::InboundStream<crate::proto::marco::v1::WriteRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::WriteResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Ops in one request. The relation comes from each URI's collection.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn write_batch<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::marco::v1::WriteBatchRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::marco::v1::WriteBatchResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
/// Prefer [`Router::add_service`](::connectrpc::Router::add_service) for
/// top-down registration; `register` remains available for compatibility
/// and cases where the service-first call shape is more convenient.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait GraphExt: Graph {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: Graph> GraphExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "Has",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::HasRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::HasRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.has(ctx, sreq)
                                .await?
                                .encode::<crate::proto::marco::v1::HasResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_HAS_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "Relationships",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::RelationshipsRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::RelationshipsRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.relationships(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::marco::v1::RelationshipsResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_RELATIONSHIPS_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "ListOut",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::ListOutRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::ListOutRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.list_out(ctx, sreq)
                                .await?
                                .encode::<crate::proto::marco::v1::ListOutResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_LIST_OUT_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "ListIn",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::ListInRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::ListInRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.list_in(ctx, sreq)
                                .await?
                                .encode::<crate::proto::marco::v1::ListInResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_LIST_IN_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "Counts",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::CountsRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::CountsRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.counts(ctx, sreq)
                                .await?
                                .encode::<crate::proto::marco::v1::CountsResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_COUNTS_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "Intersect",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::IntersectRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::IntersectRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.intersect(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::marco::v1::IntersectResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_INTERSECT_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_NAME,
                "Filter",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::FilterRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::FilterRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.filter(ctx, sreq)
                                .await?
                                .encode::<crate::proto::marco::v1::FilterResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_FILTER_SPEC)
            .route_view_client_stream(
                GRAPH_SERVICE_NAME,
                "Write",
                ::connectrpc::view_client_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<
                                crate::proto::marco::v1::WriteRequest,
                            >(req);
                            svc.write(ctx, req)
                                .await?
                                .encode::<crate::proto::marco::v1::WriteResponse>(format)
                        }
                    }
                }),
            )
            .with_spec(GRAPH_WRITE_SPEC)
            .route_view(
                GRAPH_SERVICE_NAME,
                "WriteBatch",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::marco::v1::__buffa::view::WriteBatchRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::marco::v1::WriteBatchRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.write_batch(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::marco::v1::WriteBatchResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_WRITE_BATCH_SPEC)
    }
}
/// Type-inference marker used by [`Router::add_service`](::connectrpc::Router::add_service).
#[doc(hidden)]
pub struct GraphRegisterMarker;
impl<S: Graph> ::connectrpc::ServiceRegister<GraphRegisterMarker>
for ::std::sync::Arc<S> {
    fn register_service(self, router: ::connectrpc::Router) -> ::connectrpc::Router {
        <S as GraphExt>::register(self, router)
    }
}
/// Monomorphic dispatcher for `Graph`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = GraphServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct GraphServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: Graph> GraphServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for GraphServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: Graph> ::connectrpc::Dispatcher for GraphServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("marco.v1.Graph/")?;
        match method {
            "Has" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_HAS_SPEC),
                )
            }
            "Relationships" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_RELATIONSHIPS_SPEC),
                )
            }
            "ListOut" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_LIST_OUT_SPEC),
                )
            }
            "ListIn" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_LIST_IN_SPEC),
                )
            }
            "Counts" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_COUNTS_SPEC),
                )
            }
            "Intersect" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_INTERSECT_SPEC),
                )
            }
            "Filter" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_FILTER_SPEC),
                )
            }
            "Write" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::client_streaming()
                        .with_spec(GRAPH_WRITE_SPEC),
                )
            }
            "WriteBatch" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(GRAPH_WRITE_BATCH_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("marco.v1.Graph/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "Has" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::HasRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::HasRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::HasRequest,
                    >::from_parts(&req, &body);
                    svc.has(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::HasResponse>(format)
                })
            }
            "Relationships" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::RelationshipsRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::RelationshipsRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::RelationshipsRequest,
                    >::from_parts(&req, &body);
                    svc.relationships(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::RelationshipsResponse>(format)
                })
            }
            "ListOut" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::ListOutRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::ListOutRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::ListOutRequest,
                    >::from_parts(&req, &body);
                    svc.list_out(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::ListOutResponse>(format)
                })
            }
            "ListIn" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::ListInRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::ListInRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::ListInRequest,
                    >::from_parts(&req, &body);
                    svc.list_in(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::ListInResponse>(format)
                })
            }
            "Counts" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::CountsRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::CountsRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::CountsRequest,
                    >::from_parts(&req, &body);
                    svc.counts(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::CountsResponse>(format)
                })
            }
            "Intersect" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::IntersectRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::IntersectRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::IntersectRequest,
                    >::from_parts(&req, &body);
                    svc.intersect(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::IntersectResponse>(format)
                })
            }
            "Filter" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::FilterRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::FilterRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::FilterRequest,
                    >::from_parts(&req, &body);
                    svc.filter(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::FilterResponse>(format)
                })
            }
            "WriteBatch" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::marco::v1::WriteBatchRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::marco::v1::__buffa::view::WriteBatchRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::marco::v1::WriteBatchRequest,
                    >::from_parts(&req, &body);
                    svc.write_batch(ctx, req)
                        .await?
                        .encode::<crate::proto::marco::v1::WriteBatchResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("marco.v1.Graph/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("marco.v1.Graph/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "Write" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_message_request_stream::<
                        crate::proto::marco::v1::WriteRequest,
                    >(requests, format, ctx.decode_options().clone());
                    svc.write(ctx, req_stream)
                        .await?
                        .encode::<crate::proto::marco::v1::WriteResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("marco.v1.Graph/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = GraphClient::new(conn, config);
/// let response = client.has(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = GraphClient::new(http, config);
/// let response = client.has(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// [`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
/// message, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.has(request).await?;
/// let name: &str = resp.view().name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.has(request).await?.into_owned();
/// ```
///
/// [`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
/// zero-copy decoded body (an `OwnedView`) without copying; field access on it
/// goes through `.reborrow()`. Streaming responses yield one
/// [`StreamMessage`](::connectrpc::StreamMessage) per received message from
/// `.message().await` — read fields zero-copy through the generated accessor
/// methods (`msg.name()`) or `.view()`, or convert with `.to_owned_message()`.
#[derive(Clone)]
pub struct GraphClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> GraphClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::connectrpc::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the Has RPC. Sends a request to /marco.v1.Graph/Has.
    pub async fn has(
        &self,
        request: crate::proto::marco::v1::HasRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::HasResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.has_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Has RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn has_with_options(
        &self,
        request: crate::proto::marco::v1::HasRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::HasResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_HAS_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the Relationships RPC. Sends a request to /marco.v1.Graph/Relationships.
    pub async fn relationships(
        &self,
        request: crate::proto::marco::v1::RelationshipsRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::RelationshipsResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.relationships_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the Relationships RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn relationships_with_options(
        &self,
        request: crate::proto::marco::v1::RelationshipsRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::RelationshipsResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_RELATIONSHIPS_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the ListOut RPC. Sends a request to /marco.v1.Graph/ListOut.
    pub async fn list_out(
        &self,
        request: crate::proto::marco::v1::ListOutRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::ListOutResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.list_out_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the ListOut RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn list_out_with_options(
        &self,
        request: crate::proto::marco::v1::ListOutRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::ListOutResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_LIST_OUT_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the ListIn RPC. Sends a request to /marco.v1.Graph/ListIn.
    pub async fn list_in(
        &self,
        request: crate::proto::marco::v1::ListInRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::ListInResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.list_in_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the ListIn RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn list_in_with_options(
        &self,
        request: crate::proto::marco::v1::ListInRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::ListInResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_LIST_IN_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the Counts RPC. Sends a request to /marco.v1.Graph/Counts.
    pub async fn counts(
        &self,
        request: crate::proto::marco::v1::CountsRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::CountsResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.counts_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Counts RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn counts_with_options(
        &self,
        request: crate::proto::marco::v1::CountsRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::CountsResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_COUNTS_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the Intersect RPC. Sends a request to /marco.v1.Graph/Intersect.
    pub async fn intersect(
        &self,
        request: crate::proto::marco::v1::IntersectRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::IntersectResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.intersect_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the Intersect RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn intersect_with_options(
        &self,
        request: crate::proto::marco::v1::IntersectRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::IntersectResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_INTERSECT_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the Filter RPC. Sends a request to /marco.v1.Graph/Filter.
    pub async fn filter(
        &self,
        request: crate::proto::marco::v1::FilterRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::FilterResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.filter_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Filter RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn filter_with_options(
        &self,
        request: crate::proto::marco::v1::FilterRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::FilterResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_FILTER_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the Write RPC. Sends a request to /marco.v1.Graph/Write.
    ///
    /// `requests` is any `Stream<Item = ...> + Send + 'static` of
    /// request messages (the `ClientRequestStream` bound); messages
    /// are sent as the stream yields them. It backs the request
    /// body, so yield owned messages or feed the call from a
    /// channel-backed stream. For a collection that is already in
    /// hand, wrap it with `::connectrpc::stream_iter(...)`.
    ///
    /// Dropping the returned future cancels the call: the request
    /// body is dropped along with it, so messages the stream had
    /// not yet yielded are never delivered. A caller that needs the
    /// request delivered must drive the call to completion rather
    /// than, say, wrapping it in a `timeout`.
    pub async fn write(
        &self,
        requests: impl ::connectrpc::client::ClientRequestStream<
            crate::proto::marco::v1::WriteRequest,
        >,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::WriteResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.write_with_options(requests, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Write RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    ///
    /// `requests` is any `Stream<Item = ...> + Send + 'static` of
    /// request messages (the `ClientRequestStream` bound); messages
    /// are sent as the stream yields them. It backs the request
    /// body, so yield owned messages or feed the call from a
    /// channel-backed stream. For a collection that is already in
    /// hand, wrap it with `::connectrpc::stream_iter(...)`.
    ///
    /// Dropping the returned future cancels the call: the request
    /// body is dropped along with it, so messages the stream had
    /// not yet yielded are never delivered. A caller that needs the
    /// request delivered must drive the call to completion rather
    /// than, say, wrapping it in a `timeout`.
    pub async fn write_with_options(
        &self,
        requests: impl ::connectrpc::client::ClientRequestStream<
            crate::proto::marco::v1::WriteRequest,
        >,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::WriteResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_client_stream(
                &self.transport,
                &self.config,
                GRAPH_WRITE_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                requests,
                options,
            )
            .await
    }
    /// Call the WriteBatch RPC. Sends a request to /marco.v1.Graph/WriteBatch.
    pub async fn write_batch(
        &self,
        request: crate::proto::marco::v1::WriteBatchRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::WriteBatchResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.write_batch_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the WriteBatch RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn write_batch_with_options(
        &self,
        request: crate::proto::marco::v1::WriteBatchRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::marco::v1::__buffa::view::WriteBatchResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_WRITE_BATCH_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
}
