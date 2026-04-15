use std::collections::HashMap;

use prost::Message;
use pyo3::prelude::*;

use crate::proto;
use crate::pytypes;

use super::{
    encode_attributes, encode_instrumentation_scope, encode_resource, encode_span_id,
    encode_trace_id, encode_trace_state, EncodeResult,
};

const SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK: u32 = 0x100;
const SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK: u32 = 0x200;

#[pyfunction]
pub(crate) fn serialize_spans(
    py: Python<'_>,
    spans: Vec<pytypes::ReadableSpan>,
) -> PyResult<Vec<u8>> {
    let request = encode_spans(py, &spans)?;
    Ok(request.encode_to_vec())
}

/// Encode a list of ReadableSpan objects into an ExportTraceServiceRequest.
fn encode_spans(
    py: Python<'_>,
    spans: &[pytypes::ReadableSpan],
) -> EncodeResult<proto::collector::trace::v1::ExportTraceServiceRequest> {
    let mut resource_map: HashMap<&pytypes::Resource, ResourceGroup<'_>> = HashMap::new();

    for span in spans {
        let group = resource_map
            .entry(&span.resource)
            .or_insert_with(|| ResourceGroup {
                schema_url: span.resource.schema_url.clone(),
                scope_map: HashMap::new(),
            });

        let scope_schema_url = span
            .instrumentation_scope
            .as_ref()
            .and_then(|s| s.schema_url.clone())
            .unwrap_or_default();

        let scope_group =
            group
                .scope_map
                .entry(span.instrumentation_scope.as_ref())
                .or_insert_with(|| ScopeGroup {
                    schema_url: scope_schema_url,
                    spans: Vec::new(),
                });

        scope_group.spans.push(encode_span(py, span)?);
    }

    let resource_spans = resource_map
        .into_iter()
        .map(|(resource, rg)| {
            Ok(proto::trace::v1::ResourceSpans {
                resource: Some(encode_resource(py, resource)?),
                schema_url: rg.schema_url,
                scope_spans: rg
                    .scope_map
                    .into_iter()
                    .map(|(scope, sg)| {
                        Ok(proto::trace::v1::ScopeSpans {
                            scope: scope
                                .map(|s| encode_instrumentation_scope(py, s))
                                .transpose()?,
                            schema_url: sg.schema_url,
                            spans: sg.spans,
                        })
                    })
                    .collect::<EncodeResult<_>>()?,
            })
        })
        .collect::<EncodeResult<_>>()?;

    Ok(proto::collector::trace::v1::ExportTraceServiceRequest { resource_spans })
}

fn encode_span(
    py: Python<'_>,
    span: &pytypes::ReadableSpan,
) -> EncodeResult<proto::trace::v1::Span> {
    let (trace_id, span_id, trace_state) = match &span.context {
        Some(ctx) => (
            encode_trace_id(ctx.trace_id),
            encode_span_id(ctx.span_id),
            encode_trace_state(py, &ctx.trace_state)?,
        ),
        None => Default::default(),
    };

    let parent_span_id = span
        .parent
        .as_ref()
        .map(|p| encode_span_id(p.span_id))
        .unwrap_or_default();

    let flags = span_flags(span.parent.as_ref());

    // SDK SpanKind values are offset by +1 from proto values
    let kind = (span.kind as i32) + 1;

    let status = Some(proto::trace::v1::Status {
        code: span.status.status_code as i32,
        message: span.status.description.clone().unwrap_or_default(),
    });

    Ok(proto::trace::v1::Span {
        trace_id,
        span_id,
        trace_state,
        parent_span_id,
        flags,
        name: span.name.clone(),
        kind,
        start_time_unix_nano: span.start_time.unwrap_or(0),
        end_time_unix_nano: span.end_time.unwrap_or(0),
        attributes: encode_attributes(py, &span.attributes)?,
        dropped_attributes_count: span.dropped_attributes,
        events: span
            .events
            .iter()
            .map(|e| encode_event(py, e))
            .collect::<EncodeResult<_>>()?,
        dropped_events_count: span.dropped_events,
        links: span
            .links
            .iter()
            .map(|l| encode_link(py, l))
            .collect::<EncodeResult<_>>()?,
        dropped_links_count: span.dropped_links,
        status,
    })
}

fn encode_event(
    py: Python<'_>,
    event: &pytypes::Event,
) -> EncodeResult<proto::trace::v1::span::Event> {
    Ok(proto::trace::v1::span::Event {
        time_unix_nano: event.timestamp,
        name: event.name.clone(),
        attributes: encode_attributes(py, &event.attributes)?,
        dropped_attributes_count: event.dropped_attributes,
    })
}

fn encode_link(
    py: Python<'_>,
    link: &pytypes::Link,
) -> EncodeResult<proto::trace::v1::span::Link> {
    Ok(proto::trace::v1::span::Link {
        trace_id: encode_trace_id(link.context.trace_id),
        span_id: encode_span_id(link.context.span_id),
        // TODO: Examine if trace_state should be encoded on links in the Python SDK.
        // Currently the Python OTLP exporter does not set this field.
        // trace_state: encode_trace_state(py, &link.context.trace_state)?,
        attributes: encode_attributes(py, &link.attributes)?,
        dropped_attributes_count: link.dropped_attributes,
        flags: span_flags(Some(&link.context)),
        ..Default::default()
    })
}

fn span_flags(parent: Option<&pytypes::SpanContext>) -> u32 {
    let mut flags = SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK;
    if parent.is_some_and(|p| p.is_remote) {
        flags |= SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK;
    }
    flags
}

struct ResourceGroup<'a> {
    schema_url: String,
    scope_map: HashMap<Option<&'a pytypes::InstrumentationScope>, ScopeGroup>,
}

struct ScopeGroup {
    schema_url: String,
    spans: Vec<proto::trace::v1::Span>,
}
