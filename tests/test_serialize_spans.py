from __future__ import annotations

import itertools
from typing import Sequence

import pytest
from opentelemetry.exporter.otlp.proto.common._internal.trace_encoder import (
    encode_spans as py_encode_spans,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto_native._rs import serialize_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import SpanContext, TraceFlags, TraceState
from opentelemetry.trace.status import Status, StatusCode

_TRACE_ID = 0x12345678901234567890123456789012
_SPAN_ID = 0x1234567890123456
_PARENT_SPAN_ID = 0xABCDEF1234567890
_LINK_TRACE_ID = 0xAABBCCDDEEFF00112233445566778899
_LINK_SPAN_ID = 0xDEADBEEFCAFEBABE

_BASE_TS = 1_700_000_000_000_000_000


def _make_context(
    trace_id: int = _TRACE_ID,
    span_id: int = _SPAN_ID,
    is_remote: bool = False,
    trace_flags: TraceFlags = TraceFlags(TraceFlags.SAMPLED),
    trace_state: TraceState | None = None,
) -> SpanContext:
    return SpanContext(
        trace_id=trace_id,
        span_id=span_id,
        is_remote=is_remote,
        trace_flags=trace_flags,
        trace_state=trace_state or TraceState(),
    )


def _make_resource(
    attrs: dict[str, str | int | float | bool] | None = None,
    schema_url: str | None = None,
) -> Resource:
    return Resource(attributes=attrs or {}, schema_url=schema_url)


def _make_scope(
    name: str = "test.scope",
    version: str | None = "1.0.0",
    schema_url: str | None = None,
    attrs: dict[str, str | int | float | bool] | None = None,
) -> InstrumentationScope:
    return InstrumentationScope(
        name=name,
        version=version,
        schema_url=schema_url,
        attributes=attrs,
    )


def _make_span(
    name: str = "test-span",
    context: SpanContext | None = None,
    parent: SpanContext | None = None,
    resource: Resource | None = None,
    attributes: dict | None = None,
    events: Sequence[Event] = (),
    links: Sequence[Link] = (),
    kind: SpanKind = SpanKind.INTERNAL,
    status: Status | None = None,
    start_time: int = _BASE_TS,
    end_time: int = _BASE_TS + 1_000_000_000,
    instrumentation_scope: InstrumentationScope | None = None,
) -> ReadableSpan:
    return ReadableSpan(
        name=name,
        context=context or _make_context(),
        parent=parent,
        resource=resource or _make_resource(),
        attributes=attributes,
        events=events,
        links=links,
        kind=kind,
        status=status or Status(StatusCode.UNSET),
        start_time=start_time,
        end_time=end_time,
        instrumentation_scope=instrumentation_scope or _make_scope(),
    )


def _sort_attributes(attrs: list) -> None:  # type: ignore[type-arg]
    """Sort a repeated KeyValue field by key for deterministic comparison."""
    attrs.sort(key=lambda kv: kv.key)


def _normalize(request: ExportTraceServiceRequest) -> ExportTraceServiceRequest:
    """Normalize for deterministic comparison."""
    for rs in request.resource_spans:
        _sort_attributes(rs.resource.attributes)
        for ss in rs.scope_spans:
            _sort_attributes(ss.scope.attributes)
            for span in ss.spans:
                _sort_attributes(span.attributes)
                for event in span.events:
                    _sort_attributes(event.attributes)
                for link in span.links:
                    _sort_attributes(link.attributes)
        rs.scope_spans.sort(key=lambda ss: ss.scope.SerializeToString())
    request.resource_spans.sort(
        key=lambda rs: rs.resource.SerializeToString(),
    )
    return request


def _assert_same(spans: Sequence[ReadableSpan]) -> None:
    """Serialize with both implementations, parse back, and compare."""
    rust_bytes: bytes = serialize_spans(list(spans))
    py_bytes: bytes = py_encode_spans(spans).SerializeToString()

    rust_parsed = ExportTraceServiceRequest()
    rust_parsed.ParseFromString(rust_bytes)

    py_parsed = ExportTraceServiceRequest()
    py_parsed.ParseFromString(py_bytes)

    _normalize(rust_parsed)
    _normalize(py_parsed)

    assert rust_parsed == py_parsed, (
        f"Mismatch!\n"
        f"--- Rust ---\n{rust_parsed}\n"
        f"--- Python ---\n{py_parsed}"
    )


class TestMinimalSpans:
    """Minimal span configurations."""

    def test_single_empty_span(self) -> None:
        _assert_same([_make_span()])

    def test_span_with_no_optional_fields(self) -> None:
        span = ReadableSpan(
            name="bare",
            context=_make_context(),
            resource=_make_resource(),
            status=Status(StatusCode.UNSET),
            start_time=_BASE_TS,
            end_time=_BASE_TS + 100,
            instrumentation_scope=_make_scope(),
        )
        _assert_same([span])

    def test_empty_list(self) -> None:
        _assert_same([])


class TestSpanKind:
    @pytest.mark.parametrize("kind", list(SpanKind))
    def test_all_span_kinds(self, kind: SpanKind) -> None:
        _assert_same([_make_span(kind=kind)])


class TestStatus:
    @pytest.mark.parametrize(
        ("code", "description"),
        [
            (StatusCode.UNSET, None),
            (StatusCode.OK, None),
            (StatusCode.ERROR, "something went wrong"),
            (StatusCode.ERROR, ""),
            (StatusCode.ERROR, None),
        ],
    )
    def test_status_variants(
        self, code: StatusCode, description: str | None
    ) -> None:
        _assert_same([_make_span(status=Status(code, description))])


class TestScalarAttributes:
    def test_string_attribute(self) -> None:
        _assert_same([_make_span(attributes={"key": "value"})])

    def test_int_attribute(self) -> None:
        _assert_same([_make_span(attributes={"count": 42})])

    def test_negative_int_attribute(self) -> None:
        _assert_same([_make_span(attributes={"temp": -273})])

    def test_large_int_attribute(self) -> None:
        _assert_same([_make_span(attributes={"big": 2**53})])

    def test_float_attribute(self) -> None:
        _assert_same([_make_span(attributes={"ratio": 3.14})])

    def test_negative_float_attribute(self) -> None:
        _assert_same([_make_span(attributes={"delta": -0.001})])

    def test_bool_true_attribute(self) -> None:
        _assert_same([_make_span(attributes={"flag": True})])

    def test_bool_false_attribute(self) -> None:
        _assert_same([_make_span(attributes={"flag": False})])

    def test_empty_string_attribute(self) -> None:
        _assert_same([_make_span(attributes={"empty": ""})])

    def test_unicode_attribute(self) -> None:
        _assert_same([_make_span(attributes={"emoji": "🔥🎉", "cjk": "日本語"})])

    def test_no_attributes(self) -> None:
        _assert_same([_make_span(attributes=None)])

    def test_empty_attributes(self) -> None:
        _assert_same([_make_span(attributes={})])


class TestSequenceAttributes:
    def test_string_list(self) -> None:
        _assert_same([_make_span(attributes={"tags": ["a", "b", "c"]})])

    def test_int_list(self) -> None:
        _assert_same([_make_span(attributes={"ids": [1, 2, 3]})])

    def test_float_list(self) -> None:
        _assert_same([_make_span(attributes={"values": [1.1, 2.2]})])

    def test_bool_list(self) -> None:
        _assert_same([_make_span(attributes={"flags": [True, False, True]})])

    def test_empty_list(self) -> None:
        _assert_same([_make_span(attributes={"empty": []})])

    def test_single_element_list(self) -> None:
        _assert_same([_make_span(attributes={"single": ["only"]})])

    def test_tuple_attribute(self) -> None:
        _assert_same([_make_span(attributes={"coords": (1, 2, 3)})])


class TestManyAttributes:
    def test_many_mixed_attributes(self) -> None:
        attrs = {
            "str_key": "value",
            "int_key": 99,
            "float_key": 1.5,
            "bool_key": True,
            "str_list": ["x", "y"],
            "int_list": [10, 20],
            "float_list": [0.1, 0.2],
            "bool_list": [False],
        }
        _assert_same([_make_span(attributes=attrs)])

    def test_100_attributes(self) -> None:
        attrs = {f"attr_{i}": f"val_{i}" for i in range(100)}
        _assert_same([_make_span(attributes=attrs)])


class TestEvents:
    def test_single_event(self) -> None:
        event = Event(name="ev1", timestamp=_BASE_TS + 500)
        _assert_same([_make_span(events=[event])])

    def test_event_with_attributes(self) -> None:
        event = Event(
            name="db.query",
            attributes={"db.statement": "SELECT 1", "db.rows": 42},
            timestamp=_BASE_TS + 100,
        )
        _assert_same([_make_span(events=[event])])

    def test_multiple_events(self) -> None:
        events = [
            Event(name=f"event_{i}", timestamp=_BASE_TS + i * 1000)
            for i in range(10)
        ]
        _assert_same([_make_span(events=events)])

    def test_event_empty_attributes(self) -> None:
        event = Event(name="bare", attributes={}, timestamp=_BASE_TS + 1)
        _assert_same([_make_span(events=[event])])

    def test_event_no_attributes(self) -> None:
        event = Event(name="bare", timestamp=_BASE_TS + 1)
        _assert_same([_make_span(events=[event])])


class TestLinks:
    def test_single_link(self) -> None:
        link = Link(
            context=_make_context(
                trace_id=_LINK_TRACE_ID, span_id=_LINK_SPAN_ID
            ),
        )
        _assert_same([_make_span(links=[link])])

    def test_link_with_attributes(self) -> None:
        link = Link(
            context=_make_context(
                trace_id=_LINK_TRACE_ID,
                span_id=_LINK_SPAN_ID,
                is_remote=True,
            ),
            attributes={"link.reason": "retry"},
        )
        _assert_same([_make_span(links=[link])])

    def test_multiple_links(self) -> None:
        links = [
            Link(
                context=_make_context(
                    trace_id=_LINK_TRACE_ID, span_id=0x1000 + i
                ),
            )
            for i in range(5)
        ]
        _assert_same([_make_span(links=links)])

    def test_link_remote_context(self) -> None:
        link = Link(
            context=_make_context(
                trace_id=_LINK_TRACE_ID,
                span_id=_LINK_SPAN_ID,
                is_remote=True,
            ),
        )
        _assert_same([_make_span(links=[link])])

    def test_link_with_trace_state(self) -> None:
        link = Link(
            context=_make_context(
                trace_id=_LINK_TRACE_ID,
                span_id=_LINK_SPAN_ID,
                trace_state=TraceState([("vendor", "val")]),
            ),
        )
        _assert_same([_make_span(links=[link])])


class TestSpanContext:
    def test_sampled_flag(self) -> None:
        ctx = _make_context(trace_flags=TraceFlags(TraceFlags.SAMPLED))
        _assert_same([_make_span(context=ctx)])

    def test_default_flags(self) -> None:
        ctx = _make_context(trace_flags=TraceFlags(TraceFlags.DEFAULT))
        _assert_same([_make_span(context=ctx)])

    def test_remote_context(self) -> None:
        ctx = _make_context(is_remote=True)
        _assert_same([_make_span(context=ctx)])

    def test_with_trace_state(self) -> None:
        ts = TraceState([("rojo", "00f067aa0ba902b7"), ("congo", "t61rcWkgMzE")])
        ctx = _make_context(trace_state=ts)
        _assert_same([_make_span(context=ctx)])

    def test_empty_trace_state(self) -> None:
        ctx = _make_context(trace_state=TraceState())
        _assert_same([_make_span(context=ctx)])

    def test_with_parent(self) -> None:
        parent = _make_context(span_id=_PARENT_SPAN_ID, is_remote=True)
        _assert_same([_make_span(parent=parent)])

    def test_no_parent(self) -> None:
        _assert_same([_make_span(parent=None)])

    def test_min_ids(self) -> None:
        ctx = _make_context(trace_id=1, span_id=1)
        _assert_same([_make_span(context=ctx)])

    def test_max_ids(self) -> None:
        ctx = _make_context(
            trace_id=(2**128) - 1,
            span_id=(2**64) - 1,
        )
        _assert_same([_make_span(context=ctx)])


class TestResource:
    def test_resource_with_attributes(self) -> None:
        resource = _make_resource({"service.name": "my-svc", "version": 3})
        _assert_same([_make_span(resource=resource)])

    def test_resource_with_schema_url(self) -> None:
        resource = _make_resource(
            {"service.name": "svc"},
            schema_url="https://opentelemetry.io/schemas/1.0.0",
        )
        _assert_same([_make_span(resource=resource)])

    def test_empty_resource(self) -> None:
        resource = _make_resource()
        _assert_same([_make_span(resource=resource)])

    def test_resource_many_attributes(self) -> None:
        resource = _make_resource(
            {f"resource.attr.{i}": f"val_{i}" for i in range(50)}
        )
        _assert_same([_make_span(resource=resource)])


class TestInstrumentationScope:
    def test_scope_with_version(self) -> None:
        scope = _make_scope(name="my.lib", version="2.5.0")
        _assert_same([_make_span(instrumentation_scope=scope)])

    def test_scope_without_version(self) -> None:
        scope = _make_scope(name="my.lib", version=None)
        _assert_same([_make_span(instrumentation_scope=scope)])

    def test_scope_with_schema_url(self) -> None:
        scope = _make_scope(
            schema_url="https://opentelemetry.io/schemas/1.0.0"
        )
        _assert_same([_make_span(instrumentation_scope=scope)])

    def test_scope_with_attributes(self) -> None:
        scope = _make_scope(attrs={"scope.key": "scope.val"})
        _assert_same([_make_span(instrumentation_scope=scope)])


class TestGrouping:
    def test_same_resource_same_scope(self) -> None:
        resource = _make_resource({"service.name": "grouped"})
        scope = _make_scope(name="shared.scope")
        spans = [
            _make_span(
                name=f"span-{i}",
                context=_make_context(span_id=0x1000 + i),
                resource=resource,
                instrumentation_scope=scope,
            )
            for i in range(5)
        ]
        _assert_same(spans)

    def test_same_resource_different_scopes(self) -> None:
        resource = _make_resource({"service.name": "multi-scope"})
        spans = [
            _make_span(
                name=f"span-{i}",
                context=_make_context(span_id=0x2000 + i),
                resource=resource,
                instrumentation_scope=_make_scope(name=f"scope-{i % 3}"),
            )
            for i in range(9)
        ]
        _assert_same(spans)

    def test_different_resources(self) -> None:
        spans = [
            _make_span(
                name=f"span-{i}",
                context=_make_context(span_id=0x3000 + i),
                resource=_make_resource({f"svc-{i % 2}": "val"}),
            )
            for i in range(6)
        ]
        _assert_same(spans)

    def test_different_resources_different_scopes(self) -> None:
        spans = []
        for r_idx in range(3):
            resource = _make_resource({f"resource-{r_idx}": "yes"})
            for s_idx in range(2):
                scope = _make_scope(name=f"scope-{s_idx}")
                for sp_idx in range(4):
                    sid = r_idx * 100 + s_idx * 10 + sp_idx + 1
                    spans.append(
                        _make_span(
                            name=f"span-{sid}",
                            context=_make_context(span_id=sid),
                            resource=resource,
                            instrumentation_scope=scope,
                        )
                    )
        _assert_same(spans)


class TestLargeBatch:
    def test_100_spans(self) -> None:
        resource = _make_resource({"service.name": "bulk"})
        scope = _make_scope()
        spans = [
            _make_span(
                name=f"op-{i}",
                context=_make_context(span_id=i + 1),
                resource=resource,
                instrumentation_scope=scope,
                attributes={"index": i, "tag": f"val-{i}"},
            )
            for i in range(100)
        ]
        _assert_same(spans)

    def test_500_spans_varied(self) -> None:
        """500 spans across multiple resources, scopes, and kinds."""
        kinds = list(SpanKind)
        status_codes = [StatusCode.UNSET, StatusCode.OK, StatusCode.ERROR]
        resources = [
            _make_resource({f"svc": f"service-{i}"}) for i in range(5)
        ]
        scopes = [_make_scope(name=f"lib-{i}") for i in range(4)]

        spans = []
        for i in range(500):
            spans.append(
                _make_span(
                    name=f"span-{i}",
                    context=_make_context(span_id=i + 1),
                    resource=resources[i % len(resources)],
                    instrumentation_scope=scopes[i % len(scopes)],
                    kind=kinds[i % len(kinds)],
                    status=Status(status_codes[i % len(status_codes)]),
                    attributes={"i": i, "mod5": i % 5},
                    start_time=_BASE_TS + i * 1_000_000,
                    end_time=_BASE_TS + (i + 1) * 1_000_000,
                )
            )
        _assert_same(spans)


class TestFullyLoaded:
    def test_span_with_everything(self) -> None:
        """Single span with every field populated."""
        ctx = _make_context(
            trace_id=0xFFEEDDCCBBAA99887766554433221100,
            span_id=0xAABBCCDDEEFF0011,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState([("vendor1", "abc"), ("vendor2", "def")]),
        )
        parent = _make_context(
            trace_id=0xFFEEDDCCBBAA99887766554433221100,
            span_id=0x1111111111111111,
            is_remote=True,
        )
        resource = _make_resource(
            attrs={
                "service.name": "full-service",
                "service.version": "1.2.3",
                "host.name": "prod-01",
                "numeric": 42,
                "enabled": True,
                "ratio": 0.95,
            },
            schema_url="https://opentelemetry.io/schemas/1.21.0",
        )
        scope = _make_scope(
            name="full.instrumentation",
            version="3.0.0",
            schema_url="https://opentelemetry.io/schemas/1.21.0",
            attrs={"scope.attr": "scope_value"},
        )
        events = [
            Event(
                name="exception",
                attributes={
                    "exception.type": "ValueError",
                    "exception.message": "invalid input",
                    "exception.stacktrace": "Traceback...\n  File...",
                },
                timestamp=_BASE_TS + 500_000_000,
            ),
            Event(
                name="log",
                attributes={"log.severity": "WARN"},
                timestamp=_BASE_TS + 600_000_000,
            ),
        ]
        links = [
            Link(
                context=_make_context(
                    trace_id=_LINK_TRACE_ID,
                    span_id=_LINK_SPAN_ID,
                    is_remote=True,
                    trace_state=TraceState([("linked", "yes")]),
                ),
                attributes={"link.kind": "parent"},
            ),
            Link(
                context=_make_context(
                    trace_id=0x11111111111111111111111111111111,
                    span_id=0x2222222222222222,
                ),
            ),
        ]

        span = _make_span(
            name="full-operation",
            context=ctx,
            parent=parent,
            resource=resource,
            attributes={
                "http.method": "POST",
                "http.url": "https://example.com/api",
                "http.status_code": 201,
                "http.request.header.x-request-id": ["abc-123"],
                "custom.tags": ["alpha", "beta", "gamma"],
                "custom.scores": [1, 2, 3],
                "custom.ratios": [0.1, 0.5, 0.9],
                "custom.flags": [True, False],
            },
            events=events,
            links=links,
            kind=SpanKind.SERVER,
            status=Status(StatusCode.ERROR, "downstream timeout"),
            start_time=_BASE_TS,
            end_time=_BASE_TS + 2_000_000_000,
            instrumentation_scope=scope,
        )
        _assert_same([span])


class TestCombinations:
    """Test many (kind × status × attribute type) combinations."""

    @pytest.mark.parametrize(
        ("kind", "status_code"),
        list(itertools.product(SpanKind, StatusCode)),
    )
    def test_kind_cross_status(
        self, kind: SpanKind, status_code: StatusCode
    ) -> None:
        description = "err" if status_code == StatusCode.ERROR else None
        _assert_same(
            [_make_span(kind=kind, status=Status(status_code, description))]
        )

    @pytest.mark.parametrize(
        "attrs",
        [
            {"s": "str"},
            {"i": 1},
            {"f": 1.5},
            {"b": True},
            {"sl": ["a", "b"]},
            {"il": [1, 2]},
            {"fl": [1.0, 2.0]},
            {"bl": [True, False]},
            {"s": "str", "i": 1, "f": 1.5, "b": True},
        ],
    )
    def test_attribute_type_variants(self, attrs: dict) -> None:
        _assert_same([_make_span(attributes=attrs)])


class TestEdgeCases:
    def test_zero_duration_span(self) -> None:
        _assert_same([_make_span(start_time=_BASE_TS, end_time=_BASE_TS)])

    def test_span_name_with_special_chars(self) -> None:
        _assert_same([_make_span(name="foo/bar:baz.qux#123")])

    def test_span_name_unicode(self) -> None:
        _assert_same([_make_span(name="日本語スパン")])

    def test_span_name_empty(self) -> None:
        _assert_same([_make_span(name="")])

    def test_trace_state_multiple_entries(self) -> None:
        ts = TraceState(
            [("key1", "val1"), ("key2", "val2"), ("key3", "val3")]
        )
        ctx = _make_context(trace_state=ts)
        _assert_same([_make_span(context=ctx)])

    def test_attribute_long_string_value(self) -> None:
        _assert_same([_make_span(attributes={"big": "x" * 10_000})])

    def test_attribute_large_list(self) -> None:
        _assert_same(
            [_make_span(attributes={"many": list(range(1000))})]
        )

    def test_many_events(self) -> None:
        events = [
            Event(
                name=f"e{i}",
                attributes={"idx": i},
                timestamp=_BASE_TS + i,
            )
            for i in range(50)
        ]
        _assert_same([_make_span(events=events)])

    def test_many_links(self) -> None:
        links = [
            Link(
                context=_make_context(
                    trace_id=_LINK_TRACE_ID, span_id=i + 1
                ),
                attributes={"idx": i},
            )
            for i in range(50)
        ]
        _assert_same([_make_span(links=links)])
