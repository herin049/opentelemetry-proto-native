from __future__ import annotations

from typing import Sequence

import pytest
from opentelemetry.exporter.otlp.proto.common._internal.trace_encoder import (
    encode_spans as py_encode_spans,
)
from opentelemetry.proto_native import serialize_spans as rust_serialize_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import SpanContext, TraceFlags, TraceState
from opentelemetry.trace.status import Status, StatusCode

_TRACE_ID = 0x12345678901234567890123456789012
_SPAN_ID = 0x1234567890123456
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
) -> Resource:
    return Resource(attributes=attrs or {})


def _make_scope(
    name: str = "bench.scope",
    version: str | None = "1.0.0",
) -> InstrumentationScope:
    return InstrumentationScope(name=name, version=version)


def _make_span(
    name: str = "bench-span",
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


def _py_serialize(spans: list[ReadableSpan]) -> bytes:
    """End-to-end Python serialization: encode + SerializeToString."""
    return py_encode_spans(spans).SerializeToString()


def _rust_serialize(spans: list[ReadableSpan]) -> bytes:
    """End-to-end Rust serialization."""
    return rust_serialize_spans(spans)


def _minimal_spans(n: int) -> list[ReadableSpan]:
    """Spans with no attributes, events, or links."""
    resource = _make_resource()
    scope = _make_scope()
    return [
        _make_span(
            name=f"span-{i}",
            context=_make_context(span_id=i + 1),
            resource=resource,
            instrumentation_scope=scope,
        )
        for i in range(n)
    ]


def _simple_attributed_spans(n: int) -> list[ReadableSpan]:
    """Spans with a handful of mixed-type attributes."""
    resource = _make_resource({"service.name": "bench-svc", "version": 1})
    scope = _make_scope()
    return [
        _make_span(
            name=f"op-{i}",
            context=_make_context(span_id=i + 1),
            resource=resource,
            instrumentation_scope=scope,
            attributes={
                "http.method": "GET",
                "http.status_code": 200,
                "http.url": f"https://example.com/api/{i}",
                "success": True,
                "latency_ms": 42.5 + i,
            },
        )
        for i in range(n)
    ]


def _heavy_attributed_spans(n: int) -> list[ReadableSpan]:
    """Spans with many attributes including sequences."""
    resource = _make_resource(
        {f"resource.attr.{j}": f"val-{j}" for j in range(20)}
    )
    scope = _make_scope(name="heavy.lib", version="2.0.0")
    return [
        _make_span(
            name=f"heavy-{i}",
            context=_make_context(span_id=i + 1),
            resource=resource,
            instrumentation_scope=scope,
            attributes={
                "str_attr": "hello world",
                "int_attr": i * 100,
                "float_attr": 3.14159,
                "bool_attr": i % 2 == 0,
                "str_list": ["alpha", "beta", "gamma", "delta"],
                "int_list": [10, 20, 30, 40, 50],
                "float_list": [1.1, 2.2, 3.3],
                "bool_list": [True, False, True],
                **{f"extra_{j}": f"value_{j}" for j in range(20)},
            },
        )
        for i in range(n)
    ]


def _spans_with_events(n: int) -> list[ReadableSpan]:
    """Spans each carrying 5 events with attributes."""
    resource = _make_resource({"service.name": "event-svc"})
    scope = _make_scope()
    return [
        _make_span(
            name=f"evented-{i}",
            context=_make_context(span_id=i + 1),
            resource=resource,
            instrumentation_scope=scope,
            attributes={"idx": i},
            events=[
                Event(
                    name=f"event-{j}",
                    attributes={"event.idx": j, "msg": f"happened-{j}"},
                    timestamp=_BASE_TS + j * 1000,
                )
                for j in range(5)
            ],
        )
        for i in range(n)
    ]


def _spans_with_links(n: int) -> list[ReadableSpan]:
    """Spans each carrying 3 links with attributes."""
    resource = _make_resource({"service.name": "link-svc"})
    scope = _make_scope()
    return [
        _make_span(
            name=f"linked-{i}",
            context=_make_context(span_id=i + 1),
            resource=resource,
            instrumentation_scope=scope,
            links=[
                Link(
                    context=_make_context(
                        trace_id=_LINK_TRACE_ID, span_id=i * 10 + j + 1
                    ),
                    attributes={"link.idx": j},
                )
                for j in range(3)
            ],
        )
        for i in range(n)
    ]


def _fully_loaded_spans(n: int) -> list[ReadableSpan]:
    """Spans with all fields populated: attributes, events, links, status."""
    resource = _make_resource(
        {
            "service.name": "full-svc",
            "service.version": "3.2.1",
            "host.name": "bench-host",
            "deploy.env": "staging",
        }
    )
    scope = _make_scope(name="full.lib", version="1.0.0")
    parent = _make_context(span_id=0xFFFF, is_remote=True)

    return [
        _make_span(
            name=f"full-{i}",
            context=_make_context(span_id=i + 1),
            parent=parent,
            resource=resource,
            instrumentation_scope=scope,
            kind=list(SpanKind)[i % 5],
            status=Status(
                StatusCode.ERROR if i % 3 == 0 else StatusCode.OK,
                "error msg" if i % 3 == 0 else None,
            ),
            attributes={
                "http.method": "POST",
                "http.url": f"https://api.example.com/v2/resource/{i}",
                "http.status_code": 200 + (i % 5),
                "custom.tags": ["a", "b", "c"],
                "custom.scores": [1, 2, 3, 4, 5],
                "custom.enabled": True,
                "custom.ratio": 0.95,
            },
            events=[
                Event(
                    name="exception",
                    attributes={
                        "exception.type": "RuntimeError",
                        "exception.message": f"msg-{i}",
                    },
                    timestamp=_BASE_TS + 500_000,
                ),
                Event(
                    name="retry",
                    attributes={"attempt": 2},
                    timestamp=_BASE_TS + 600_000,
                ),
            ],
            links=[
                Link(
                    context=_make_context(
                        trace_id=_LINK_TRACE_ID,
                        span_id=i + 1000,
                        is_remote=True,
                    ),
                    attributes={"link.type": "causal"},
                ),
            ],
            start_time=_BASE_TS + i * 1_000_000,
            end_time=_BASE_TS + (i + 1) * 1_000_000,
        )
        for i in range(n)
    ]


def _multi_resource_spans(n: int) -> list[ReadableSpan]:
    """Spans spread across 10 resources and 5 scopes."""
    resources = [
        _make_resource({f"service.name": f"svc-{r}"}) for r in range(10)
    ]
    scopes = [_make_scope(name=f"lib-{s}") for s in range(5)]
    return [
        _make_span(
            name=f"multi-{i}",
            context=_make_context(span_id=i + 1),
            resource=resources[i % len(resources)],
            instrumentation_scope=scopes[i % len(scopes)],
            attributes={"i": i},
        )
        for i in range(n)
    ]


_DATASET_BUILDERS: dict[str, type[...]] = {
    "minimal": _minimal_spans,
    "simple_attrs": _simple_attributed_spans,
    "heavy_attrs": _heavy_attributed_spans,
    "with_events": _spans_with_events,
    "with_links": _spans_with_links,
    "fully_loaded": _fully_loaded_spans,
    "multi_resource": _multi_resource_spans,
}

_BATCH_SIZES = [1, 10, 100, 500, 1000]


def _dataset_id(params: tuple[str, int]) -> str:
    name, size = params
    return f"{name}_{size}"


_ALL_PARAMS: list[tuple[str, int]] = [
    (name, size) for name in _DATASET_BUILDERS for size in _BATCH_SIZES
]


@pytest.fixture(params=_ALL_PARAMS, ids=[_dataset_id(p) for p in _ALL_PARAMS])
def span_batch(request: pytest.FixtureRequest) -> list[ReadableSpan]:
    """Pre-built span batch — dataset name x batch size."""
    name, size = request.param
    return _DATASET_BUILDERS[name](size)


class TestRustSerialize:
    """Benchmarks for the Rust (prost) serializer."""

    def test_serialize(
        self,
        benchmark: pytest.fixture,
        span_batch: list[ReadableSpan],
    ) -> None:
        benchmark(_rust_serialize, span_batch)


class TestPythonSerialize:
    """Benchmarks for the Python (protobuf) serializer."""

    def test_serialize(
        self,
        benchmark: pytest.fixture,
        span_batch: list[ReadableSpan],
    ) -> None:
        benchmark(_py_serialize, span_batch)
