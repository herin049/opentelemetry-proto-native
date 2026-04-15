from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan

def serialize_spans(spans: list[ReadableSpan]) -> bytes: ...
