# opentelemetry-proto-native

A proof-of-concept Python package that serializes OpenTelemetry SDK spans to
OTLP protobuf bytes using Rust (via [Prost](https://github.com/tokio-rs/prost))

## Local Development 

### Prerequisites

- Python >= 3.9
- Rust toolchain (stable)
- [uv](https://github.com/astral-sh/uv)

### Build and install

```bash
git clone --recurse-submodules <repo-url>
cd opentelemetry-proto-native

uv sync
uv run maturin develop
```

## Usage

```python
from opentelemetry.proto_native import serialize_spans

# `spans` is a list of opentelemetry.sdk.trace.ReadableSpan objects,
# e.g. from a BatchSpanProcessor or SpanExporter.export() call.
otlp_bytes: bytes = serialize_spans(spans)
```

The returned bytes are a serialized `ExportTraceServiceRequest` protobuf
message, identical to what `opentelemetry-exporter-otlp-proto-common` produces.

## Testing
Tests can be ran with `pytest`
```bash
uv run pytest tests/test_serialize_spans.py -v
```

## Benchmarks

End-to-end serialization benchmarks compare the Rust and Python implementations.
```bash
# Run with side-by-side comparison
uv run pytest tests/bench_serialize_spans.py --benchmark-enable --benchmark-group-by=param -v

# Or save and compare separately
uv run pytest tests/bench_serialize_spans.py --benchmark-enable -k "TestRustSerialize" --benchmark-save=rust
uv run pytest tests/bench_serialize_spans.py --benchmark-enable -k "TestPythonSerialize" --benchmark-save=python
uv run pytest-benchmark compare 0001_rust 0002_python --group-by=param
```

### Results (MacBook M5 Pro)

| Dataset        | Batch size | Rust (mean) | Python (mean) | Speedup |
|----------------|------------|-------------|---------------|---------|
| minimal        | 1          | 3.01 us     | 6.71 us       | 2.2x    |
| minimal        | 100        | 242.2 us    | 321.6 us      | 1.3x    |
| minimal        | 1000       | 2.38 ms     | 3.42 ms       | 1.4x    |
| simple_attrs   | 1          | 4.19 us     | 14.44 us      | 3.4x    |
| simple_attrs   | 100        | 352.4 us    | 797.9 us      | 2.3x    |
| simple_attrs   | 1000       | 3.45 ms     | 8.06 ms       | 2.3x    |
| heavy_attrs    | 1          | 12.17 us    | 60.76 us      | 5.0x    |
| heavy_attrs    | 100        | 1.04 ms     | 3.76 ms       | 3.6x    |
| heavy_attrs    | 1000       | 10.51 ms    | 38.42 ms      | 3.7x    |
| with_events    | 100        | 581.6 us    | 1.78 ms       | 3.1x    |
| with_events    | 1000       | 5.87 ms     | 18.06 ms      | 3.1x    |
| with_links     | 100        | 445.9 us    | 950.7 us      | 2.1x    |
| with_links     | 1000       | 4.41 ms     | 9.90 ms       | 2.2x    |
| fully_loaded   | 100        | 656.7 us    | 2.25 ms       | 3.4x    |
| fully_loaded   | 1000       | 6.63 ms     | 22.92 ms      | 3.5x    |
| multi_resource | 100        | 278.6 us    | 487.4 us      | 1.8x    |
| multi_resource | 1000       | 2.75 ms     | 4.62 ms       | 1.7x    |

### Package Sizes 

The Rust wheel (279 KB) replaces all three Python packages (506 KB combined), a ~45% reduction in distribution size.

| Package                                    | Wheel size |
|--------------------------------------------|------------|
| **opentelemetry-proto-native** (this)      | **279 KB** |
| protobuf (Google)                          | 418 KB     |
| opentelemetry-proto (pb2 files)            | 70 KB      |
| opentelemetry-exporter-otlp-proto-common   | 18 KB      |
| **Python total**                           | **506 KB** |


