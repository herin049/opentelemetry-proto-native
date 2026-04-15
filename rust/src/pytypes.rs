use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use pyo3::prelude::*;

/// Attributes extracted from any Python object implementing the Mapping protocol.
/// Values are kept as PyObject since AttributeValue is a union type
/// (str | int | float | bool | bytes | Sequence[...]).
/// Conversion to protobuf AnyValue happens during serialization.
///
/// Equality and hashing use Python object identity (`Py::as_ptr`) for values,
/// which works correctly for OTEL SDK objects where attribute values are shared.
pub(crate) struct Attributes(pub HashMap<String, Py<PyAny>>);

impl PartialEq for Attributes {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0.iter().all(|(k, v)| {
            other
                .0
                .get(k)
                .is_some_and(|ov| v.as_ptr() == ov.as_ptr())
        })
    }
}

impl Eq for Attributes {}

impl Hash for Attributes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.len().hash(state);
        let mut keys: Vec<_> = self.0.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(state);
            self.0[key].as_ptr().hash(state);
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for Attributes {
    type Error = PyErr;
    fn extract(ob: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let items = ob.call_method0("items")?;
        let mut map = HashMap::new();
        for item in items.try_iter()? {
            let pair = item?;
            let key: String = pair.get_item(0)?.extract()?;
            let value: Py<PyAny> = pair.get_item(1)?.unbind();
            map.insert(key, value);
        }
        Ok(Attributes(map))
    }
}

/// Maps Python SDK's `StatusCode` enum
#[derive(Debug, Clone, Copy)]
pub(crate) enum StatusCode {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl<'a, 'py> FromPyObject<'a, 'py> for StatusCode {
    type Error = PyErr;
    fn extract(ob: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let value: i32 = ob.getattr("value")?.extract()?;
        match value {
            0 => Ok(StatusCode::Unset),
            1 => Ok(StatusCode::Ok),
            2 => Ok(StatusCode::Error),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid StatusCode value: {value}"),
            )),
        }
    }
}

/// Maps Python SDK's `SpanKind` enum.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SpanKind {
    Internal = 0,
    Server = 1,
    Client = 2,
    Producer = 3,
    Consumer = 4,
}

impl<'a, 'py> FromPyObject<'a, 'py> for SpanKind {
    type Error = PyErr;
    fn extract(ob: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let value: i32 = ob.getattr("value")?.extract()?;
        match value {
            0 => Ok(SpanKind::Internal),
            1 => Ok(SpanKind::Server),
            2 => Ok(SpanKind::Client),
            3 => Ok(SpanKind::Producer),
            4 => Ok(SpanKind::Consumer),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid SpanKind value: {value}"),
            )),
        }
    }
}

/// Maps `opentelemetry.trace.span.SpanContext`.
#[derive(FromPyObject)]
pub(crate) struct SpanContext {
    pub trace_id: u128,
    pub span_id: u64,
    pub is_remote: bool,
    pub trace_flags: u8,
    pub trace_state: Py<PyAny>,
}

/// Maps `opentelemetry.trace.status.Status`.
#[derive(FromPyObject)]
pub(crate) struct Status {
    pub status_code: StatusCode,
    pub description: Option<String>,
}

/// Maps `opentelemetry.sdk.resources.Resource`.
#[derive(FromPyObject, Hash, PartialEq, Eq)]
pub(crate) struct Resource {
    pub attributes: Option<Attributes>,
    pub schema_url: String,
}

/// Maps `opentelemetry.sdk.util.instrumentation.InstrumentationScope`.
#[derive(FromPyObject, Hash, PartialEq, Eq)]
pub(crate) struct InstrumentationScope {
    pub name: String,
    pub version: Option<String>,
    pub schema_url: Option<String>,
    pub attributes: Option<Attributes>,
}

/// Maps `opentelemetry.sdk.trace.Event`.
#[derive(FromPyObject)]
pub(crate) struct Event {
    pub name: String,
    pub timestamp: u64,
    pub attributes: Option<Attributes>,
    pub dropped_attributes: u32,
}

/// Maps `opentelemetry.trace.Link`.
#[derive(FromPyObject)]
pub(crate) struct Link {
    pub context: SpanContext,
    pub attributes: Option<Attributes>,
    pub dropped_attributes: u32,
}

/// Maps `opentelemetry.sdk.trace.ReadableSpan`.
#[derive(FromPyObject)]
pub(crate) struct ReadableSpan {
    pub name: String,
    pub context: Option<SpanContext>,
    pub parent: Option<SpanContext>,
    pub resource: Resource,
    pub attributes: Option<Attributes>,
    pub events: Vec<Event>,
    pub links: Vec<Link>,
    pub kind: SpanKind,
    pub status: Status,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub instrumentation_scope: Option<InstrumentationScope>,
    pub dropped_attributes: u32,
    pub dropped_events: u32,
    pub dropped_links: u32,
}
