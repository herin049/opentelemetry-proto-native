use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::proto;
use crate::pytypes;

pub(crate) mod traces;

#[derive(Debug)]
pub(crate) enum EncodeError {
    UnsupportedType(String),
    ExtractionFailed(String),
    TraceStateError(String),
}

impl From<EncodeError> for PyErr {
    fn from(err: EncodeError) -> PyErr {
        match err {
            EncodeError::UnsupportedType(t) => {
                PyValueError::new_err(format!("unsupported attribute type: {t}"))
            }
            EncodeError::ExtractionFailed(msg) => {
                PyValueError::new_err(format!("extraction failed: {msg}"))
            }
            EncodeError::TraceStateError(msg) => {
                PyValueError::new_err(format!("trace state error: {msg}"))
            }
        }
    }
}

pub(crate) type EncodeResult<T> = Result<T, EncodeError>;

pub(crate) fn encode_resource(
    py: Python<'_>,
    resource: &pytypes::Resource,
) -> EncodeResult<proto::resource::v1::Resource> {
    Ok(proto::resource::v1::Resource {
        attributes: encode_attributes(py, &resource.attributes)?,
        ..Default::default()
    })
}

pub(crate) fn encode_instrumentation_scope(
    py: Python<'_>,
    scope: &pytypes::InstrumentationScope,
) -> EncodeResult<proto::common::v1::InstrumentationScope> {
    Ok(proto::common::v1::InstrumentationScope {
        name: scope.name.clone(),
        version: scope.version.clone().unwrap_or_default(),
        attributes: encode_attributes(py, &scope.attributes)?,
        ..Default::default()
    })
}

pub(crate) fn encode_attributes(
    py: Python<'_>,
    attrs: &Option<pytypes::Attributes>,
) -> EncodeResult<Vec<proto::common::v1::KeyValue>> {
    let Some(attrs) = attrs else {
        return Ok(Vec::new());
    };
    attrs
        .0
        .iter()
        .map(|(key, value)| {
            Ok(proto::common::v1::KeyValue {
                key: key.clone(),
                value: Some(encode_any_value(py, value)?),
                ..Default::default()
            })
        })
        .collect()
}

fn encode_any_value(
    py: Python<'_>,
    value: &Py<PyAny>,
) -> EncodeResult<proto::common::v1::AnyValue> {
    use proto::common::v1::{any_value, AnyValue, ArrayValue};

    let obj = value.bind(py);

    // Check bool before int — Python bool is a subclass of int
    let v = if obj.is_instance_of::<PyBool>() {
        any_value::Value::BoolValue(obj.extract::<bool>().map_err(|e| {
            EncodeError::ExtractionFailed(format!("bool: {e}"))
        })?)
    } else if obj.is_instance_of::<PyString>() {
        any_value::Value::StringValue(obj.extract::<String>().map_err(|e| {
            EncodeError::ExtractionFailed(format!("string: {e}"))
        })?)
    } else if obj.is_instance_of::<PyInt>() {
        any_value::Value::IntValue(obj.extract::<i64>().map_err(|e| {
            EncodeError::ExtractionFailed(format!("int: {e}"))
        })?)
    } else if obj.is_instance_of::<PyFloat>() {
        any_value::Value::DoubleValue(obj.extract::<f64>().map_err(|e| {
            EncodeError::ExtractionFailed(format!("float: {e}"))
        })?)
    } else if obj.is_instance_of::<PyBytes>() {
        any_value::Value::BytesValue(obj.extract::<Vec<u8>>().map_err(|e| {
            EncodeError::ExtractionFailed(format!("bytes: {e}"))
        })?)
    } else if let Ok(list) = obj.cast::<PyList>() {
        let values: Vec<AnyValue> = list
            .iter()
            .map(|item| encode_any_value(py, &item.unbind()))
            .collect::<EncodeResult<_>>()?;
        any_value::Value::ArrayValue(ArrayValue { values })
    } else if let Ok(tuple) = obj.cast::<PyTuple>() {
        let values: Vec<AnyValue> = tuple
            .iter()
            .map(|item| encode_any_value(py, &item.unbind()))
            .collect::<EncodeResult<_>>()?;
        any_value::Value::ArrayValue(ArrayValue { values })
    } else {
        let type_name = obj
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "<unknown>".to_string());
        return Err(EncodeError::UnsupportedType(type_name));
    };

    Ok(AnyValue { value: Some(v) })
}

pub(crate) fn encode_trace_id(trace_id: u128) -> Vec<u8> {
    trace_id.to_be_bytes().to_vec()
}

pub(crate) fn encode_span_id(span_id: u64) -> Vec<u8> {
    span_id.to_be_bytes().to_vec()
}

pub(crate) fn encode_trace_state(
    py: Python<'_>,
    trace_state: &Py<PyAny>,
) -> EncodeResult<String> {
    trace_state
        .bind(py)
        .call_method0("to_header")
        .and_then(|s| s.extract::<String>())
        .map_err(|e| EncodeError::TraceStateError(format!("{e}")))
}
