use pyo3::prelude::*;

pub(crate) mod encode;
pub(crate) mod proto;
pub(crate) mod pytypes;

#[pymodule]
#[pyo3(name = "_rs")]
fn otlp_proto(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(encode::traces::serialize_spans))?;
    Ok(())
}
