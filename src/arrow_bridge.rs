//! Bridge between the arrow version used by rerun and the one used by
//! duckdb-rs, via the ABI-stable Arrow C Data Interface.
//!
//! The two crates track different arrow major versions, so their types are
//! distinct to the Rust type system even though the underlying memory layout
//! of the C data interface structs is identical by specification.

use std::error::Error;
use std::mem::transmute;

use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use duckdb::arrow as arrow_db;

const _: () =
    assert!(size_of::<FFI_ArrowArray>() == size_of::<arrow_db::ffi::FFI_ArrowArray>());
const _: () =
    assert!(size_of::<FFI_ArrowSchema>() == size_of::<arrow_db::ffi::FFI_ArrowSchema>());

type BoxError = Box<dyn Error>;

/// rerun-arrow array -> duckdb-arrow array (zero-copy).
pub fn array_to_db(array: &dyn Array) -> Result<arrow_db::array::ArrayRef, BoxError> {
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array.to_data())?;
    // SAFETY: both types are #[repr(C)] structs defined by the Arrow C data
    // interface specification; moving ownership across arrow versions is the
    // interface's purpose.
    let ffi_array: arrow_db::ffi::FFI_ArrowArray = unsafe { transmute(ffi_array) };
    let ffi_schema: arrow_db::ffi::FFI_ArrowSchema = unsafe { transmute(ffi_schema) };
    let data = unsafe { arrow_db::ffi::from_ffi(ffi_array, &ffi_schema)? };
    Ok(arrow_db::array::make_array(data))
}

/// rerun-arrow field -> duckdb-arrow field.
pub fn field_to_db(field: &arrow::datatypes::Field) -> Result<arrow_db::datatypes::Field, BoxError> {
    let ffi = FFI_ArrowSchema::try_from(field)?;
    // SAFETY: see `array_to_db`.
    let ffi: arrow_db::ffi::FFI_ArrowSchema = unsafe { transmute(ffi) };
    Ok(arrow_db::datatypes::Field::try_from(&ffi)?)
}
