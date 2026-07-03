//! DuckDB extension for reading and writing Rerun `.rrd` recording files.

mod arrow_bridge;
mod read;
mod store;
mod write;

use std::error::Error;
use std::ffi::CString;

use duckdb::{Connection, ffi};

/// Must match the DuckDB version the extension is built against
/// (`TARGET_DUCKDB_VERSION` in the Makefile, forwarded by the build).
const MIN_DUCKDB_VERSION: &str = match option_env!("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION") {
    Some(version) => version,
    None => "v1.5.4",
};

/// The body of the extension entrypoint.
///
/// This is spelled out by hand instead of via `#[duckdb_entrypoint_c_api]`
/// because registering the COPY function needs the raw `duckdb_connection`,
/// which the macro does not expose.
///
/// # Safety
/// Called by DuckDB with valid `info`/`access` pointers.
unsafe fn init(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    unsafe {
        let have_api_struct = ffi::duckdb_rs_extension_api_init(info, access, MIN_DUCKDB_VERSION)?;
        if !have_api_struct {
            // Likely an API version mismatch; DuckDB already knows the reason.
            return Ok(false);
        }

        let get_database = (*access)
            .get_database
            .ok_or("get_database function pointer is null in duckdb_extension_access")?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(false);
        }
        let db: ffi::duckdb_database = *db_ptr;

        let connection = Connection::open_from_raw(db.cast())?;
        connection.register_table_function::<read::ReadRrd>("read_rrd")?;
        connection.register_table_function::<read::RrdEntities>("rrd_entities")?;
        connection.register_table_function::<read::RrdSchema>("rrd_schema")?;
        connection.register_table_function::<read::RrdRecordings>("rrd_recordings")?;

        let mut raw_connection: ffi::duckdb_connection = std::ptr::null_mut();
        if ffi::duckdb_connect(db, &mut raw_connection) != ffi::DuckDBSuccess {
            return Err("failed to open a connection to register the rrd copy function".into());
        }
        // Hands the connection over; it stays open on purpose (see write.rs).
        if let Err(err) = write::register_rrd_copy(raw_connection) {
            ffi::duckdb_disconnect(&mut raw_connection);
            return Err(err);
        }

        Ok(true)
    }
}

/// Entrypoint called by DuckDB when loading the extension.
///
/// # Safety
/// Called by DuckDB with valid `info`/`access` pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    match unsafe { init(info, access) } {
        Ok(ok) => ok,
        Err(err) => {
            unsafe {
                if let Some(set_error) = (*access).set_error {
                    let message = CString::new(err.to_string())
                        .unwrap_or_else(|_| c"rrd extension initialization failed".to_owned());
                    set_error(info, message.as_ptr());
                }
            }
            false
        }
    }
}
