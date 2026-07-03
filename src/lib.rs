use duckdb::{duckdb_entrypoint_c_api, Connection, Result};
use std::error::Error;

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(_con: Connection) -> Result<(), Box<dyn Error>> {
    Ok(())
}
