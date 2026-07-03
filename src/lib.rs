//! DuckDB extension for reading Rerun `.rrd` recording files.

mod arrow_bridge;
mod read;
mod store;

use std::error::Error;

use duckdb::{Connection, Result, duckdb_entrypoint_c_api};

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<read::ReadRrd>("read_rrd")?;
    con.register_table_function::<read::RrdEntities>("rrd_entities")?;
    con.register_table_function::<read::RrdSchema>("rrd_schema")?;
    con.register_table_function::<read::RrdRecordings>("rrd_recordings")?;
    Ok(())
}
