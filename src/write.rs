//! `COPY ... TO 'file.rrd' (FORMAT rrd)`: write query results as a Rerun
//! recording.
//!
//! duckdb-rs does not wrap DuckDB's copy-function C API yet, so this module
//! talks to `duckdb::ffi` directly.
//!
//! Options:
//! - `ENTITY '/path'` (required): entity the components are logged to.
//! - `TIMELINE 'name'` (default `'index'`): name of the index timeline. The
//!   column with this name (see `COLUMNS`) is the index; integer columns
//!   become a sequence timeline, TIMESTAMP columns a timestamp timeline.
//!   `TIMELINE ''` writes static data (no index column).
//! - `COLUMNS 'a,b,c'`: column names, positionally. The C API does not expose
//!   the sink's column names, only their types, so they must be restated to
//!   be preserved. Without it, column 0 is the index and the rest are named
//!   `col_1..col_N`.

use std::error::Error;
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Arc;
use std::sync::Mutex;

use duckdb::ffi;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, TimeUnit};

use re_chunk::external::nohash_hasher::IntMap;
use re_chunk::{Chunk, ChunkComponents, ChunkId, EntityPath, RowId, TimeColumn, Timeline, TimelineName};
use re_log_encoding::Encoder;
use re_log_types::{LogMsg, SetStoreInfo, StoreId, StoreInfo, StoreKind, StoreSource};
use re_types_core::{ComponentDescriptor, SerializedComponentColumn};

type BoxError = Box<dyn Error>;

// === state ==================================================================

struct CopyBindData {
    entity: String,
    /// Timeline name; `None` means static data (no index column).
    timeline: Option<String>,
    /// One name per sink column, positionally.
    column_names: Vec<String>,
    /// Index into `column_names`, if a column acts as the index.
    time_column: Option<usize>,
}

struct CopyGlobalState {
    path: String,
    inner: Mutex<CopyWriter>,
}

struct CopyWriter {
    /// In-memory encoder; the file is written on finalize.
    encoder: Option<Encoder<Vec<u8>>>,
    store_id: StoreId,
}

// === registration ===========================================================

/// Owned by the copy function; carries connection-level arrow export options
/// into the sink callback (the C API only exposes an options getter on
/// connections).
///
/// The arrow options hold a *non-owning* pointer to the connection's client
/// context (`ClientProperties::client_context`), which DuckDB dereferences in
/// `duckdb_data_chunk_to_arrow`. The connection must therefore outlive every
/// COPY, i.e. live as long as the database — extensions cannot be unloaded,
/// so it is deliberately never disconnected.
struct CopyExtraInfo {
    arrow_options: ffi::duckdb_arrow_options,
    _connection: ffi::duckdb_connection,
}

impl Drop for CopyExtraInfo {
    fn drop(&mut self) {
        // Runs at database teardown. Only the options are freed; disconnecting
        // here would re-enter database locks mid-teardown.
        unsafe { ffi::duckdb_destroy_arrow_options(&mut self.arrow_options) }
    }
}

/// Registers the `rrd` COPY format. Takes ownership of `con` (see
/// [`CopyExtraInfo`] for why it is kept open).
pub unsafe fn register_rrd_copy(con: ffi::duckdb_connection) -> Result<(), BoxError> {
    unsafe {
        let mut arrow_options: ffi::duckdb_arrow_options = std::ptr::null_mut();
        ffi::duckdb_connection_get_arrow_options(con, &mut arrow_options);
        if arrow_options.is_null() {
            return Err("duckdb_connection_get_arrow_options returned null".into());
        }
        let extra_info = Box::new(CopyExtraInfo {
            arrow_options,
            _connection: con,
        });

        let mut function = ffi::duckdb_create_copy_function();
        if function.is_null() {
            return Err("duckdb_create_copy_function returned null".into());
        }
        ffi::duckdb_copy_function_set_name(function, c"rrd".as_ptr());
        ffi::duckdb_copy_function_set_bind(function, Some(copy_bind));
        ffi::duckdb_copy_function_set_global_init(function, Some(copy_global_init));
        ffi::duckdb_copy_function_set_sink(function, Some(copy_sink));
        ffi::duckdb_copy_function_set_finalize(function, Some(copy_finalize));
        ffi::duckdb_copy_function_set_extra_info(
            function,
            Box::into_raw(extra_info).cast(),
            Some(destroy_box::<CopyExtraInfo>),
        );
        let state = ffi::duckdb_register_copy_function(con, function);
        ffi::duckdb_destroy_copy_function(&mut function);
        if state != ffi::DuckDBSuccess {
            return Err("failed to register rrd copy function".into());
        }
    }
    Ok(())
}

unsafe extern "C" fn destroy_box<T>(ptr: *mut c_void) {
    unsafe { drop(Box::<T>::from_raw(ptr.cast())) }
}

/// Runs a callback body, converting panics into `Err` so they never unwind
/// across the `extern "C"` boundary (which would abort the process).
fn no_unwind<T>(body: impl FnOnce() -> Result<T, BoxError>) -> Result<T, BoxError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(result) => result,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic in rrd copy function".to_owned());
            Err(format!("internal error: {msg}").into())
        }
    }
}

fn set_error_c(msg: &str, set: impl FnOnce(*const c_char)) {
    let msg = CString::new(msg).unwrap_or_else(|_| c"invalid error message".to_owned());
    set(msg.as_ptr());
}

// === options parsing ========================================================

/// Extracts a `duckdb_value` as a string, unwrapping single-element lists
/// (COPY options may arrive as value lists).
unsafe fn option_value_to_string(value: ffi::duckdb_value) -> Option<String> {
    unsafe {
        let type_id = {
            let mut logical_type = ffi::duckdb_get_value_type(value);
            let id = ffi::duckdb_get_type_id(logical_type);
            // NOTE: `duckdb_get_value_type` borrows; do not destroy. Binding
            // returns it by value, so silence the unused warning instead.
            let _ = &mut logical_type;
            id
        };
        if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST {
            let len = ffi::duckdb_get_list_size(value);
            if len == 0 {
                return Some(String::new());
            }
            let mut child = ffi::duckdb_get_list_child(value, 0);
            let out = option_value_to_string(child);
            ffi::duckdb_destroy_value(&mut child);
            return out;
        }
        let chars = ffi::duckdb_get_varchar(value);
        if chars.is_null() {
            return None;
        }
        let out = CStr::from_ptr(chars).to_string_lossy().into_owned();
        ffi::duckdb_free(chars.cast());
        Some(out)
    }
}

/// COPY options arrive as a STRUCT value (one field per option, names
/// uppercased by DuckDB), or as a NULL value when no options were given.
unsafe fn parse_options(
    info: ffi::duckdb_copy_function_bind_info,
) -> Result<Vec<(String, String)>, BoxError> {
    unsafe {
        let mut options = ffi::duckdb_copy_function_bind_get_options(info);
        if options.is_null() || ffi::duckdb_is_null_value(options) {
            if !options.is_null() {
                ffi::duckdb_destroy_value(&mut options);
            }
            return Ok(Vec::new());
        }

        let mut logical_type = ffi::duckdb_get_value_type(options);
        let field_count = ffi::duckdb_struct_type_child_count(logical_type);

        let mut out = Vec::new();
        for i in 0..field_count {
            let name_ptr = ffi::duckdb_struct_type_child_name(logical_type, i);
            let key = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
            ffi::duckdb_free(name_ptr.cast());

            let mut value = ffi::duckdb_get_struct_child(options, i);
            let value_str = option_value_to_string(value);
            ffi::duckdb_destroy_value(&mut value);

            out.push((
                key.to_lowercase(),
                value_str.ok_or_else(|| format!("option {key:?} has no value"))?,
            ));
        }

        // NOTE: `duckdb_get_value_type` borrows from the value, don't destroy.
        let _ = &mut logical_type;
        ffi::duckdb_destroy_value(&mut options);
        Ok(out)
    }
}

// === bind ===================================================================

unsafe fn copy_bind_inner(
    info: ffi::duckdb_copy_function_bind_info,
) -> Result<CopyBindData, BoxError> {
    let column_count = unsafe { ffi::duckdb_copy_function_bind_get_column_count(info) } as usize;
    if column_count == 0 {
        return Err("COPY TO (FORMAT rrd) requires at least one column".into());
    }

    let mut entity = None;
    let mut timeline: Option<String> = None;
    let mut columns: Option<Vec<String>> = None;

    for (key, value) in unsafe { parse_options(info)? } {
        match key.as_str() {
            "entity" => entity = Some(value),
            "timeline" => timeline = Some(value),
            "columns" => {
                columns = Some(
                    value
                        .split(',')
                        .map(|name| name.trim().to_owned())
                        .collect(),
                );
            }
            other => {
                return Err(format!(
                    "unknown option {other:?} for COPY TO (FORMAT rrd); \
                     supported: ENTITY, TIMELINE, COLUMNS"
                )
                .into());
            }
        }
    }

    let entity =
        entity.ok_or("COPY TO (FORMAT rrd) requires the ENTITY option, e.g. ENTITY '/metrics'")?;

    // `TIMELINE ''` means: everything is static.
    let timeline = match timeline {
        Some(name) if name.is_empty() => None,
        Some(name) => Some(name),
        None => Some("index".to_owned()),
    };

    let (column_names, time_column) = match columns {
        Some(names) => {
            if names.len() != column_count {
                return Err(format!(
                    "COLUMNS names {} columns but the query produces {column_count}",
                    names.len()
                )
                .into());
            }
            let time_column = match &timeline {
                Some(timeline_name) => Some(
                    names
                        .iter()
                        .position(|name| name == timeline_name)
                        .ok_or_else(|| {
                            format!(
                                "no column named {timeline_name:?} in COLUMNS to use as the \
                                 index; add it, set TIMELINE to an existing column, or use \
                                 TIMELINE '' for static data"
                            )
                        })?,
                ),
                None => None,
            };
            (names, time_column)
        }
        None => match &timeline {
            Some(timeline_name) => {
                let mut names = vec![timeline_name.clone()];
                names.extend((1..column_count).map(|i| format!("col_{i}")));
                (names, Some(0))
            }
            None => ((0..column_count).map(|i| format!("col_{i}")).collect(), None),
        },
    };

    if time_column.is_some() && column_count == 1 {
        return Err("only an index column and no components; nothing to write".into());
    }

    Ok(CopyBindData {
        entity,
        timeline,
        column_names,
        time_column,
    })
}

unsafe extern "C" fn copy_bind(info: ffi::duckdb_copy_function_bind_info) {
    match no_unwind(|| unsafe { copy_bind_inner(info) }) {
        Ok(bind_data) => unsafe {
            ffi::duckdb_copy_function_bind_set_bind_data(
                info,
                Box::into_raw(Box::new(bind_data)).cast(),
                Some(destroy_box::<CopyBindData>),
            );
        },
        Err(err) => set_error_c(&err.to_string(), |msg| unsafe {
            ffi::duckdb_copy_function_bind_set_error(info, msg);
        }),
    }
}

// === global init ============================================================

unsafe extern "C" fn copy_global_init(info: ffi::duckdb_copy_function_global_init_info) {
    let result: Result<CopyGlobalState, BoxError> = no_unwind(|| unsafe {
        (|| {
            let path_ptr = ffi::duckdb_copy_function_global_init_get_file_path(info);
            if path_ptr.is_null() {
                return Err("no output file path".into());
            }
            let path = CStr::from_ptr(path_ptr).to_string_lossy().into_owned();

            let store_id = StoreId::random(StoreKind::Recording, "duckdb");
            let mut encoder = Encoder::local()?;
            encoder.append(&LogMsg::SetStoreInfo(SetStoreInfo {
                row_id: *RowId::new(),
                info: StoreInfo {
                    store_id: store_id.clone(),
                    cloned_from: None,
                    store_source: StoreSource::Other("duckdb-rrd".to_owned()),
                    store_version: None,
                },
            }))?;

            Ok(CopyGlobalState {
                path,
                inner: Mutex::new(CopyWriter {
                    encoder: Some(encoder),
                    store_id,
                }),
            })
        })()
    });

    match result {
        Ok(state) => unsafe {
            ffi::duckdb_copy_function_global_init_set_global_state(
                info,
                Box::into_raw(Box::new(state)).cast(),
                Some(destroy_box::<CopyGlobalState>),
            );
        },
        Err(err) => set_error_c(&err.to_string(), |msg| unsafe {
            ffi::duckdb_copy_function_global_init_set_error(info, msg);
        }),
    }
}

// === sink ===================================================================

/// Renames the list-item field to arrow's conventional `item` (DuckDB's arrow
/// export calls it `l`), so written files match what rerun tooling produces.
fn normalize_list_item_field(list: ListArray) -> ListArray {
    let (field, offsets, values, nulls) = list.into_parts();
    if field.name() == "item" {
        return ListArray::new(field, offsets, values, nulls);
    }
    let field = Arc::new(Field::new(
        "item",
        field.data_type().clone(),
        field.is_nullable(),
    ));
    ListArray::new(field, offsets, values, nulls)
}

/// Wraps per-row values into single-instance component batches, or reuses the
/// row's own list for LIST-typed columns.
fn column_to_component_lists(column: &ArrayRef) -> Result<ListArray, BoxError> {
    match column.data_type() {
        DataType::List(_) => Ok(normalize_list_item_field(column.as_list::<i32>().clone())),
        DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let target = DataType::List(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )));
            Ok(normalize_list_item_field(
                cast(column, &target)?.as_list::<i32>().clone(),
            ))
        }
        _ => {
            let field = Arc::new(Field::new("item", column.data_type().clone(), true));
            let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, column.len()));
            Ok(ListArray::new(
                field,
                offsets,
                column.clone(),
                column.nulls().cloned(),
            ))
        }
    }
}

fn index_column_to_time_column(
    column: &ArrayRef,
    timeline_name: &str,
) -> Result<TimeColumn, BoxError> {
    if column.null_count() > 0 {
        return Err(format!("index column {timeline_name:?} contains NULLs").into());
    }
    let (timeline, times) = match column.data_type() {
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => {
            let nanos = cast(
                &cast(column, &DataType::Timestamp(TimeUnit::Nanosecond, None))?,
                &DataType::Int64,
            )?;
            (
                Timeline::new_timestamp(timeline_name),
                nanos.as_primitive::<arrow::datatypes::Int64Type>().clone(),
            )
        }
        data_type if data_type.is_integer() => {
            let ints: Int64Array = cast(column, &DataType::Int64)?
                .as_primitive::<arrow::datatypes::Int64Type>()
                .clone();
            (Timeline::new_sequence(timeline_name), ints)
        }
        other => {
            return Err(format!(
                "index column {timeline_name:?} must be an integer or timestamp, got {other}"
            )
            .into());
        }
    };
    Ok(TimeColumn::new(None, timeline, times.values().clone()))
}

unsafe fn check_error_data(error_data: ffi::duckdb_error_data) -> Result<(), BoxError> {
    unsafe {
        if error_data.is_null() {
            return Ok(());
        }
        let mut error_data = error_data;
        let result = if ffi::duckdb_error_data_has_error(error_data) {
            let message = CStr::from_ptr(ffi::duckdb_error_data_message(error_data))
                .to_string_lossy()
                .into_owned();
            Err(message.into())
        } else {
            Ok(())
        };
        ffi::duckdb_destroy_error_data(&mut error_data);
        result
    }
}

/// Exports a DuckDB data chunk through the Arrow C data interface and imports
/// it as rerun-arrow columns. DuckDB fills in the schema/array structs, arrow
/// takes ownership through their release callbacks.
unsafe fn chunk_to_arrow_columns(
    arrow_options: ffi::duckdb_arrow_options,
    input: ffi::duckdb_data_chunk,
) -> Result<Vec<ArrayRef>, BoxError> {
    unsafe {
        let column_count = ffi::duckdb_data_chunk_get_column_count(input);

        let mut types = (0..column_count)
            .map(|i| ffi::duckdb_vector_get_column_type(ffi::duckdb_data_chunk_get_vector(input, i)))
            .collect::<Vec<_>>();
        let names = (0..column_count)
            .map(|i| CString::new(format!("c{i}")).expect("no NUL"))
            .collect::<Vec<_>>();
        let mut name_ptrs = names.iter().map(|name| name.as_ptr()).collect::<Vec<_>>();

        let mut c_schema: ffi::ArrowSchema = std::mem::zeroed();
        let schema_result = check_error_data(ffi::duckdb_to_arrow_schema(
            arrow_options,
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            column_count,
            &mut c_schema,
        ));
        for logical_type in &mut types {
            ffi::duckdb_destroy_logical_type(logical_type);
        }
        schema_result?;
        // SAFETY: identical #[repr(C)] layout per the Arrow C data interface.
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = std::mem::transmute(c_schema);

        let mut c_array: ffi::ArrowArray = std::mem::zeroed();
        check_error_data(ffi::duckdb_data_chunk_to_arrow(
            arrow_options,
            input,
            &mut c_array,
        ))?;
        // SAFETY: see above.
        let ffi_array: arrow::ffi::FFI_ArrowArray = std::mem::transmute(c_array);

        let data = arrow::ffi::from_ffi(ffi_array, &ffi_schema)?;
        let root = arrow::array::make_array(data);
        let root = root
            .as_struct_opt()
            .ok_or("expected a struct array at the root of the exported chunk")?;
        Ok(root.columns().to_vec())
    }
}

unsafe fn copy_sink_inner(
    info: ffi::duckdb_copy_function_sink_info,
    input: ffi::duckdb_data_chunk,
) -> Result<(), BoxError> {
    let bind_data = unsafe {
        &*(ffi::duckdb_copy_function_sink_get_bind_data(info) as *const CopyBindData)
    };
    let global_state = unsafe {
        &*(ffi::duckdb_copy_function_sink_get_global_state(info) as *const CopyGlobalState)
    };
    let extra_info = unsafe {
        &*(ffi::duckdb_copy_function_sink_get_extra_info(info) as *const CopyExtraInfo)
    };

    if unsafe { ffi::duckdb_data_chunk_get_size(input) } == 0 {
        return Ok(());
    }

    let columns = unsafe { chunk_to_arrow_columns(extra_info.arrow_options, input)? };

    let mut timelines = IntMap::<TimelineName, TimeColumn>::default();
    if let (Some(time_column), Some(timeline_name)) =
        (bind_data.time_column, bind_data.timeline.as_deref())
    {
        let time_column = index_column_to_time_column(&columns[time_column], timeline_name)?;
        timelines.insert(*time_column.timeline().name(), time_column);
    }

    let mut components = ChunkComponents::default();
    for (idx, column) in columns.iter().enumerate() {
        if Some(idx) == bind_data.time_column {
            continue;
        }
        components.insert(SerializedComponentColumn {
            list_array: column_to_component_lists(column)?,
            descriptor: ComponentDescriptor::partial(bind_data.column_names[idx].as_str()),
        });
    }

    let chunk = Chunk::from_auto_row_ids(
        ChunkId::new(),
        EntityPath::parse_forgiving(&bind_data.entity),
        timelines,
        components,
    )?;

    let mut writer = global_state.inner.lock().map_err(|err| err.to_string())?;
    let store_id = writer.store_id.clone();
    writer
        .encoder
        .as_mut()
        .ok_or("writer already finalized")?
        .append(&LogMsg::ArrowMsg(store_id, chunk.to_arrow_msg()?))?;
    Ok(())
}

unsafe extern "C" fn copy_sink(
    info: ffi::duckdb_copy_function_sink_info,
    input: ffi::duckdb_data_chunk,
) {
    if let Err(err) = no_unwind(|| unsafe { copy_sink_inner(info, input) }) {
        set_error_c(&err.to_string(), |msg| unsafe {
            ffi::duckdb_copy_function_sink_set_error(info, msg);
        });
    }
}

// === finalize ===============================================================

unsafe fn copy_finalize_inner(
    info: ffi::duckdb_copy_function_finalize_info,
) -> Result<(), BoxError> {
    let global_state = unsafe {
        &*(ffi::duckdb_copy_function_finalize_get_global_state(info) as *const CopyGlobalState)
    };
    let mut writer = global_state.inner.lock().map_err(|err| err.to_string())?;
    let mut encoder = writer.encoder.take().ok_or("writer already finalized")?;
    encoder.finish()?;
    std::fs::write(&global_state.path, encoder.into_inner()?)?;
    Ok(())
}

unsafe extern "C" fn copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    if let Err(err) = no_unwind(|| unsafe { copy_finalize_inner(info) }) {
        set_error_c(&err.to_string(), |msg| unsafe {
            ffi::duckdb_copy_function_finalize_set_error(info, msg);
        });
    }
}
