//! `read_rrd` and friends: table functions for querying `.rrd` files.

use std::error::Error;
use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};

use duckdb::arrow::datatypes::{Schema as DbSchema, SchemaRef as DbSchemaRef};
use duckdb::arrow::record_batch::RecordBatch as DbRecordBatch;

use re_dataframe::{
    EntityPathFilter, QueryEngine, QueryExpression, QueryHandle, SparseFillStrategy,
    StorageEngine, TimelineName, ViewContentsSelector,
};

use crate::arrow_bridge;
use crate::store::{open_recording, open_recordings};

type BoxError = Box<dyn Error>;

/// Rows DuckDB vectors can hold per output chunk (STANDARD_VECTOR_SIZE).
const VECTOR_SIZE: usize = 2048;

fn varchar() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Varchar)
}

// === read_rrd ===============================================================

pub struct ReadRrdBind {
    engine: QueryEngine<StorageEngine>,
    query: QueryExpression,
    schema: DbSchemaRef,
}

pub struct ReadRrdInit {
    handle: Mutex<QueryHandle<StorageEngine>>,
}

pub struct ReadRrd;

impl VTab for ReadRrd {
    type BindData = ReadRrdBind;
    type InitData = ReadRrdInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, BoxError> {
        let path = bind.get_parameter(0).to_string();
        let entity = bind.get_named_parameter("entity").map(|v| v.to_string());
        let timeline = bind.get_named_parameter("timeline").map(|v| v.to_string());
        let recording = bind.get_named_parameter("recording").map(|v| v.to_string());
        let fill_latest = bind
            .get_named_parameter("fill_latest")
            .map(|v| v.to_string() == "true")
            .unwrap_or(false);
        let static_only = bind
            .get_named_parameter("static_only")
            .map(|v| v.to_string() == "true")
            .unwrap_or(false);

        let (_store_id, engine) = open_recording(&path, recording.as_deref())?;

        let store_schema = engine.schema();

        // Resolve the index timeline: explicit param, else `log_time`, else the
        // first timeline in the store.
        let filtered_index = if static_only {
            None
        } else if let Some(wanted) = &timeline {
            let wanted = TimelineName::from(wanted.as_str());
            if !store_schema
                .indices
                .iter()
                .any(|index| index.timeline_name() == wanted)
            {
                return Err(format!(
                    "timeline {wanted:?} not found in {path:?}; available: {}",
                    store_schema
                        .indices
                        .iter()
                        .map(|index| format!("{:?}", index.column_name()))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .into());
            }
            Some(wanted)
        } else {
            let log_time = TimelineName::log_time();
            store_schema
                .indices
                .iter()
                .find(|index| index.timeline_name() == log_time)
                .or_else(|| store_schema.indices.first())
                .map(|index| index.timeline_name())
        };

        // Resolve the entity filter against the entities present in the store.
        let view_contents: Option<ViewContentsSelector> = match &entity {
            Some(expr) => {
                let filter = EntityPathFilter::parse_forgiving(expr);
                let selector: ViewContentsSelector = engine
                    .iter_entity_paths_sorted(&filter)
                    .map(|entity_path| (entity_path, None))
                    .collect();
                if selector.is_empty() {
                    return Err(format!(
                        "entity filter {expr:?} matches no entity in {path:?}; try rrd_entities({path:?})"
                    )
                    .into());
                }
                Some(selector)
            }
            None => None,
        };

        let query = QueryExpression {
            view_contents,
            filtered_index,
            sparse_fill_strategy: if fill_latest {
                SparseFillStrategy::LatestAtGlobal
            } else {
                SparseFillStrategy::None
            },
            ..Default::default()
        };

        let handle = engine.query(query.clone());

        let mut db_fields = Vec::with_capacity(handle.schema().fields().len());
        for field in handle.schema().fields() {
            let db_field = arrow_bridge::field_to_db(field)?;
            let logical_type = to_duckdb_logical_type(db_field.data_type()).map_err(|err| {
                format!(
                    "column {:?} has unsupported datatype {}: {err}",
                    field.name(),
                    field.data_type()
                )
            })?;
            bind.add_result_column(field.name(), logical_type);
            db_fields.push(db_field);
        }

        if db_fields.is_empty() {
            return Err(format!("query over {path:?} yields no columns").into());
        }

        Ok(ReadRrdBind {
            engine,
            query,
            schema: DbSchemaRef::new(DbSchema::new(db_fields)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, BoxError> {
        let bind = unsafe { &*init.get_bind_data::<ReadRrdBind>() };
        Ok(ReadRrdInit {
            handle: Mutex::new(bind.engine.query(bind.query.clone())),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), BoxError> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();

        let mut handle = init.handle.lock().map_err(|err| err.to_string())?;
        let rows = handle.next_n_rows(VECTOR_SIZE, usize::MAX);
        if rows.num_rows == 0 {
            output.set_len(0);
            return Ok(());
        }

        let columns = rows
            .columns
            .iter()
            .map(|column| arrow_bridge::array_to_db(column.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = DbRecordBatch::try_new(bind.schema.clone(), columns)?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![varchar()])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("entity".to_string(), varchar()),
            ("timeline".to_string(), varchar()),
            ("recording".to_string(), varchar()),
            (
                "fill_latest".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "static_only".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ])
    }
}

// === helpers behind rrd_entities / rrd_schema / rrd_recordings ==============

/// Shared plumbing for the introspection functions: rows are fully computed at
/// bind time as strings, then streamed out.
pub struct StringRowsBind {
    rows: Vec<Vec<Option<String>>>,
}

pub struct StringRowsInit {
    cursor: Mutex<usize>,
}

fn emit_string_rows(
    rows: &[Vec<Option<String>>],
    cursor: &Mutex<usize>,
    output: &mut DataChunkHandle,
) -> Result<(), BoxError> {
    let mut cursor = cursor.lock().map_err(|err| err.to_string())?;
    let remaining = rows.len().saturating_sub(*cursor);
    let count = remaining.min(VECTOR_SIZE);

    for (row_idx, row) in rows[*cursor..*cursor + count].iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            let mut vector = output.flat_vector(col_idx);
            match value {
                Some(value) => vector.insert(row_idx, value.as_str()),
                None => vector.set_null(row_idx),
            }
        }
    }

    *cursor += count;
    output.set_len(count);
    Ok(())
}

macro_rules! string_rows_vtab {
    ($name:ident, $columns:expr, $rows:expr) => {
        pub struct $name;

        impl VTab for $name {
            type BindData = StringRowsBind;
            type InitData = StringRowsInit;

            fn bind(bind: &BindInfo) -> Result<Self::BindData, BoxError> {
                let path = bind.get_parameter(0).to_string();
                for column in $columns {
                    bind.add_result_column(column, varchar());
                }
                #[allow(clippy::redundant_closure_call)]
                let rows = ($rows)(&path)?;
                Ok(StringRowsBind { rows })
            }

            fn init(_: &InitInfo) -> Result<Self::InitData, BoxError> {
                Ok(StringRowsInit {
                    cursor: Mutex::new(0),
                })
            }

            fn func(
                func: &TableFunctionInfo<Self>,
                output: &mut DataChunkHandle,
            ) -> Result<(), BoxError> {
                let bind = func.get_bind_data();
                let init = func.get_init_data();
                emit_string_rows(&bind.rows, &init.cursor, output)
            }

            fn parameters() -> Option<Vec<LogicalTypeHandle>> {
                Some(vec![varchar()])
            }
        }
    };
}

string_rows_vtab!(
    RrdEntities,
    ["entity_path"],
    |path: &str| -> Result<Vec<Vec<Option<String>>>, BoxError> {
        let (_store_id, engine) = open_recording(path, None)?;
        Ok(engine
            .iter_entity_paths_sorted(&EntityPathFilter::all())
            .map(|entity_path| vec![Some(entity_path.to_string())])
            .collect())
    }
);

string_rows_vtab!(
    RrdSchema,
    ["column_name", "kind", "entity_path", "component", "component_type", "datatype", "is_static"],
    |path: &str| -> Result<Vec<Vec<Option<String>>>, BoxError> {
        let (_store_id, engine) = open_recording(path, None)?;
        let schema = engine.schema();
        let mut rows = Vec::new();
        for index in &schema.indices {
            rows.push(vec![
                Some(index.column_name().to_string()),
                Some("index".to_string()),
                None,
                None,
                None,
                Some(index.datatype().to_string()),
                Some("false".to_string()),
            ]);
        }
        for component in &schema.components {
            rows.push(vec![
                Some(component.column_name(re_sorbet::BatchType::Dataframe)),
                Some("component".to_string()),
                Some(component.entity_path.to_string()),
                Some(component.component.to_string()),
                component.component_type.map(|ty| ty.to_string()),
                Some(component.store_datatype.to_string()),
                Some(component.is_static.to_string()),
            ]);
        }
        Ok(rows)
    }
);

string_rows_vtab!(
    RrdRecordings,
    ["application_id", "recording_id"],
    |path: &str| -> Result<Vec<Vec<Option<String>>>, BoxError> {
        let recordings = open_recordings(path)?;
        Ok(recordings
            .iter()
            .map(|(store_id, _)| {
                vec![
                    Some(store_id.application_id().to_string()),
                    Some(store_id.recording_id().to_string()),
                ]
            })
            .collect())
    }
);
