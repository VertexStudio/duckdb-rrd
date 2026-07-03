//! Generates the `.rrd` fixtures used by the sqllogictests.
//!
//! Usage: `cargo run --bin rrd_testgen -- <output-dir>`
//!
//! Produces:
//! - `simple.rrd`: one recording, scalars on `/metrics/loss`, 3D-ish points on
//!   `/world/points`, one static label on `/world`.
//! - `truncated.rrd`: the same stream without a footer and with its tail cut
//!   mid-chunk, emulating a file that is still being written to.

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Float64Array, StringArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use re_chunk::{Chunk, RowId, TimePoint, Timeline};
use re_log_encoding::Encoder;
use re_log_types::{LogMsg, SetStoreInfo, StoreId, StoreInfo, StoreKind, StoreSource};
use re_types_core::{ComponentDescriptor, SerializedComponentBatch};

fn main() -> anyhow::Result<()> {
    let out_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "test/data".to_owned());
    std::fs::create_dir_all(&out_dir)?;

    let store_id = StoreId::new(
        StoreKind::Recording,
        "duckdb_rrd_test",
        "test_recording_001",
    );

    let mut messages = vec![LogMsg::SetStoreInfo(SetStoreInfo {
        row_id: *RowId::new(),
        info: StoreInfo {
            store_id: store_id.clone(),
            cloned_from: None,
            store_source: StoreSource::Other("duckdb-rrd testgen".to_owned()),
            store_version: None,
        },
    })];

    let frame = Timeline::new_sequence("frame");

    // Scalars: /metrics/loss, one f64 per frame, frames 0..100.
    let mut scalars = Chunk::builder("/metrics/loss");
    for i in 0..100i64 {
        let value = (i as f64 / 10.0).sin();
        scalars = scalars.with_serialized_batch(
            RowId::new(),
            TimePoint::default().with(frame, i),
            SerializedComponentBatch {
                descriptor: ComponentDescriptor::partial("value"),
                array: Arc::new(Float64Array::from(vec![value])),
            },
        );
    }
    messages.push(LogMsg::ArrowMsg(
        store_id.clone(),
        scalars.build()?.to_arrow_msg()?,
    ));

    // Points: /world/points, two xyz triplets per frame, frames 0..10.
    let mut points = Chunk::builder("/world/points");
    for i in 0..10i64 {
        let coords = Float32Array::from(vec![
            i as f32,
            0.0,
            0.0,
            0.0,
            i as f32,
            1.0,
        ]);
        let positions = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            3,
            Arc::new(coords),
            None::<NullBuffer>,
        );
        points = points.with_serialized_batch(
            RowId::new(),
            TimePoint::default().with(frame, i),
            SerializedComponentBatch {
                descriptor: ComponentDescriptor::partial("positions"),
                array: Arc::new(positions),
            },
        );
    }
    messages.push(LogMsg::ArrowMsg(
        store_id.clone(),
        points.build()?.to_arrow_msg()?,
    ));

    // Big point cloud: one row holding 3000 points. Regression coverage for
    // list children larger than DuckDB's standard vector size (2048), which
    // requires explicit child reservation and size publication when writing
    // list vectors.
    let coords = Float32Array::from((0..9000).map(|i| i as f32).collect::<Vec<_>>());
    let positions = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        3,
        Arc::new(coords),
        None::<NullBuffer>,
    );
    let cloud = Chunk::builder("/world/cloud").with_serialized_batch(
        RowId::new(),
        TimePoint::default().with(frame, 0i64),
        SerializedComponentBatch {
            descriptor: ComponentDescriptor::partial("positions"),
            array: Arc::new(positions),
        },
    );
    messages.push(LogMsg::ArrowMsg(
        store_id.clone(),
        cloud.build()?.to_arrow_msg()?,
    ));

    // Static label on /world (empty timepoint = static data).
    let label = Chunk::builder("/world").with_serialized_batch(
        RowId::new(),
        TimePoint::default(),
        SerializedComponentBatch {
            descriptor: ComponentDescriptor::partial("label"),
            array: Arc::new(StringArray::from(vec!["hello rrd"])),
        },
    );
    messages.push(LogMsg::ArrowMsg(
        store_id.clone(),
        label.build()?.to_arrow_msg()?,
    ));

    // simple.rrd: complete stream, footer included.
    let mut encoder = Encoder::local()?;
    for message in &messages {
        encoder.append(message)?;
    }
    encoder.finish()?;
    let complete = encoder.into_inner()?;
    std::fs::write(format!("{out_dir}/simple.rrd"), &complete)?;

    // truncated.rrd: no footer, tail cut mid-message, as if a writer were
    // still appending. Readers must see all fully-written chunks.
    let mut encoder = Encoder::local()?;
    encoder.do_not_emit_footer();
    for message in &messages {
        encoder.append(message)?;
    }
    encoder.finish()?;
    let unfooted = encoder.into_inner()?;
    let cut = unfooted.len() - 17;
    std::fs::write(format!("{out_dir}/truncated.rrd"), &unfooted[..cut])?;

    eprintln!("wrote {out_dir}/simple.rrd ({} bytes)", complete.len());
    eprintln!("wrote {out_dir}/truncated.rrd ({cut} bytes)");
    Ok(())
}
