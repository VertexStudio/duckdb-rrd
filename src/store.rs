//! Opening `.rrd` files and selecting a recording out of them.

use std::error::Error;

use re_chunk_store::ChunkStoreConfig;
use re_dataframe::{QueryEngine, StorageEngine, StoreKind};
use re_log_types::StoreId;

type BoxError = Box<dyn Error>;

/// Opens all recording stores contained in an `.rrd` file.
///
/// Blueprint stores are skipped: they describe viewer layout, not logged data.
pub fn open_recordings(path: &str) -> Result<Vec<(StoreId, QueryEngine<StorageEngine>)>, BoxError> {
    let engines = QueryEngine::from_rrd_filepath(&ChunkStoreConfig::DEFAULT, path)
        .map_err(|err| format!("failed to read rrd file {path:?}: {err}"))?;

    Ok(engines
        .into_iter()
        .filter(|(store_id, _)| store_id.kind() == StoreKind::Recording)
        .collect())
}

/// Opens `path` and picks a single recording.
///
/// `recording`: optional recording id to disambiguate files holding multiple
/// recordings.
pub fn open_recording(
    path: &str,
    recording: Option<&str>,
) -> Result<(StoreId, QueryEngine<StorageEngine>), BoxError> {
    let mut recordings = open_recordings(path)?;

    if let Some(wanted) = recording {
        let found = recordings
            .iter()
            .position(|(store_id, _)| store_id.recording_id().as_str() == wanted);
        return match found {
            Some(idx) => Ok(recordings.swap_remove(idx)),
            None => Err(format!(
                "no recording {wanted:?} in {path:?}; available: {}",
                recording_ids(&recordings)
            )
            .into()),
        };
    }

    match recordings.len() {
        0 => Err(format!("no recordings found in {path:?}").into()),
        1 => Ok(recordings.pop().expect("len checked")),
        _ => Err(format!(
            "{path:?} holds multiple recordings, disambiguate with recording => '...'; available: {}",
            recording_ids(&recordings)
        )
        .into()),
    }
}

fn recording_ids(recordings: &[(StoreId, QueryEngine<StorageEngine>)]) -> String {
    recordings
        .iter()
        .map(|(store_id, _)| format!("{:?}", store_id.recording_id().as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}
