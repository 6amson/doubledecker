use crate::db::queries::{
    get_dataset_by_id, update_dataset_status,
};
use crate::normalization::{DistributorSource, RoyaltyAdapter, unified_royalty_schema};
use crate::utils::error::DoubledeckerError;
use crate::utils::s3::S3Uploader;
use arrow::array::Array;
use arrow_csv::reader::Format;
use inngest::{
    client::Inngest,
    function::{FunctionOpts, Input, ServableFn, Trigger},
    step_tool::Step as StepTool,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogItem {
    pub artist: String,
    pub album_title: Option<String>,
    pub upc: Option<String>,
    pub isrc: String,
    pub track_title: String,
}

pub fn process_csv_and_extract_catalog(
    csv_bytes: &[u8],
    adapter: &dyn RoyaltyAdapter,
) -> Result<(Vec<u8>, i64, Vec<CatalogItem>), DoubledeckerError> {
    let mut cursor = std::io::Cursor::new(csv_bytes);
    let format = Format::default().with_header(true);
    let (inferred_schema, _) = format.infer_schema(&mut cursor, Some(100))
        .map_err(|e| DoubledeckerError::Internal(format!("CSV schema infer error: {}", e)))?;
    cursor.set_position(0);
    let reader = arrow_csv::ReaderBuilder::new(Arc::new(inferred_schema))
        .with_header(true)
        .build(cursor)
        .map_err(|e| DoubledeckerError::Internal(format!("CSV reader error: {}", e)))?;

    let schema = unified_royalty_schema();
    let mut buffer = Vec::new();
    let props = parquet::file::properties::WriterProperties::builder().build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))
        .map_err(|e| DoubledeckerError::Internal(format!("Parquet writer init error: {}", e)))?;

    let mut total_rows = 0i64;
    let mut discovered_items = std::collections::HashSet::new();

    for maybe_batch in reader {
        let batch: arrow::array::RecordBatch = maybe_batch
            .map_err(|e| DoubledeckerError::Internal(format!("CSV batch read error: {}", e)))?;
        
        let mut artist_vec = vec!["Unknown Artist".to_string(); batch.num_rows()];
        let mut album_vec: Vec<Option<String>> = vec![None; batch.num_rows()];
        let mut upc_vec: Vec<Option<String>> = vec![None; batch.num_rows()];

        for (idx, field) in batch.schema().fields().iter().enumerate() {
            let name_lower = field.name().to_lowercase();
            let name_trimmed = name_lower.trim();
            if name_trimmed == "artist" || name_trimmed == "artist name" || name_trimmed == "band" || name_trimmed == "performer" {
                if let Some(str_arr) = batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>() {
                    for i in 0..batch.num_rows() {
                        if !str_arr.is_null(i) && !str_arr.value(i).trim().is_empty() {
                            artist_vec[i] = str_arr.value(i).trim().to_string();
                        }
                    }
                }
            } else if ["album", "album title", "release", "release title", "project", "project title", "album name", "release name"].contains(&name_trimmed) {
                if let Some(str_arr) = batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>() {
                    for i in 0..batch.num_rows() {
                        if !str_arr.is_null(i) && !str_arr.value(i).trim().is_empty() {
                            album_vec[i] = Some(str_arr.value(i).trim().to_string());
                        }
                    }
                }
            } else if ["upc", "upc code", "barcode", "gtin", "ean", "album upc", "release upc", "ean/upc", "upc/ean", "bar code", "upc/barcode"].contains(&name_trimmed) {
                if let Some(str_arr) = batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>() {
                    for i in 0..batch.num_rows() {
                        if !str_arr.is_null(i) && !str_arr.value(i).trim().is_empty() {
                            upc_vec[i] = Some(str_arr.value(i).trim().to_string());
                        }
                    }
                }
            }
        }

        let norm_batch = adapter.normalize_batch(batch)?;
        total_rows += norm_batch.num_rows() as i64;

        // Extract Catalog metadata from 12-column normalized batch
        if let (Some(isrc_arr), Some(upc_arr), Some(title_arr), Some(artist_arr), Some(album_arr)) = (
            norm_batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>(),
            norm_batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>(),
            norm_batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>(),
            norm_batch.column(3).as_any().downcast_ref::<arrow::array::StringArray>(),
            norm_batch.column(4).as_any().downcast_ref::<arrow::array::StringArray>(),
        ) {
            for i in 0..norm_batch.num_rows() {
                let isrc_str = isrc_arr.value(i).trim();
                let title_str = title_arr.value(i).trim();
                if !isrc_str.is_empty() && !title_str.is_empty() {
                    discovered_items.insert(CatalogItem {
                        artist: if artist_arr.value(i).trim().is_empty() {
                            "Unknown Artist".to_string()
                        } else {
                            artist_arr.value(i).trim().to_string()
                        },
                        album_title: if album_arr.value(i).trim().is_empty() {
                            None
                        } else {
                            Some(album_arr.value(i).trim().to_string())
                        },
                        upc: if upc_arr.value(i).trim().is_empty() {
                            None
                        } else {
                            Some(upc_arr.value(i).trim().to_string())
                        },
                        isrc: isrc_str.to_string(),
                        track_title: title_str.to_string(),
                    });
                }
            }
        }

        writer
            .write(&norm_batch)
            .map_err(|e| DoubledeckerError::Internal(format!("Parquet batch write error: {}", e)))?;
    }
    writer
        .close()
        .map_err(|e| DoubledeckerError::Internal(format!("Parquet writer close error: {}", e)))?;

    Ok((buffer, total_rows, discovered_items.into_iter().collect()))
}

pub fn register_ingestion_workflow(
    client: &Inngest,
    db_pool: PgPool,
    uploader: Arc<S3Uploader>,
) -> ServableFn<Value, DoubledeckerError> {
    client.create_function(
        FunctionOpts::new("process-dataset").name("Process Royalty Dataset"),
        Trigger::event("dataset/uploaded"),
        move |input: Input<Value>, step: StepTool| {
            let db_pool = db_pool.clone();
            let uploader = uploader.clone();
            async move {
                let data = &input.event.data;
                let dataset_id_str = data.get("dataset_id").and_then(|v| v.as_str()).unwrap_or_default();
                let workspace_id_str = data.get("workspace_id").and_then(|v| v.as_str()).unwrap_or_default();
                let staging_key = data.get("staging_key").and_then(|v| v.as_str()).unwrap_or_default().to_string();

                let dataset_id = uuid::Uuid::parse_str(dataset_id_str).map_err(|e| {
                    inngest::result::Error::Dev(inngest::result::DevError::Basic(format!("Invalid dataset_id: {}", e)))
                })?;
                let workspace_id = uuid::Uuid::parse_str(workspace_id_str).map_err(|e| {
                    inngest::result::Error::Dev(inngest::result::DevError::Basic(format!("Invalid workspace_id: {}", e)))
                })?;

                let step_prefix = format!("{}-{}", workspace_id_str, dataset_id_str);

                // Step 1: Update status to PROCESSING
                let _ = step.run(&format!("set-status-processing-{}", step_prefix), || {
                    let db_pool = db_pool.clone();
                    async move {
                        tokio::spawn(async move {
                            let _ = update_dataset_status(&db_pool, dataset_id, "PROCESSING", 0, None).await;
                            Ok::<_, DoubledeckerError>(json!({ "status": "PROCESSING" }))
                        })
                        .await
                        .map_err(|e| DoubledeckerError::Internal(e.to_string()))?
                    }
                }).await?;

                // Step 2: Download CSV, normalize to Parquet, upload Parquet, delete staging
                let (total_rows, discovered_items) = step.run(&format!("convert-csv-to-parquet-{}", step_prefix), || {
                    let db_pool = db_pool.clone();
                    let uploader = uploader.clone();
                    let staging_key = staging_key.clone();
                    async move {
                        // tokio::spawn is required here: aws-smithy-runtime futures (IdentityFuture,
                        // MaybeTimeoutFuture, etc.) are Send but NOT Sync. The inngest step closure
                        // must return a Sync future. Spawning isolates the non-Sync S3 futures in a
                        // separate task; JoinHandle<T> is Send + Sync regardless of T.
                        tokio::spawn(async move {
                            let dataset = get_dataset_by_id(&db_pool, workspace_id, dataset_id).await?;
                            let csv_bytes = uploader.download_csv(&staging_key).await?;

                            let source = DistributorSource::from_str_lenient(&dataset.distributor_source)
                                .unwrap_or_else(|| DistributorSource::detect_from_csv_bytes(&csv_bytes));
                            let adapter: Box<dyn RoyaltyAdapter> = source.to_adapter();

                            let (parquet_bytes, total_rows, discovered_items) = process_csv_and_extract_catalog(&csv_bytes, &*adapter)?;

                            let s3_parquet_key = dataset.s3_parquet_key.clone();
                            uploader.upload_parquet(&s3_parquet_key, parquet_bytes).await?;
                            let _ = uploader.delete_file(&staging_key).await;

                            Ok::<_, DoubledeckerError>((total_rows, discovered_items))
                        })
                        .await
                        .map_err(|e| DoubledeckerError::Internal(e.to_string()))?
                    }
                }).await?;

                // Step 3: Auto-Catalog Discovery
                let _ = step.run(&format!("auto-catalog-discovery-{}", step_prefix), || {
                    let db_pool = db_pool.clone();
                    let discovered_items = discovered_items.clone();
                    async move {
                        tokio::spawn(async move {
                            if !discovered_items.is_empty() {
                                if let Ok(workspace) = sqlx::query_as::<_, crate::db::models::Workspace>(
                                    "SELECT id, owner_user_id, name, storage_used_bytes, created_at, updated_at FROM workspaces WHERE id = $1"
                                )
                                .bind(workspace_id)
                                .fetch_one(&db_pool)
                                .await {
                                    let owner_user_id = workspace.owner_user_id;
                                    for item in discovered_items {
                                        if let Ok(artist) = crate::db::queries::upsert_artist(&db_pool, owner_user_id, item.artist).await {
                                            let album_id = if let Some(album_title) = item.album_title {
                                                crate::db::queries::upsert_album(
                                                    &db_pool,
                                                    owner_user_id,
                                                    artist.id,
                                                    album_title,
                                                    item.upc.clone(),
                                                    None,
                                                )
                                                .await
                                                .map(|a| a.id)
                                                .ok()
                                            } else {
                                                None
                                            };
                                            let _ = crate::db::queries::upsert_track(&db_pool, owner_user_id, artist.id, album_id, item.isrc, item.track_title).await;
                                        }
                                    }
                                }
                            }
                            Ok::<_, DoubledeckerError>(json!({ "discovered": true }))
                        })
                        .await
                        .map_err(|e| DoubledeckerError::Internal(e.to_string()))?
                    }
                }).await?;

                // Step 4: Update status to READY
                let _ = step.run(&format!("set-status-ready-{}", step_prefix), || {
                    let db_pool = db_pool.clone();
                    async move {
                        tokio::spawn(async move {
                            let _ = update_dataset_status(&db_pool, dataset_id, "READY", total_rows, None).await;
                            Ok::<_, DoubledeckerError>(json!({ "status": "READY" }))
                        })
                        .await
                        .map_err(|e| DoubledeckerError::Internal(e.to_string()))?
                    }
                }).await?;

                Ok(json!({ "success": true, "rows": total_rows }))
            }
        },
    )
}