use crate::normalization::unified_royalty_schema;
use crate::utils::error::DoubledeckerError;
use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use object_store::aws::AmazonS3Builder;
use object_store::prefix::PrefixStore;
use sqlx::PgPool;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
pub struct EngineProvider {
    s3_bucket: String,
    db_pool: PgPool,
    rt_env: Arc<RuntimeEnv>,
}

impl EngineProvider {
    pub fn new(db_pool: PgPool) -> Self {
        let s3_bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "dd-query-csv-bucket".to_string());

        // 1. Configure a single, server-wide bounded memory pool (2GB RAM limit)
        // and let DataFusion automatically manage ephemeral OS temp directories for disk spilling.
        let rt_config = RuntimeConfig::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(2 * 1024 * 1024 * 1024)))
            .with_disk_manager(DiskManagerConfig::NewOs);

        let rt_env = Arc::new(
            RuntimeEnv::try_new(rt_config)
                .expect("Failed to initialize global DataFusion RuntimeEnv"),
        );

        Self {
            s3_bucket,
            db_pool,
            rt_env,
        }
    }

    /// Executes analytics queries in a strictly isolated, ephemeral session context.
    /// Eliminates race conditions and cross-tenant data leakage.
    pub async fn execute_royalty_analytics(
        &self,
        workspace_id: Uuid,
        query_sql: &str,
    ) -> Result<Vec<RecordBatch>, DoubledeckerError> {
        // 1. Create an ephemeral session context borrowing the shared global runtime environment
        let session_config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config_rt(session_config, self.rt_env.clone());

        // 2. Instantiate Tenant-Scoped Object Store rooted strictly at the workspace prefix
        let prefix = format!("workspaces/{}", workspace_id);
        if let Ok(s3_store) = AmazonS3Builder::from_env()
            .with_bucket_name(&self.s3_bucket)
            .build()
        {
            let prefix_store = PrefixStore::new(s3_store, prefix);
            if let Ok(url) = Url::parse("s3://tenant_data/") {
                ctx.runtime_env()
                    .register_object_store(&url, Arc::new(prefix_store));
            }
        }

        // 3. Register music UDFs
        crate::engine::udfs::register_music_udfs(&ctx);

        // 4. Register logical table `royalty_data`
        let parquet_url = "s3://tenant_data/processed/";
        let options = ParquetReadOptions::default();
        if let Err(e) = ctx.register_parquet("royalty_data", parquet_url, options).await {
            eprintln!(
                "Note: Could not register Parquet files for workspace {} (may be empty): {}. Registering empty memory table.",
                workspace_id, e
            );
            let empty_batch = RecordBatch::new_empty(unified_royalty_schema());
            if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(
                unified_royalty_schema(),
                vec![vec![empty_batch]],
            ) {
                let _ = ctx.register_table("royalty_data", Arc::new(mem_table));
            }
        }

        // 4b. Load and register cascading splits for relational join fan-out
        if let Ok(splits) = crate::db::queries::get_effective_splits(
            &self.db_pool,
            workspace_id,
            chrono::Utc::now().date_naive(),
        )
        .await
        {
            let num_splits = splits.len();
            let mut payee_names = Vec::with_capacity(num_splits);
            let mut percentages = Vec::with_capacity(num_splits);
            let mut track_ids = Vec::with_capacity(num_splits);
            let mut album_ids = Vec::with_capacity(num_splits);
            let mut artist_ids = Vec::with_capacity(num_splits);

            for s in &splits {
                payee_names.push(s.payee_name.clone());
                percentages.push(s.percentage.to_string().parse::<f64>().unwrap_or(0.0));
                track_ids.push(s.track_id.map(|u| u.to_string()));
                album_ids.push(s.album_id.map(|u| u.to_string()));
                artist_ids.push(s.artist_id.map(|u| u.to_string()));
            }

            let payee_arr = Arc::new(StringArray::from(payee_names)) as ArrayRef;
            let pct_arr = Arc::new(Float64Array::from(percentages)) as ArrayRef;
            let track_arr = Arc::new(StringArray::from(track_ids)) as ArrayRef;
            let album_arr = Arc::new(StringArray::from(album_ids)) as ArrayRef;
            let artist_arr = Arc::new(StringArray::from(artist_ids)) as ArrayRef;

            let splits_schema = Arc::new(Schema::new(vec![
                Field::new("payee_name", DataType::Utf8, false),
                Field::new("percentage", DataType::Float64, false),
                Field::new("track_id_str", DataType::Utf8, true),
                Field::new("album_id_str", DataType::Utf8, true),
                Field::new("artist_id_str", DataType::Utf8, true),
            ]));

            if let Ok(batch) = RecordBatch::try_new(
                splits_schema.clone(),
                vec![payee_arr, pct_arr, track_arr, album_arr, artist_arr],
            ) {
                if let Ok(mem_table) =
                    datafusion::datasource::MemTable::try_new(splits_schema, vec![vec![batch]])
                {
                    let _ = ctx.register_table("cascading_splits", Arc::new(mem_table));
                }
            }
        }

        // 5. Execute relational query plan
        let df = ctx
            .sql(query_sql)
            .await
            .map_err(|e| DoubledeckerError::Internal(format!("SQL query planning error: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| DoubledeckerError::Internal(format!("Query execution error: {}", e)))?;

        Ok(batches)
    }
}
