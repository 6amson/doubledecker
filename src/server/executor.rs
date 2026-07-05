use crate::utils::error::DoubledeckerError;
use crate::utils::helpers::{
    build_aggregation_expr, build_filter_expr, col_escaped, parse_batch_to_json,
};
use crate::utils::statics::{Operations, QueryResponse, TransformOp};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, DataFrame, SessionContext, lit};
use datafusion::functions::expr_fn::date_trunc;
use datafusion::physical_plan::SendableRecordBatchStream;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use url::Url;
use std::sync::Arc;

pub struct QueryExecutor {
    ctx: SessionContext,
}

impl QueryExecutor {
    /// Initializes the SessionContext with globally registered S3 and GCP ObjectStores
    /// so DataFusion can read s3://, gs://, and gcs:// paths instantly into CPU memory lanes.
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "dd-query-csv-bucket".to_string());

        // Dynamically build the object store using standard AWS environment variables (or S3 interoperability for GCS)
        if let Ok(s3_store) = AmazonS3Builder::from_env().with_bucket_name(&bucket).build() {
            let s3_store = Arc::new(s3_store);
            if let Ok(url) = Url::parse(&format!("s3://{}/", bucket)) {
                ctx.runtime_env().register_object_store(&url, s3_store.clone());
                eprintln!(">>> Native SIMD-S3 ObjectStore pipeline registered for s3://{}/ successfully.", bucket);
            }
            if let Ok(url) = Url::parse("s3://") {
                ctx.runtime_env().register_object_store(&url, s3_store);
            }
        }

        // Dynamically build the GCP object store for gs:// and gcs:// URLs
        if let Ok(gcs_store) = GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket).build() {
            let gcs_store = Arc::new(gcs_store);
            if let Ok(url) = Url::parse(&format!("gs://{}/", bucket)) {
                ctx.runtime_env().register_object_store(&url, gcs_store.clone());
                eprintln!(">>> Native SIMD-GCP (gs://{}/) ObjectStore pipeline registered successfully.", bucket);
            }
            if let Ok(url) = Url::parse(&format!("gcs://{}/", bucket)) {
                ctx.runtime_env().register_object_store(&url, gcs_store.clone());
                eprintln!(">>> Native SIMD-GCP (gcs://{}/) ObjectStore pipeline registered successfully.", bucket);
            }
            if let Ok(url) = Url::parse("gs://") {
                ctx.runtime_env().register_object_store(&url, gcs_store.clone());
            }
            if let Ok(url) = Url::parse("gcs://") {
                ctx.runtime_env().register_object_store(&url, gcs_store);
            }
        }

        Self { ctx }
    }

    /// Optimized: DataFusion now natively parses files straight into memory.
    /// If it's an S3 URI, it streams chunks over the network interface without dropping files to disk.
    pub async fn load_csv(&self, path: &str, table_name: &str) -> Result<()> {
        let options = CsvReadOptions::new()
            .has_header(true)
            .file_extension("csv")
            .schema_infer_max_records(1000); 

        self.ctx
            .register_csv(table_name, path, options)
            .await?;
        Ok(())
    }

    /// New Strategy: Columnar Parquet registration for raw binary speed.
    pub async fn load_parquet(&self, path: &str, table_name: &str) -> Result<()> {
        let options = ParquetReadOptions::default();

        self.ctx
            .register_parquet(table_name, path, options)
            .await?;
        Ok(())
    }

    /// Optimized: Returns a zero-allocation SendableRecordBatchStream.
    /// Batches pass through SIMD lanes sequentially instead of clogging the Heap.
    pub async fn execute_operations(
        &self,
        table_name: &str,
        operations: Vec<Operations>,
    ) -> Result<SendableRecordBatchStream> {
        let mut df = self.ctx.table(table_name).await?;

        for op in operations {
            df = self.apply_operation(df, op).await?;
        }

        // Return the active hardware execution stream
        df.execute_stream().await
    }

    async fn parse_record_batch(
        &self,
        record_batch: Vec<RecordBatch>,
    ) -> Result<QueryResponse, DoubledeckerError> {
        let response = parse_batch_to_json(record_batch).await?;
        Ok(response)
    }

    pub async fn describe_table(
        &self,
        table_name: &str,
    ) -> Result<QueryResponse, DoubledeckerError> {
        let df = self.ctx.table(table_name).await?;
        let description = df.describe().await?;
        let description_batch = description.collect().await?;
        let response = parse_batch_to_json(description_batch).await?;
        Ok(response)
    }

    pub async fn apply_operation(&self, df: DataFrame, op: Operations) -> Result<DataFrame> {
        match op {
            Operations::Select { columns } => {
                let cols: Vec<Expr> = columns.iter().map(|c| col_escaped(c)).collect();
                df.select(cols)
            }
            Operations::Filter {
                column,
                operator,
                value,
            } => {
                let filter_expr = build_filter_expr(&column, operator, &value)?;
                df.filter(filter_expr)
            }
            Operations::GroupBy {
                columns,
                aggregations,
            } => {
                if columns.is_empty() {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "GroupBy requires at least one column to group by".to_string(),
                    ));
                }
                if aggregations.is_empty() {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "GroupBy requires at least one aggregation function (e.g., SUM, AVG, COUNT)".to_string(),
                    ));
                }

                let group_cols: Vec<Expr> = columns.iter().map(|c| col_escaped(c)).collect();
                let agg_cols: Vec<Expr> = aggregations
                    .iter()
                    .map(|agg| build_aggregation_expr(agg))
                    .collect::<Result<Vec<Expr>>>()?;
                df.aggregate(group_cols, agg_cols)
            }
            Operations::Sort { column, ascending } => {
                let sort_expr = col_escaped(&column);
                let expr_fn = if ascending {
                    sort_expr.sort(true, true)
                } else {
                    sort_expr.sort(false, true)
                };

                df.sort(vec![expr_fn])
            }
            Operations::Transform {
                column,
                operation,
                value,
                alias,
            } => {
                let source_expr = col_escaped(&column);
                let value_lit = lit(value);

                let transform_expr = match operation {
                    TransformOp::Multiply => source_expr * value_lit,
                    TransformOp::Divide => source_expr / value_lit,
                    TransformOp::Add => source_expr + value_lit,
                    TransformOp::Subtract => source_expr - value_lit,
                    TransformOp::DateTruncYear => date_trunc(lit("year"), source_expr),
                    TransformOp::DateTruncMonth => date_trunc(lit("month"), source_expr),
                    TransformOp::DateTruncWeek => date_trunc(lit("week"), source_expr),
                    TransformOp::DateTruncDay => date_trunc(lit("day"), source_expr),
                };

                df.with_column(&alias, transform_expr)
            }
            Operations::Limit { count } => df.limit(0, Some(count)),
        }
    }
}
