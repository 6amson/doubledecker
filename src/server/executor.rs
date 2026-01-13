use crate::utils::error::DoubledeckerError;
use crate::utils::helpers::{build_aggregation_expr, build_filter_expr, parse_batch_to_json};
use crate::utils::statics::{Operations, QueryResponse, TransformOp};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, col};
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext, lit};

pub struct QueryExecutor {
    ctx: SessionContext,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub async fn load_csv(&self, path: &str, table_name: &str) -> Result<()> {
        let options = CsvReadOptions::new()
            .has_header(true)
            .file_extension("csv")
            .schema_infer_max_records(1000); // Infer schema from first 1000 rows for speed

        self.ctx.register_csv(table_name, path, options).await?;
        Ok(())
    }

    pub async fn execute_operations(
        &self,
        table_name: &str,
        operations: Vec<Operations>,
    ) -> Result<Vec<RecordBatch>> {
        let mut df = self.ctx.table(table_name).await?;

        for op in operations {
            df = self.apply_operation(df, op).await?;
        }
        // df.execute_stream().await?;
        // TODO: Implement streaming over GRPC maybe?? for now serve via http.

        df.collect().await
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
        let reponse = parse_batch_to_json(description_batch).await?;
        Ok(reponse)
    }

    pub async fn apply_operation(&self, df: DataFrame, op: Operations) -> Result<DataFrame> {
        match op {
            Operations::Select { columns } => {
                let cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
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
                let group_cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
                let agg_cols: Vec<Expr> = aggregations
                    .iter()
                    .map(|agg| build_aggregation_expr(agg))
                    .collect::<Result<Vec<Expr>>>()?;
                df.aggregate(group_cols, agg_cols)
            }
            Operations::Sort { column, ascending } => {
                let sort_expr = col(&column);
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
                let source_expr = col(&column);
                let value_lit = lit(value);

                let transform_expr = match operation {
                    TransformOp::Multiply => source_expr * value_lit,
                    TransformOp::Divide => source_expr / value_lit,
                    TransformOp::Add => source_expr + value_lit,
                    TransformOp::Subtract => source_expr - value_lit,
                };

                df.with_column(&alias, transform_expr)
            }
            Operations::Limit { count } => df.limit(0, Some(count)),
        }
    }
}
