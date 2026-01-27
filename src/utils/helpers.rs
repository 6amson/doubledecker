use crate::utils::error::DoubledeckerError;
use crate::utils::s3::S3Uploader;
use crate::utils::statics::{AggFunc, Aggregation, FilterOp, QueryResponse};
use arrow_json::writer::{JsonArray, WriterBuilder};
use axum::extract::Multipart;
use datafusion::arrow::array::*;
use datafusion::error::Result as DfResult;
use datafusion::functions_aggregate::expr_fn::*;
use datafusion::logical_expr::{Expr, col, lit};

pub fn col_escaped(name: &str) -> Expr {
    if name.contains('.') || name.contains(' ') || name.contains('-') {
        col(format!("\"{}\"", name))
    } else {
        col(name)
    }
}

pub fn build_filter_expr(column: &str, operator: FilterOp, value: &str) -> DfResult<Expr> {
    let col_expr = col_escaped(column);

    let lit_value = if let Ok(num) = value.parse::<f64>() {
        lit(num)
    } else {
        lit(value)
    };

    Ok(match operator {
        FilterOp::Eq => col_expr.eq(lit_value),
        FilterOp::Ne => col_expr.not_eq(lit_value),
        FilterOp::Gt => col_expr.gt(lit_value),
        FilterOp::Ge => col_expr.gt_eq(lit_value),
        FilterOp::Lt => col_expr.lt(lit_value),
        FilterOp::Le => col_expr.lt_eq(lit_value),
        FilterOp::Contains => col_expr.like(lit(format!("%{}%", value))),
    })
}

pub fn build_aggregation_expr(agg: &Aggregation) -> DfResult<Expr> {
    let col_expr = col_escaped(&agg.column);

    let expr = match agg.function {
        AggFunc::Sum => sum(col_expr),
        AggFunc::Avg => avg(col_expr),
        AggFunc::Max => max(col_expr),
        AggFunc::Min => min(col_expr),
        AggFunc::Count => count(col_expr),
    };

    Ok(if let Some(alias) = &agg.alias {
        expr.alias(alias)
    } else {
        expr
    })
}

pub async fn handle_file_upload(
    mut multipart: Multipart,
) -> Result<(String, String, usize), DoubledeckerError> {
    eprintln!("Starting handle_file_upload");
    while let Some(field) = multipart.next_field().await? {
        eprintln!("Processing field: {:?}", field.name());
        if field.name() == Some("file") {
            let file_name = field
                .file_name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            // Collect all chunks into a buffer
            let mut stream = field;
            let mut buffer = Vec::new();

            while let Some(chunk) = stream.chunk().await? {
                buffer.extend_from_slice(&chunk);
            }

            // Parse CSV and normalize headers to lowercase
            let csv_str = String::from_utf8(buffer)
                .map_err(|e| DoubledeckerError::FileUpload(format!("Invalid UTF-8: {}", e)))?;
            eprintln!("Read file size: {} bytes", csv_str.len());

            let lines: Vec<&str> = csv_str.lines().collect();
            if lines.is_empty() {
                return Err(DoubledeckerError::FileUpload("Empty CSV file".to_string()));
            }

            // Normalize the header row to lowercase and reconstruct CSV
            let lowercase_header = lines[0].to_lowercase();
            let mut normalized_lines = vec![lowercase_header];
            normalized_lines.extend(lines[1..].iter().map(|s| s.to_string()));

            let normalized_csv = normalized_lines.join("\n");
            let normalized_bytes = normalized_csv.as_bytes().to_vec();
            let file_size = normalized_bytes.len();

            let s3_key = S3Uploader::new()
                .await
                .upload_csv(normalized_bytes)
                .await
                .map_err(|e| DoubledeckerError::FileUpload(format!("S3 upload failed: {}", e)))?;

            eprintln!("S3 upload successful, key: {}", s3_key);

            return Ok((format!("s3://{}", s3_key), file_name, file_size));
        }
    }
    Err(DoubledeckerError::FileUpload(
        "No file field found in multipart data".to_string(),
    ))
}

pub async fn parse_batch_to_json(
    batches: Vec<RecordBatch>,
) -> Result<QueryResponse, DoubledeckerError> {
    if batches.is_empty() {
        return Ok(QueryResponse {
            columns: vec![],
            rows: vec![],
        });
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Use Arrow's optimized JSON writer with explicit nulls
    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true) // Ensure null values are written as {"key": null}
        .build::<_, JsonArray>(&mut buf);

    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
    writer
        .write_batches(&batch_refs)
        .map_err(|e| DoubledeckerError::DataFusionError(format!("JSON conversion error: {}", e)))?;
    writer.finish().map_err(|e| {
        DoubledeckerError::DataFusionError(format!("JSON finalization error: {}", e))
    })?;

    // Parse the JSON objects
    let json_str = String::from_utf8(buf).map_err(|e| {
        DoubledeckerError::DataFusionError(format!("UTF-8 conversion error: {}", e))
    })?;

    // cos json string returned by arrow_json crate is an object, we then map and convert to array
    let json_objects: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(&json_str)
            .map_err(|e| DoubledeckerError::DataFusionError(format!("JSON parse error: {}", e)))?;

    // Convert objects to arrays using column order
    let json_rows: Vec<serde_json::Value> = json_objects
        .into_iter()
        .map(|obj| {
            let row_array: Vec<serde_json::Value> = columns
                .iter()
                .map(|col_name| {
                    obj.get(col_name)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();
            serde_json::Value::Array(row_array)
        })
        .collect();

    Ok(QueryResponse {
        columns,
        rows: json_rows,
    })
}

// Convert QueryResponse to CSV format
pub fn query_response_to_csv(response: &QueryResponse) -> String {
    let mut csv = String::new();

    csv.push_str(&response.columns.join(","));
    csv.push('\n');

    for row in &response.rows {
        if let serde_json::Value::Array(values) = row {
            let row_str: Vec<String> = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => {
                        // Escape quotes and wrap in quotes if contains comma or quote
                        if s.contains(',') || s.contains('"') || s.contains('\n') {
                            format!("\"{}\"", s.replace('"', "\"\""))
                        } else {
                            s.clone()
                        }
                    }
                    serde_json::Value::Null => String::new(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => v.to_string(),
                })
                .collect();
            csv.push_str(&row_str.join(","));
            csv.push('\n');
        }
    }

    csv
}
