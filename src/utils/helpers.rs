use crate::utils::error::DoubledeckerError;
use crate::utils::statics::{AggFunc, Aggregation, FilterOp, QueryResponse};
use axum::extract::Multipart;
use datafusion::arrow::array::*;
use datafusion::error::Result as DfResult;
use datafusion::functions_aggregate::expr_fn::*;
use datafusion::logical_expr::{Expr, col, lit};
use uuid::Uuid;

pub fn build_filter_expr(column: &str, operator: FilterOp, value: &str) -> DfResult<Expr> {
    let col_expr = col(column);

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
    let col_expr = col(&agg.column);

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

pub async fn handle_file_upload(mut multipart: Multipart) -> Result<String, DoubledeckerError> {
    use tokio::io::AsyncWriteExt;

    let upload_dir = "./uploads";
    tokio::fs::create_dir_all(upload_dir).await?;

    while let Some(field) = multipart.next_field().await? {
        if field.name() == Some("file") {
            let file_path = format!("{}/upload_{}.csv", upload_dir, Uuid::new_v4());

            eprintln!("Creating file: {}", file_path);

            // Collect all chunks into a buffer
            let mut stream = field;
            let mut buffer = Vec::new();
            let mut chunk_count = 0usize;

            while let Some(chunk) = stream.chunk().await? {
                buffer.extend_from_slice(&chunk);
                chunk_count += 1;

                if chunk_count % 100 == 0 {
                    eprintln!(
                        "Received {} bytes in {} chunks...",
                        buffer.len(),
                        chunk_count
                    );
                }
            }

            eprintln!("Received {} bytes in {} chunks", buffer.len(), chunk_count);

            // Parse CSV and normalize headers to lowercase
            let csv_str = String::from_utf8(buffer)
                .map_err(|e| DoubledeckerError::FileUpload(format!("Invalid UTF-8: {}", e)))?;

            let lines: Vec<&str> = csv_str.lines().collect();
            if lines.is_empty() {
                return Err(DoubledeckerError::FileUpload("Empty CSV file".to_string()));
            }

            // Normalize the header row to lowercase and reconstruct CSV
            let lowercase_header = lines[0].to_lowercase();
            let mut normalized_lines = vec![lowercase_header];
            normalized_lines.extend(lines[1..].iter().map(|s| s.to_string()));

            // Write the normalized CSV to file
            let normalized_csv = normalized_lines.join("\n");
            let mut file = tokio::fs::File::create(&file_path).await?;
            file.write_all(normalized_csv.as_bytes()).await?;
            file.flush().await?;

            eprintln!("Wrote normalized CSV with lowercase headers");
            return Ok(file_path);
        }
    }
    Err(DoubledeckerError::FileUpload(
        "No file field found in multipart data".to_string(),
    ))
}

fn extract_typed_value(
    array: &dyn Array,
    row_idx: usize,
) -> Result<serde_json::Value, DoubledeckerError> {
    use datafusion::arrow::datatypes::DataType;

    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let value = arr.value(row_idx);
            serde_json::Number::from_f64(value as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    DoubledeckerError::DataFusionError("Invalid float value".to_string())
                })
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let value = arr.value(row_idx);
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    DoubledeckerError::DataFusionError("Invalid float value".to_string())
                })
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(serde_json::Value::Bool(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(serde_json::Value::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(serde_json::Value::String(arr.value(row_idx).to_string()))
        }
        _ => {
            let value_str = datafusion::arrow::util::display::array_value_to_string(array, row_idx)
                .map_err(|e| DoubledeckerError::DataFusionError(e.to_string()))?;
            Ok(serde_json::Value::String(value_str))
        }
    }
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
    eprintln!("Batch schema: {:#?}", batches[0].schema());
    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut all_rows = Vec::new();

    for batch in batches.iter() {
        let num_rows = batch.num_rows();
        eprintln!("number of rows {}", num_rows);
        for row_idx in 0..num_rows {
            let mut row_data = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = extract_typed_value(col, row_idx)?;
                row_data.push(value);
            }
            all_rows.push(serde_json::Value::Array(row_data));
        }
    }

    Ok(QueryResponse {
        columns,
        rows: all_rows,
    })
}
