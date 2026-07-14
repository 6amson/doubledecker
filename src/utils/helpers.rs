use crate::utils::error::DoubledeckerError;
use crate::server::dtos::analytics::AnalyticsQueryResponse;
use arrow_json::writer::{JsonArray, WriterBuilder};
use datafusion::arrow::array::RecordBatch;

pub async fn parse_batch_to_json(
    batches: Vec<RecordBatch>,
) -> Result<AnalyticsQueryResponse, DoubledeckerError> {
    if batches.is_empty() {
        return Ok(AnalyticsQueryResponse {
            columns: vec![],
            rows: vec![],
        });
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(&mut buf);

    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
    writer
        .write_batches(&batch_refs)
        .map_err(|e| DoubledeckerError::DataFusionError(format!("JSON conversion error: {}", e)))?;
    writer.finish().map_err(|e| {
        DoubledeckerError::DataFusionError(format!("JSON finalization error: {}", e))
    })?;

    let json_str = String::from_utf8(buf).map_err(|e| {
        DoubledeckerError::DataFusionError(format!("UTF-8 conversion error: {}", e))
    })?;

    let json_objects: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(&json_str)
            .map_err(|e| DoubledeckerError::DataFusionError(format!("JSON parse error: {}", e)))?;

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

    Ok(AnalyticsQueryResponse {
        columns,
        rows: json_rows,
    })
}

pub fn query_response_to_csv(response: &AnalyticsQueryResponse) -> String {
    let mut csv = String::new();

    csv.push_str(&response.columns.join(","));
    csv.push('\n');

    for row in &response.rows {
        if let serde_json::Value::Array(values) = row {
            let row_str: Vec<String> = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => {
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
