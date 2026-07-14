use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::utils::error::DoubledeckerError;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

pub fn unified_royalty_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("isrc", DataType::Utf8, false),
        Field::new("upc", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, false),
        Field::new("artist", DataType::Utf8, false),
        Field::new("album", DataType::Utf8, true),
        Field::new("platform", DataType::Utf8, false),
        Field::new("territory", DataType::Utf8, true),
        Field::new("transaction_type", DataType::Utf8, true),
        Field::new("reporting_date", DataType::Date32, false),
        Field::new("net_revenue", DataType::Decimal128(38, 9), false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
    ]))
}

#[async_trait::async_trait]
pub trait RoyaltyAdapter: Send + Sync {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, DoubledeckerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum DistributorSource {
    DistroKid,
    TuneCore,
    CDBaby,
    Symphonic,
}

impl DistributorSource {
    /// Automatically detects distributor source from CSV content snippet
    pub fn detect_from_csv_bytes(bytes: &[u8]) -> Self {
        let snippet = std::str::from_utf8(&bytes[..bytes.len().min(1024)])
            .unwrap_or("")
            .to_lowercase();

        if snippet.contains("sales type") || snippet.contains("total earned") {
            Self::TuneCore
        } else if snippet.contains("partner") || snippet.contains("payable") {
            Self::CDBaby
        } else if snippet.contains("dsp") || snippet.contains("net revenue") {
            Self::Symphonic
        } else {
            Self::DistroKid
        }
    }

    pub fn from_str_lenient(s: &str) -> Option<Self> {
        let lower = s.trim().to_lowercase();
        match lower.as_str() {
            "distrokid" | "distro_kid" => Some(Self::DistroKid),
            "tunecore" | "tune_core" => Some(Self::TuneCore),
            "cdbaby" | "cd_baby" => Some(Self::CDBaby),
            "symphonic" => Some(Self::Symphonic),
            _ => None,
        }
    }

    pub fn to_adapter(&self) -> Box<dyn RoyaltyAdapter> {
        match self {
            Self::DistroKid => Box::new(DistroKidAdapter),
            Self::TuneCore => Box::new(TunecoreAdapter),
            Self::CDBaby => Box::new(CDBabyAdapter),
            Self::Symphonic => Box::new(SymphonicAdapter),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DistroKid => "DistroKid",
            Self::TuneCore => "TuneCore",
            Self::CDBaby => "CDBaby",
            Self::Symphonic => "Symphonic",
        }
    }
}

fn find_column_idx(schema: &Schema, aliases: &[&str]) -> Option<usize> {
    for (idx, field) in schema.fields().iter().enumerate() {
        let name_lower = field.name().to_lowercase();
        let name_trimmed = name_lower.trim();
        for alias in aliases {
            if name_trimmed == *alias || name_trimmed.contains(alias) {
                return Some(idx);
            }
        }
    }
    None
}

fn array_to_string_vec(arr: &ArrayRef, num_rows: usize) -> Vec<String> {
    if let Ok(cast_arr) = cast(arr, &DataType::Utf8) {
        if let Some(str_arr) = cast_arr.as_string_opt::<i32>() {
            return (0..num_rows)
                .map(|i| {
                    if str_arr.is_null(i) {
                        String::new()
                    } else {
                        str_arr.value(i).to_string()
                    }
                })
                .collect();
        }
    }
    vec![String::new(); num_rows]
}

fn get_opt_string_array(
    batch: &RecordBatch,
    schema: &Schema,
    aliases: &[&str],
    num_rows: usize,
    default_val: &str,
) -> ArrayRef {
    if let Some(idx) = find_column_idx(schema, aliases) {
        Arc::new(StringArray::from(array_to_string_vec(
            batch.column(idx),
            num_rows,
        ))) as ArrayRef
    } else {
        Arc::new(StringArray::from(vec![default_val.to_string(); num_rows])) as ArrayRef
    }
}

fn array_to_i64_vec(arr: &ArrayRef, num_rows: usize) -> Vec<i64> {
    if let Ok(cast_arr) = cast(arr, &DataType::Int64) {
        if let Some(int_arr) = cast_arr.as_any().downcast_ref::<Int64Array>() {
            return (0..num_rows)
                .map(|i| {
                    if int_arr.is_null(i) {
                        1
                    } else {
                        int_arr.value(i)
                    }
                })
                .collect();
        }
    }
    let str_vec = array_to_string_vec(arr, num_rows);
    str_vec
        .iter()
        .map(|s| {
            let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '-').collect();
            cleaned.parse::<i64>().unwrap_or(1)
        })
        .collect()
}

fn parse_date_to_epoch_days(s: &str) -> i32 {
    let s_trimmed = s.trim();
    if s_trimmed.is_empty() {
        return 0;
    }

    let formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y-%m",
        "%m/%Y",
    ];

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    for fmt in &formats {
        if let Ok(date) = NaiveDate::parse_from_str(s_trimmed, fmt) {
            return (date - epoch).num_days() as i32;
        }
    }

    // Try adding day if Year-Month or Month-Year failed parse_from_str
    if let Ok(date) = NaiveDate::parse_from_str(&format!("{}-01", s_trimmed), "%Y-%m-%d") {
        return (date - epoch).num_days() as i32;
    }
    if let Ok(date) = NaiveDate::parse_from_str(&format!("01/{}", s_trimmed), "%d/%m/%Y") {
        return (date - epoch).num_days() as i32;
    }

    0
}

fn parse_currency_to_decimal_mantissa(s: &str) -> i128 {
    let cleaned: String = s
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();

    if let Ok(mut dec) = Decimal::from_str(&cleaned) {
        dec.rescale(9);
        dec.mantissa()
    } else {
        0
    }
}

pub struct DistroKidAdapter;

#[async_trait::async_trait]
impl RoyaltyAdapter for DistroKidAdapter {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, DoubledeckerError> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let isrc_idx = find_column_idx(&schema, &["isrc", "isrc code"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing ISRC column in DistroKid file".to_string()))?;
        let title_idx = find_column_idx(&schema, &["song title", "title", "track title"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Title column in DistroKid file".to_string()))?;
        let platform_idx = find_column_idx(&schema, &["store", "platform", "service", "shop"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Store column in DistroKid file".to_string()))?;
        let date_idx = find_column_idx(&schema, &["reporting month", "month", "date", "period"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Reporting Month column in DistroKid file".to_string()))?;
        let earnings_idx = find_column_idx(&schema, &["earnings (usd)", "earnings", "revenue", "amount", "net"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Earnings column in DistroKid file".to_string()))?;
        let quantity_idx = find_column_idx(&schema, &["quantity", "units", "streams", "stream count", "count"]);

        let isrc_vec = array_to_string_vec(batch.column(isrc_idx), num_rows);
        let title_vec = array_to_string_vec(batch.column(title_idx), num_rows);
        let platform_vec = array_to_string_vec(batch.column(platform_idx), num_rows);
        let date_str_vec = array_to_string_vec(batch.column(date_idx), num_rows);
        let earnings_str_vec = array_to_string_vec(batch.column(earnings_idx), num_rows);
        let quantity_vec: Vec<i64> = if let Some(idx) = quantity_idx {
            array_to_i64_vec(batch.column(idx), num_rows)
        } else {
            vec![1; num_rows]
        };

        let date_days: Vec<i32> = date_str_vec.iter().map(|s| parse_date_to_epoch_days(s)).collect();
        let revenue_mantissas: Vec<i128> = earnings_str_vec.iter().map(|s| parse_currency_to_decimal_mantissa(s)).collect();
        let currency_vec: Vec<String> = vec!["USD".to_string(); num_rows];

        let isrc_array = Arc::new(StringArray::from(isrc_vec)) as ArrayRef;
        let upc_array = get_opt_string_array(&batch, &schema, &["upc", "upc code", "barcode", "gtin", "ean", "album upc", "release upc", "ean/upc"], num_rows, "");
        let title_array = Arc::new(StringArray::from(title_vec)) as ArrayRef;
        let artist_array = get_opt_string_array(&batch, &schema, &["artist", "artist name", "band", "performer"], num_rows, "Unknown Artist");
        let album_array = get_opt_string_array(&batch, &schema, &["album", "album title", "release", "release title", "project", "project title"], num_rows, "");
        let platform_array = Arc::new(StringArray::from(platform_vec)) as ArrayRef;
        let territory_array = get_opt_string_array(&batch, &schema, &["territory", "country", "country code", "region", "sale country"], num_rows, "");
        let transaction_type_array = get_opt_string_array(&batch, &schema, &["transaction type", "sale type", "type", "activity type", "stream type"], num_rows, "");
        let date_array = Arc::new(Date32Array::from(date_days)) as ArrayRef;
        let revenue_array = Arc::new(Decimal128Array::from(revenue_mantissas).with_data_type(DataType::Decimal128(38, 9))) as ArrayRef;
        let currency_array = Arc::new(StringArray::from(currency_vec)) as ArrayRef;
        let quantity_array = Arc::new(Int64Array::from(quantity_vec)) as ArrayRef;

        let unified_schema = unified_royalty_schema();
        RecordBatch::try_new(
            unified_schema,
            vec![
                isrc_array,
                upc_array,
                title_array,
                artist_array,
                album_array,
                platform_array,
                territory_array,
                transaction_type_array,
                date_array,
                revenue_array,
                currency_array,
                quantity_array,
            ],
        )
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to create normalized RecordBatch: {}", e)))
    }
}

pub struct TunecoreAdapter;

#[async_trait::async_trait]
impl RoyaltyAdapter for TunecoreAdapter {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, DoubledeckerError> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let isrc_idx = find_column_idx(&schema, &["isrc", "isrc code"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing ISRC column in TuneCore file".to_string()))?;
        let title_idx = find_column_idx(&schema, &["song title", "title", "track", "track title"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Title column in TuneCore file".to_string()))?;
        let platform_idx = find_column_idx(&schema, &["store", "platform", "sales platform", "sales type"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Store column in TuneCore file".to_string()))?;
        let date_idx = find_column_idx(&schema, &["sales period", "reporting date", "date", "period", "posting date"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Date column in TuneCore file".to_string()))?;
        let earnings_idx = find_column_idx(&schema, &["total earned", "earnings", "revenue", "net revenue"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Earnings column in TuneCore file".to_string()))?;
        let currency_idx = find_column_idx(&schema, &["currency", "curr"]);
        let quantity_idx = find_column_idx(&schema, &["quantity", "units", "streams", "stream count", "count"]);

        let isrc_vec = array_to_string_vec(batch.column(isrc_idx), num_rows);
        let title_vec = array_to_string_vec(batch.column(title_idx), num_rows);
        let platform_vec = array_to_string_vec(batch.column(platform_idx), num_rows);
        let date_str_vec = array_to_string_vec(batch.column(date_idx), num_rows);
        let earnings_str_vec = array_to_string_vec(batch.column(earnings_idx), num_rows);

        let currency_vec: Vec<String> = if let Some(idx) = currency_idx {
            array_to_string_vec(batch.column(idx), num_rows)
        } else {
            vec!["USD".to_string(); num_rows]
        };
        let quantity_vec: Vec<i64> = if let Some(idx) = quantity_idx {
            array_to_i64_vec(batch.column(idx), num_rows)
        } else {
            vec![1; num_rows]
        };

        let date_days: Vec<i32> = date_str_vec.iter().map(|s| parse_date_to_epoch_days(s)).collect();
        let revenue_mantissas: Vec<i128> = earnings_str_vec.iter().map(|s| parse_currency_to_decimal_mantissa(s)).collect();

        let isrc_array = Arc::new(StringArray::from(isrc_vec)) as ArrayRef;
        let upc_array = get_opt_string_array(&batch, &schema, &["upc", "upc code", "barcode", "gtin", "ean", "album upc", "release upc", "ean/upc"], num_rows, "");
        let title_array = Arc::new(StringArray::from(title_vec)) as ArrayRef;
        let artist_array = get_opt_string_array(&batch, &schema, &["artist", "artist name", "band", "performer"], num_rows, "Unknown Artist");
        let album_array = get_opt_string_array(&batch, &schema, &["album", "album title", "release", "release title", "project", "project title"], num_rows, "");
        let platform_array = Arc::new(StringArray::from(platform_vec)) as ArrayRef;
        let territory_array = get_opt_string_array(&batch, &schema, &["territory", "country", "country code", "region", "sale country"], num_rows, "");
        let transaction_type_array = get_opt_string_array(&batch, &schema, &["transaction type", "sale type", "type", "activity type", "stream type"], num_rows, "");
        let date_array = Arc::new(Date32Array::from(date_days)) as ArrayRef;
        let revenue_array = Arc::new(Decimal128Array::from(revenue_mantissas).with_data_type(DataType::Decimal128(38, 9))) as ArrayRef;
        let currency_array = Arc::new(StringArray::from(currency_vec)) as ArrayRef;
        let quantity_array = Arc::new(Int64Array::from(quantity_vec)) as ArrayRef;

        let unified_schema = unified_royalty_schema();
        RecordBatch::try_new(
            unified_schema,
            vec![
                isrc_array,
                upc_array,
                title_array,
                artist_array,
                album_array,
                platform_array,
                territory_array,
                transaction_type_array,
                date_array,
                revenue_array,
                currency_array,
                quantity_array,
            ],
        )
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to create normalized RecordBatch: {}", e)))
    }
}

pub struct CDBabyAdapter;

#[async_trait::async_trait]
impl RoyaltyAdapter for CDBabyAdapter {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, DoubledeckerError> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let isrc_idx = find_column_idx(&schema, &["isrc"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing ISRC column in CDBaby file".to_string()))?;
        let title_idx = find_column_idx(&schema, &["track title", "song title", "title"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Title column in CDBaby file".to_string()))?;
        let platform_idx = find_column_idx(&schema, &["partner", "store", "platform"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Partner/Platform column in CDBaby file".to_string()))?;
        let date_idx = find_column_idx(&schema, &["period", "accounting period", "date", "reporting period"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Period column in CDBaby file".to_string()))?;
        let earnings_idx = find_column_idx(&schema, &["payable", "net revenue", "earnings", "amount"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Payable/Earnings column in CDBaby file".to_string()))?;
        let currency_idx = find_column_idx(&schema, &["currency"]);
        let quantity_idx = find_column_idx(&schema, &["quantity", "units"]);

        let isrc_vec = array_to_string_vec(batch.column(isrc_idx), num_rows);
        let title_vec = array_to_string_vec(batch.column(title_idx), num_rows);
        let platform_vec = array_to_string_vec(batch.column(platform_idx), num_rows);
        let date_str_vec = array_to_string_vec(batch.column(date_idx), num_rows);
        let earnings_str_vec = array_to_string_vec(batch.column(earnings_idx), num_rows);

        let currency_vec: Vec<String> = if let Some(idx) = currency_idx {
            array_to_string_vec(batch.column(idx), num_rows)
        } else {
            vec!["USD".to_string(); num_rows]
        };
        let quantity_vec: Vec<i64> = if let Some(idx) = quantity_idx {
            array_to_i64_vec(batch.column(idx), num_rows)
        } else {
            vec![1; num_rows]
        };

        let date_days: Vec<i32> = date_str_vec.iter().map(|s| parse_date_to_epoch_days(s)).collect();
        let revenue_mantissas: Vec<i128> = earnings_str_vec.iter().map(|s| parse_currency_to_decimal_mantissa(s)).collect();

        let isrc_array = Arc::new(StringArray::from(isrc_vec)) as ArrayRef;
        let upc_array = get_opt_string_array(&batch, &schema, &["upc", "upc code", "barcode", "gtin", "ean", "album upc", "release upc", "ean/upc"], num_rows, "");
        let title_array = Arc::new(StringArray::from(title_vec)) as ArrayRef;
        let artist_array = get_opt_string_array(&batch, &schema, &["artist", "artist name", "band", "performer"], num_rows, "Unknown Artist");
        let album_array = get_opt_string_array(&batch, &schema, &["album", "album title", "release", "release title", "project", "project title"], num_rows, "");
        let platform_array = Arc::new(StringArray::from(platform_vec)) as ArrayRef;
        let territory_array = get_opt_string_array(&batch, &schema, &["territory", "country", "country code", "region", "sale country"], num_rows, "");
        let transaction_type_array = get_opt_string_array(&batch, &schema, &["transaction type", "sale type", "type", "activity type", "stream type"], num_rows, "");
        let date_array = Arc::new(Date32Array::from(date_days)) as ArrayRef;
        let revenue_array = Arc::new(Decimal128Array::from(revenue_mantissas).with_data_type(DataType::Decimal128(38, 9))) as ArrayRef;
        let currency_array = Arc::new(StringArray::from(currency_vec)) as ArrayRef;
        let quantity_array = Arc::new(Int64Array::from(quantity_vec)) as ArrayRef;

        RecordBatch::try_new(
            unified_royalty_schema(),
            vec![
                isrc_array,
                upc_array,
                title_array,
                artist_array,
                album_array,
                platform_array,
                territory_array,
                transaction_type_array,
                date_array,
                revenue_array,
                currency_array,
                quantity_array,
            ],
        )
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to create normalized RecordBatch: {}", e)))
    }
}

pub struct SymphonicAdapter;

#[async_trait::async_trait]
impl RoyaltyAdapter for SymphonicAdapter {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, DoubledeckerError> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let isrc_idx = find_column_idx(&schema, &["isrc"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing ISRC column in Symphonic file".to_string()))?;
        let title_idx = find_column_idx(&schema, &["track title", "title"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Title column in Symphonic file".to_string()))?;
        let platform_idx = find_column_idx(&schema, &["dsp", "store", "platform"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing DSP/Platform column in Symphonic file".to_string()))?;
        let date_idx = find_column_idx(&schema, &["reporting month", "date", "month"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Date column in Symphonic file".to_string()))?;
        let earnings_idx = find_column_idx(&schema, &["net revenue", "earnings", "total"])
            .ok_or_else(|| DoubledeckerError::BadRequest("Missing Net Revenue column in Symphonic file".to_string()))?;
        let currency_idx = find_column_idx(&schema, &["currency"]);
        let quantity_idx = find_column_idx(&schema, &["units", "quantity"]);

        let isrc_vec = array_to_string_vec(batch.column(isrc_idx), num_rows);
        let title_vec = array_to_string_vec(batch.column(title_idx), num_rows);
        let platform_vec = array_to_string_vec(batch.column(platform_idx), num_rows);
        let date_str_vec = array_to_string_vec(batch.column(date_idx), num_rows);
        let earnings_str_vec = array_to_string_vec(batch.column(earnings_idx), num_rows);

        let currency_vec: Vec<String> = if let Some(idx) = currency_idx {
            array_to_string_vec(batch.column(idx), num_rows)
        } else {
            vec!["USD".to_string(); num_rows]
        };
        let quantity_vec: Vec<i64> = if let Some(idx) = quantity_idx {
            array_to_i64_vec(batch.column(idx), num_rows)
        } else {
            vec![1; num_rows]
        };

        let date_days: Vec<i32> = date_str_vec.iter().map(|s| parse_date_to_epoch_days(s)).collect();
        let revenue_mantissas: Vec<i128> = earnings_str_vec.iter().map(|s| parse_currency_to_decimal_mantissa(s)).collect();

        let isrc_array = Arc::new(StringArray::from(isrc_vec)) as ArrayRef;
        let upc_array = get_opt_string_array(&batch, &schema, &["upc", "upc code", "barcode", "gtin", "ean", "album upc", "release upc", "ean/upc"], num_rows, "");
        let title_array = Arc::new(StringArray::from(title_vec)) as ArrayRef;
        let artist_array = get_opt_string_array(&batch, &schema, &["artist", "artist name", "band", "performer"], num_rows, "Unknown Artist");
        let album_array = get_opt_string_array(&batch, &schema, &["album", "album title", "release", "release title", "project", "project title"], num_rows, "");
        let platform_array = Arc::new(StringArray::from(platform_vec)) as ArrayRef;
        let territory_array = get_opt_string_array(&batch, &schema, &["territory", "country", "country code", "region", "sale country"], num_rows, "");
        let transaction_type_array = get_opt_string_array(&batch, &schema, &["transaction type", "sale type", "type", "activity type", "stream type"], num_rows, "");
        let date_array = Arc::new(Date32Array::from(date_days)) as ArrayRef;
        let revenue_array = Arc::new(Decimal128Array::from(revenue_mantissas).with_data_type(DataType::Decimal128(38, 9))) as ArrayRef;
        let currency_array = Arc::new(StringArray::from(currency_vec)) as ArrayRef;
        let quantity_array = Arc::new(Int64Array::from(quantity_vec)) as ArrayRef;

        RecordBatch::try_new(
            unified_royalty_schema(),
            vec![
                isrc_array,
                upc_array,
                title_array,
                artist_array,
                album_array,
                platform_array,
                territory_array,
                transaction_type_array,
                date_array,
                revenue_array,
                currency_array,
                quantity_array,
            ],
        )
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to create normalized RecordBatch: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_csv::reader::Format;
    use datafusion::arrow::array::{Decimal128Array, StringArray};
    use std::io::Cursor;

    #[test]
    fn test_distrokid_adapter_normalization() {
        let csv_content = "ISRC,Song Title,Store,Reporting Month,Earnings (USD),Currency\nUS1234567890,My Song,Spotify,2026-06,12.345678,USD\n";
        let mut cursor = Cursor::new(csv_content.as_bytes());
        let format = Format::default().with_header(true);
        let (schema, _) = format.infer_schema(&mut cursor, Some(10)).unwrap();
        cursor.set_position(0);

        let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(cursor)
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        let adapter = DistroKidAdapter;
        let norm = adapter.normalize_batch(batch).unwrap();

        assert_eq!(norm.num_rows(), 1);
        assert_eq!(norm.schema().fields().len(), 7);

        let isrc_col = norm.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(isrc_col.value(0), "US1234567890");

        let rev_col = norm.column(4).as_any().downcast_ref::<Decimal128Array>().unwrap();
        // 12.345678 scaled by 10^9 is 12345678000
        assert_eq!(rev_col.value(0), 12_345_678_000);

        let curr_col = norm.column(5).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(curr_col.value(0), "USD");

        let qty_col = norm.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(qty_col.value(0), 1);
    }

    #[test]
    fn test_tunecore_adapter_normalization() {
        let csv_content = "ISRC,Track Title,Sales Type,Posting Date,Total Earned,Currency\nTC9876543210,Tune Song,Apple Music,2026-05-15,99.99,EUR\n";
        let mut cursor = Cursor::new(csv_content.as_bytes());
        let format = Format::default().with_header(true);
        let (schema, _) = format.infer_schema(&mut cursor, Some(10)).unwrap();
        cursor.set_position(0);

        let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(cursor)
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        let adapter = TunecoreAdapter;
        let norm = adapter.normalize_batch(batch).unwrap();

        assert_eq!(norm.num_rows(), 1);
        let isrc_col = norm.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(isrc_col.value(0), "TC9876543210");

        let rev_col = norm.column(4).as_any().downcast_ref::<Decimal128Array>().unwrap();
        // 99.99 scaled by 10^9 is 99990000000
        assert_eq!(rev_col.value(0), 99_990_000_000);
    }
}
