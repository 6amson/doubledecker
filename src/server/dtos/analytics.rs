use crate::utils::error::DoubledeckerError;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DateRangeFilter {
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryFilter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StructuredAnalyticsQuery {
    pub date_range: Option<DateRangeFilter>,
    pub dimensions: Option<Vec<String>>,
    pub metrics: Option<Vec<String>>,
    pub filters: Option<Vec<QueryFilter>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AnalyticsQueryRequest {
    pub sql: Option<String>,
    #[serde(flatten)]
    pub structured: Option<StructuredAnalyticsQuery>,
    pub dataset_ids: Option<Vec<Uuid>>,
}

impl AnalyticsQueryRequest {
    pub fn to_safe_sql(&self) -> Result<String, DoubledeckerError> {
        if let Some(ref sql) = self.sql {
            return Ok(sql.clone());
        }

        let structured = self.structured.as_ref().ok_or_else(|| {
            DoubledeckerError::BadRequest(
                "Either 'sql' or structured query parameters ('dimensions', 'metrics', etc.) must be provided"
                    .to_string(),
            )
        })?;

        let allowed_dims = [
            "isrc",
            "upc",
            "title",
            "artist",
            "album",
            "platform",
            "territory",
            "country",
            "transaction_type",
            "reporting_date",
            "currency",
        ];
        let mut dims = Vec::new();
        if let Some(ref d_list) = structured.dimensions {
            for d in d_list {
                if !allowed_dims.contains(&d.as_str()) {
                    return Err(DoubledeckerError::BadRequest(
                        format!("Dimension '{}' is not allowed", d),
                    ));
                }
                dims.push(d.clone());
            }
        }

        let mut select_clauses = dims.clone();
        if let Some(ref m_list) = structured.metrics {
            for m in m_list {
                match m.as_str() {
                    "net_revenue" => select_clauses.push("SUM(net_revenue) AS total_revenue".to_string()),
                    "quantity" => select_clauses.push("SUM(quantity) AS total_streams".to_string()),
                    other => {
                        return Err(DoubledeckerError::BadRequest(
                            format!("Metric '{}' is not supported", other),
                        ));
                    }
                }
            }
        }

        if select_clauses.is_empty() {
            select_clauses.push("*".to_string());
        }

        let mut where_clauses = Vec::new();
        if let Some(ref dr) = structured.date_range {
            if let Some(from) = dr.from {
                where_clauses.push(format!("reporting_date >= '{}'", from));
            }
            if let Some(to) = dr.to {
                where_clauses.push(format!("reporting_date <= '{}'", to));
            }
        }

        if let Some(ref filters) = structured.filters {
            for f in filters {
                if !allowed_dims.contains(&f.field.as_str()) {
                    return Err(DoubledeckerError::BadRequest(
                        format!("Filter field '{}' is not allowed", f.field),
                    ));
                }
                let val = f.value.replace('\'', "''");
                match f.operator {
                    FilterOperator::Eq => where_clauses.push(format!("{} = '{}'", f.field, val)),
                    FilterOperator::Ne => where_clauses.push(format!("{} != '{}'", f.field, val)),
                    FilterOperator::Gt => where_clauses.push(format!("{} > '{}'", f.field, val)),
                    FilterOperator::Gte => where_clauses.push(format!("{} >= '{}'", f.field, val)),
                    FilterOperator::Lt => where_clauses.push(format!("{} < '{}'", f.field, val)),
                    FilterOperator::Lte => where_clauses.push(format!("{} <= '{}'", f.field, val)),
                    FilterOperator::Like => where_clauses.push(format!("{} LIKE '{}'", f.field, val)),
                    FilterOperator::In => where_clauses.push(format!("{} = '{}'", f.field, val)),
                }
            }
        }

        let where_stmt = if where_clauses.is_empty() {
            "".to_string()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let group_stmt = if !dims.is_empty() && structured.metrics.is_some() {
            format!(" GROUP BY {}", dims.join(", "))
        } else {
            "".to_string()
        };

        let limit_stmt = format!(" LIMIT {}", structured.limit.unwrap_or(100));

        let sql = format!(
            "SELECT {from_cols} FROM royalty_data{where_stmt}{group_stmt}{limit_stmt}",
            from_cols = select_clauses.join(", "),
            where_stmt = where_stmt,
            group_stmt = group_stmt,
            limit_stmt = limit_stmt
        );

        Ok(sql)
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AnalyticsSummaryRequest {
    pub dataset_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AnalyticsQueryResponse {
    pub columns: Vec<String>,
    #[schema(value_type = Vec<Object>)]
    pub rows: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AnalyticsSummaryResponse {
    pub total_net_revenue: f64,
    pub total_streams: i64,
    pub top_artist: Option<String>,
    pub top_platform: Option<String>,
    pub top_track: Option<String>,
    pub total_tracks_monetized: i64,
}
