use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSplitRequest {
    pub artist_id: Option<Uuid>,
    pub album_id: Option<Uuid>,
    pub track_id: Option<Uuid>,
    pub payee_id: Option<Uuid>,
    pub payee_name: String,
    pub percentage: Decimal,
    pub effective_from: Option<NaiveDate>,
    pub effective_to: Option<NaiveDate>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateSplitRequest {
    pub percentage: Option<Decimal>,
    pub payee_id: Option<Uuid>,
    pub payee_name: Option<String>,
    pub effective_from: Option<NaiveDate>,
    pub effective_to: Option<NaiveDate>,
}
