use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePayeeRequest {
    pub name: String,
    pub email: Option<String>,
    pub bank_account: Option<String>,
    pub tax_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdatePayeeRequest {
    pub name: Option<String>,
    pub email: Option<String>,
    pub bank_account: Option<String>,
    pub tax_id: Option<String>,
}
