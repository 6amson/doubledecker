use crate::db::models::{PaginatedResponse, PaginationParams, Payee};
use crate::db::queries::{create_payee, delete_payee, get_payees, update_payee};
use crate::server::dtos::DeleteResponse;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use axum::extract::{Path, Query, State};
use axum::Json;
use crate::server::dtos::payees::*;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/api/v1/payees",
    request_body = CreatePayeeRequest,
    responses(
        (status = 200, description = "Payee created", body = Payee)
    ),
    tag = "payees"
)]
pub async fn create_payee_handler(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreatePayeeRequest>,
) -> Result<Json<Payee>, DoubledeckerError> {
    if payload.name.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Payee name cannot be empty".to_string(),
        ));
    }

    let payee = create_payee(
        &state.db_pool,
        auth_user.user_id,
        payload.name,
        payload.email,
        payload.bank_account,
        payload.tax_id,
    )
    .await?;
    Ok(Json(payee))
}

#[utoipa::path(
    get,
    path = "/api/v1/payees",
    params(PaginationParams),
    responses(
        (status = 200, description = "List payees", body = PaginatedPayees)
    ),
    tag = "payees"
)]
pub async fn list_payees_handler(
    auth_user: AuthenticatedUser,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<Payee>>, DoubledeckerError> {
    let limit = pagination.effective_limit();
    let payees = get_payees(&state.db_pool, auth_user.user_id, pagination.cursor, limit).await?;
    Ok(Json(payees))
}

#[utoipa::path(
    put,
    path = "/api/v1/payees/{payee_id}",
    params(
        ("payee_id" = Uuid, Path, description = "Payee ID")
    ),
    request_body = UpdatePayeeRequest,
    responses(
        (status = 200, description = "Payee updated successfully", body = Payee)
    ),
    tag = "payees"
)]
pub async fn update_payee_handler(
    _auth_user: AuthenticatedUser,
    Path(payee_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<UpdatePayeeRequest>,
) -> Result<Json<Payee>, DoubledeckerError> {
    let updated = update_payee(
        &state.db_pool,
        payee_id,
        payload.name.as_deref(),
        payload.email.as_deref(),
        payload.bank_account.as_deref(),
        payload.tax_id.as_deref(),
    )
    .await?;
    Ok(Json(updated))
}

#[utoipa::path(
    delete,
    path = "/api/v1/payees/{payee_id}",
    params(
        ("payee_id" = Uuid, Path, description = "Payee ID")
    ),
    responses(
        (status = 200, description = "Payee deleted successfully", body = DeleteResponse)
    ),
    tag = "payees"
)]
pub async fn delete_payee_handler(
    _auth_user: AuthenticatedUser,
    Path(payee_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    let _ = delete_payee(&state.db_pool, payee_id).await?;
    Ok(Json(DeleteResponse {
        message: "Payee deleted successfully".to_string(),
    }))
}
