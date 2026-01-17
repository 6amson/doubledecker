use crate::db::models::SavedQuery;
use crate::db::operations::{
    create_saved_query, delete_saved_query, get_saved_queries_by_user, get_saved_query,
    update_saved_query,
};
use crate::server::middleware::AuthenticatedUser;
use crate::utils::error::DoubledeckerError;
use crate::utils::statics::{
    AppState, CreateSavedQueryRequest, DeleteResponse, UpdateSavedQueryRequest,
};
use axum::Json;
use axum::extract::{Path, State};
use uuid::Uuid;

pub async fn create_saved_query_handler(
    State(state): State<AppState>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<CreateSavedQueryRequest>,
) -> Result<Json<SavedQuery>, DoubledeckerError> {
    let query_json = serde_json::to_value(&payload.query)
        .map_err(|e| DoubledeckerError::BadRequest(format!("Invalid query format: {}", e)))?;

    let saved_query = create_saved_query(
        &state.db_pool,
        auth_user.user_id,
        payload.name,
        payload.description,
        query_json,
    )
    .await?;

    Ok(Json(saved_query))
}

pub async fn list_saved_queries_handler(
    State(state): State<AppState>,
    auth_user: AuthenticatedUser,
) -> Result<Json<Vec<SavedQuery>>, DoubledeckerError> {
    let queries = get_saved_queries_by_user(&state.db_pool, auth_user.user_id).await?;
    Ok(Json(queries))
}

pub async fn get_saved_query_handler(
    State(state): State<AppState>,
    Path(query_id): Path<Uuid>,
    auth_user: AuthenticatedUser,
) -> Result<Json<SavedQuery>, DoubledeckerError> {
    let query = get_saved_query(&state.db_pool, query_id, auth_user.user_id).await?;
    Ok(Json(query))
}

pub async fn update_saved_query_handler(
    State(state): State<AppState>,
    Path(query_id): Path<Uuid>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<UpdateSavedQueryRequest>,
) -> Result<Json<SavedQuery>, DoubledeckerError> {
    let query_json = serde_json::to_value(&payload.query)
        .map_err(|e| DoubledeckerError::BadRequest(format!("Invalid query format: {}", e)))?;

    let updated_query = update_saved_query(
        &state.db_pool,
        query_id,
        auth_user.user_id,
        payload.name,
        payload.description,
        query_json,
    )
    .await?;

    Ok(Json(updated_query))
}

pub async fn delete_saved_query_handler(
    State(state): State<AppState>,
    Path(query_id): Path<Uuid>,
    auth_user: AuthenticatedUser,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    delete_saved_query(&state.db_pool, query_id, auth_user.user_id).await?;

    Ok(Json(DeleteResponse {
        message: "Saved query deleted successfully".to_string(),
    }))
}
