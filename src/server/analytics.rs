use crate::db::models::{PaginatedResponse, PaginationParams, QueryHistoryRecord, WorkspaceRole};
use crate::db::queries::{get_query_history_by_id, list_query_history, record_query_history};
use crate::server::extractors::verify_workspace_access;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use crate::utils::helpers::{parse_batch_to_json, query_response_to_csv};
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::Response;
use axum::Json;
use crate::server::dtos::analytics::*;
use datafusion::arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray};
use std::hash::{Hash, Hasher};
use std::time::Instant;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/analytics/query",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = AnalyticsQueryRequest,
    responses(
        (status = 200, description = "Analytics query executed", body = AnalyticsQueryResponse)
    ),
    tag = "analytics"
)]
pub async fn execute_query_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<AnalyticsQueryRequest>,
) -> Result<Json<AnalyticsQueryResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let start_time = Instant::now();
    let sql = payload.to_safe_sql()?;
    let batches = state
        .engine
        .execute_royalty_analytics(workspace_id, &sql)
        .await?;
    let elapsed_ms = start_time.elapsed().as_millis() as i64;
    let response = parse_batch_to_json(batches).await?;

    let row_count = response.rows.len() as i64;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    let query_id = format!("q_{:016x}", hasher.finish());

    let _ = record_query_history(
        &state.db_pool,
        workspace_id,
        Some(auth_user.user_id),
        &query_id,
        &sql,
        row_count,
        elapsed_ms,
    )
    .await;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/analytics/download",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = AnalyticsQueryRequest,
    responses(
        (status = 200, description = "Download query result as CSV", content_type = "text/csv")
    ),
    tag = "analytics"
)]
pub async fn download_query_csv_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<AnalyticsQueryRequest>,
) -> Result<Response<axum::body::Body>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let sql = payload.to_safe_sql()?;
    let batches = state
        .engine
        .execute_royalty_analytics(workspace_id, &sql)
        .await?;
    let response = parse_batch_to_json(batches).await?;
    let csv_content = query_response_to_csv(&response);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, "text/csv")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"royalty_analytics.csv\"",
        )
        .body(axum::body::Body::from(csv_content))
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to build response: {}", e)))?;

    Ok(response)
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/analytics/summary",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "Get analytics summary KPIs", body = AnalyticsSummaryResponse)
    ),
    tag = "analytics"
)]
pub async fn get_analytics_summary_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<AnalyticsSummaryResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let sql_summary = "SELECT COALESCE(SUM(net_revenue), 0) as total_rev, COUNT(DISTINCT isrc) as total_tracks, COALESCE(SUM(quantity), 0) as total_streams FROM royalty_data";
    let summary_batches = state
        .engine
        .execute_royalty_analytics(workspace_id, sql_summary)
        .await
        .unwrap_or_default();

    let mut total_net_revenue = 0.0;
    let mut total_tracks_monetized = 0i64;
    let mut total_streams = 0i64;

    if let Some(batch) = summary_batches.first() {
        if batch.num_rows() > 0 {
            if let Some(rev_col) = batch.column(0).as_any().downcast_ref::<Decimal128Array>() {
                if !rev_col.is_null(0) {
                    let mantissa = rev_col.value(0);
                    total_net_revenue = (mantissa as f64) / 1_000_000_000.0;
                }
            } else if let Some(rev_col) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
                if !rev_col.is_null(0) {
                    total_net_revenue = rev_col.value(0);
                }
            }

            if let Some(count_col) = batch.column(1).as_any().downcast_ref::<Int64Array>() {
                if !count_col.is_null(0) {
                    total_tracks_monetized = count_col.value(0);
                }
            }

            if let Some(streams_col) = batch.column(2).as_any().downcast_ref::<Int64Array>() {
                if !streams_col.is_null(0) {
                    total_streams = streams_col.value(0);
                }
            }
        }
    }

    let sql_platform = "SELECT platform, SUM(net_revenue) as rev FROM royalty_data GROUP BY platform ORDER BY rev DESC LIMIT 1";
    let platform_batches = state
        .engine
        .execute_royalty_analytics(workspace_id, sql_platform)
        .await
        .unwrap_or_default();
    let top_platform = extract_first_string(&platform_batches);

    let sql_track = "SELECT title, SUM(net_revenue) as rev FROM royalty_data GROUP BY title ORDER BY rev DESC LIMIT 1";
    let track_batches = state
        .engine
        .execute_royalty_analytics(workspace_id, sql_track)
        .await
        .unwrap_or_default();
    let top_track = extract_first_string(&track_batches);

    Ok(Json(AnalyticsSummaryResponse {
        total_net_revenue,
        total_streams,
        top_artist: None,
        top_platform,
        top_track,
        total_tracks_monetized,
    }))
}

fn extract_first_string(batches: &[RecordBatch]) -> Option<String> {
    if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            if let Some(str_col) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                if !str_col.is_null(0) {
                    return Some(str_col.value(0).to_string());
                }
            }
        }
    }
    None
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/analytics/history",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "List query history", body = PaginatedQueryHistory)
    ),
    tag = "analytics"
)]
pub async fn get_query_history_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<QueryHistoryRecord>>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;
    let limit = pagination.effective_limit();
    let history = list_query_history(&state.db_pool, workspace_id, pagination.cursor, limit).await?;
    Ok(Json(history))
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/analytics/history/{query_id}/download",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Download history query CSV", content_type = "text/csv")
    ),
    tag = "analytics"
)]
pub async fn download_query_history_csv_handler(
    auth_user: AuthenticatedUser,
    Path((workspace_id, query_id)): Path<(Uuid, String)>,
    State(state): State<AppState>,
) -> Result<Response<axum::body::Body>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let history_rec = get_query_history_by_id(&state.db_pool, workspace_id, &query_id).await?;
    let batches = state
        .engine
        .execute_royalty_analytics(workspace_id, &history_rec.sql_executed)
        .await?;
    let response = parse_batch_to_json(batches).await?;
    let csv_content = query_response_to_csv(&response);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, "text/csv")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"query_{}.csv\"", query_id),
        )
        .body(axum::body::Body::from(csv_content))
        .map_err(|e| DoubledeckerError::Internal(format!("Failed to build response: {}", e)))?;

    Ok(response)
}
