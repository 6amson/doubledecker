use crate::db::operations::{create_user, get_user_by_email, verify_password};
use crate::utils::error::DoubledeckerError;
use crate::utils::jwt::generate_token;
use crate::utils::statics::{AppState, AuthResponse, LoginRequest, RegisterRequest, UserInfo};
use axum::Json;
use axum::extract::State;
use std::env;

pub async fn signup(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, DoubledeckerError> {
    // Validate email format (basic check)
    if !payload.email.contains('@') {
        return Err(DoubledeckerError::BadRequest(
            "Invalid email format".to_string(),
        ));
    }

    // Validate password length
    if payload.password.len() < 6 {
        return Err(DoubledeckerError::BadRequest(
            "Password must be at least 6 characters".to_string(),
        ));
    }

    let user = create_user(&state.db_pool, payload.email, payload.password).await?;

    // Generate JWT token
    let jwt_secret = env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string());
    let token = generate_token(user.id, user.email.clone(), &jwt_secret)
        .map_err(|e| DoubledeckerError::Internal(format!("Token generation failed: {}", e)))?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo {
            id: user.id,
            email: user.email,
            total_queries: user.total_queries,
            total_files_processed: user.total_files_processed,
            total_saved_queries: user.total_saved_queries,
        },
    }))
}

pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, DoubledeckerError> {
    let user = get_user_by_email(&state.db_pool, &payload.email).await?;

    let is_valid = verify_password(&payload.password, &user.password_hash)?;

    if !is_valid {
        return Err(DoubledeckerError::AuthenticationError(
            "Invalid credentials".to_string(),
        ));
    }

    // Generate JWT token
    let jwt_secret = env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string());
    let token = generate_token(user.id, user.email.clone(), &jwt_secret)
        .map_err(|e| DoubledeckerError::Internal(format!("Token generation failed: {}", e)))?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo {
            id: user.id,
            email: user.email,
            total_queries: user.total_queries,
            total_files_processed: user.total_files_processed,
            total_saved_queries: user.total_saved_queries,
        },
    }))
}
