use crate::db::queries::{create_user, create_workspace, get_user_by_email, get_user_by_id, verify_password};
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use crate::utils::jwt::generate_token;
use axum::Json;
use axum::extract::State;
use crate::server::dtos::auth::*;
use std::env;

#[utoipa::path(
    post,
    path = "/auth/signup",
    request_body = RegisterRequest,
    responses(
        (status = 200, description = "User successfully registered", body = AuthResponse),
        (status = 400, description = "Bad request")
    ),
    tag = "auth"
)]
pub async fn signup(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, DoubledeckerError> {
    // Validate name
    if payload.name.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Name cannot be empty".to_string(),
        ));
    }

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

    let user = create_user(
        &state.db_pool,
        payload.name.clone(),
        payload.email,
        payload.password,
        payload.user_type,
    )
    .await?;

    // Automatically create a default workspace for the user upon signup
    let _workspace = create_workspace(
        &state.db_pool,
        user.id,
        format!("{}'s Catalog", user.name),
    )
    .await?;

    // Generate JWT token
    let jwt_secret = env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string());
    let token = generate_token(user.id, user.email.clone(), &jwt_secret)
        .map_err(|e| DoubledeckerError::Internal(format!("Token generation failed: {}", e)))?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo {
            id: user.id,
            name: user.name,
            email: user.email,
            user_type: user.user_type,
        },
    }))
}

#[utoipa::path(
    post,
    path = "/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "User successfully logged in", body = AuthResponse),
        (status = 401, description = "Invalid credentials")
    ),
    tag = "auth"
)]
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
            name: user.name,
            email: user.email,
            user_type: user.user_type,
        },
    }))
}

#[utoipa::path(
    get,
    path = "/profile",
    responses(
        (status = 200, description = "Get current user profile", body = UserInfo),
        (status = 401, description = "Unauthorized")
    ),
    tag = "auth"
)]
pub async fn get_profile(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<UserInfo>, DoubledeckerError> {
    let user = get_user_by_id(&state.db_pool, auth_user.user_id).await?;

    Ok(Json(UserInfo {
        id: user.id,
        name: user.name,
        email: user.email,
        user_type: user.user_type,
    }))
}
