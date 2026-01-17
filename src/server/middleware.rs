use crate::utils::error::DoubledeckerError;
use crate::utils::jwt::verify_token;
use axum::{RequestPartsExt, async_trait, extract::FromRequestParts, http::request::Parts};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use std::env;
use uuid::Uuid;

/// Authenticated user extractor - validates JWT and extracts user ID
pub struct AuthenticatedUser {
    pub user_id: Uuid,
    pub email: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = DoubledeckerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the Authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| DoubledeckerError::Unauthorized)?;

        // Get JWT secret from environment
        let jwt_secret = env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string());

        // Verify and decode the token
        let claims = verify_token(bearer.token(), &jwt_secret)
            .map_err(|e| DoubledeckerError::AuthenticationError(format!("Invalid token: {}", e)))?;

        // Parse user ID from claims
        let user_id = Uuid::parse_str(&claims.sub).map_err(|_| {
            DoubledeckerError::AuthenticationError("Invalid user ID in token".to_string())
        })?;

        Ok(AuthenticatedUser {
            user_id,
            email: claims.email,
        })
    }
}
