use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,   // Subject (user ID)
    pub email: String, // User email
    pub exp: i64,      // Expiration time
    pub iat: i64,      // Issued at
}

impl Claims {
    pub fn new(user_id: Uuid, email: String, expiration_hours: i64) -> Self {
        let now = Utc::now();
        let exp = (now + Duration::hours(expiration_hours)).timestamp();

        Self {
            sub: user_id.to_string(),
            email,
            exp,
            iat: now.timestamp(),
        }
    }
}

/// Generate a JWT token for a user
pub fn generate_token(
    user_id: Uuid,
    email: String,
    secret: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let claims = Claims::new(user_id, email, 24); // 24 hour expiration
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )?;
    Ok(token)
}

/// Verify and decode a JWT token
pub fn verify_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )?;
    Ok(token_data.claims)
}
