use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub total_queries: i32,
    pub total_files_processed: i32,
    pub total_saved_queries: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl User {
    /// Fetch all saved queries belonging to this user
    pub async fn saved_queries(&self, pool: &PgPool) -> Result<Vec<SavedQuery>, sqlx::Error> {
        sqlx::query_as::<_, SavedQuery>(
            r#"
            SELECT id, user_id, name, description, query, created_at, updated_at
            FROM saved_queries
            WHERE user_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(self.id)
        .fetch_all(pool)
        .await
    }
}

#[derive(Debug, Deserialize)]
pub struct NewUser {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SavedQuery {
    pub id: Uuid,
    pub user_id: Uuid, // Foreign key - establishes relationship to User
    pub name: String,
    pub description: Option<String>,
    pub query: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SavedQuery {
    /// Fetch the user that owns this saved query
    pub async fn user(&self, pool: &PgPool) -> Result<User, sqlx::Error> {
        sqlx::query_as::<_, User>(
            r#"
            SELECT id, email, password_hash, total_queries, total_files_processed,
                   total_saved_queries, created_at, updated_at
            FROM users
            WHERE id = $1
            "#,
        )
        .bind(self.user_id)
        .fetch_one(pool)
        .await
    }
}

#[derive(Debug, Deserialize)]
pub struct NewSavedQuery {
    pub name: String,
    pub description: Option<String>,
    pub query: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Upload {
    pub id: Uuid,
    pub user_id: Uuid,
    pub file_name: String,
    pub s3_key: String,
    pub file_size: i64,
    pub file_type: String,
    pub table_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
