use crate::db::models::{SavedQuery, Upload, User};
use crate::utils::error::DoubledeckerError;
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

// ============================================================================
// User Operations
// ============================================================================

pub async fn create_user(
    pool: &PgPool,
    email: String,
    password: String,
) -> Result<User, DoubledeckerError> {
    // Hash the password
    let password_hash = hash(password, DEFAULT_COST)
        .map_err(|e| DoubledeckerError::Internal(format!("Password hashing failed: {}", e)))?;

    let user = sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users (email, password_hash)
        VALUES ($1, $2)
        RETURNING id, email, password_hash, total_queries, total_files_processed, 
                  total_saved_queries, created_at, updated_at
        "#,
    )
    .bind(&email)
    .bind(&password_hash)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError("Email already exists".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub async fn get_user_by_email(pool: &PgPool, email: &str) -> Result<User, DoubledeckerError> {
    let user = sqlx::query_as::<_, User>(
        r#"
        SELECT id, email, password_hash, total_queries, total_files_processed,
               total_saved_queries, created_at, updated_at
        FROM users
        WHERE email = $1
        "#,
    )
    .bind(email)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("User not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub async fn get_user_by_id(pool: &PgPool, id: Uuid) -> Result<User, DoubledeckerError> {
    let user = sqlx::query_as::<_, User>(
        r#"
        SELECT id, email, password_hash, total_queries, total_files_processed,
               total_saved_queries, created_at, updated_at
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("User not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, DoubledeckerError> {
    verify(password, hash)
        .map_err(|e| DoubledeckerError::Internal(format!("Password verification failed: {}", e)))
}

pub async fn increment_query_count(pool: &PgPool, user_id: Uuid) -> Result<(), DoubledeckerError> {
    sqlx::query(
        r#"
        UPDATE users
        SET total_queries = total_queries + 1,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(())
}

pub async fn increment_file_count(pool: &PgPool, user_id: Uuid) -> Result<(), DoubledeckerError> {
    sqlx::query(
        r#"
        UPDATE users
        SET total_files_processed = total_files_processed + 1,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(())
}

// ============================================================================
// Saved Query Operations
// ============================================================================

pub async fn create_saved_query(
    pool: &PgPool,
    user_id: Uuid,
    name: String,
    description: Option<String>,
    query: serde_json::Value,
) -> Result<SavedQuery, DoubledeckerError> {
    let saved_query = sqlx::query_as::<_, SavedQuery>(
        r#"
        INSERT INTO saved_queries (user_id, name, description, query)
        VALUES ($1, $2, $3, $4)
        RETURNING id, user_id, name, description, query, created_at, updated_at
        "#,
    )
    .bind(user_id)
    .bind(&name)
    .bind(&description)
    .bind(&query)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError(
                "A saved query with this name already exists".to_string(),
            )
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    // Increment the user's total_saved_queries count
    sqlx::query(
        r#"
        UPDATE users
        SET total_saved_queries = total_saved_queries + 1,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(saved_query)
}

pub async fn get_saved_queries_by_user(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<SavedQuery>, DoubledeckerError> {
    let queries = sqlx::query_as::<_, SavedQuery>(
        r#"
        SELECT id, user_id, name, description, query, created_at, updated_at
        FROM saved_queries
        WHERE user_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(queries)
}

pub async fn get_saved_query(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
) -> Result<SavedQuery, DoubledeckerError> {
    let query = sqlx::query_as::<_, SavedQuery>(
        r#"
        SELECT id, user_id, name, description, query, created_at, updated_at
        FROM saved_queries
        WHERE id = $1 AND user_id = $2
        "#,
    )
    .bind(id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            DoubledeckerError::NotFound("Saved query not found".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(query)
}

pub async fn update_saved_query(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
    name: String,
    description: Option<String>,
    query: serde_json::Value,
) -> Result<SavedQuery, DoubledeckerError> {
    let updated_query = sqlx::query_as::<_, SavedQuery>(
        r#"
        UPDATE saved_queries
        SET name = $3,
            description = $4,
            query = $5,
            updated_at = $6
        WHERE id = $1 AND user_id = $2
        RETURNING id, user_id, name, description, query, created_at, updated_at
        "#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&name)
    .bind(&description)
    .bind(&query)
    .bind(Utc::now())
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            DoubledeckerError::NotFound("Saved query not found".to_string())
        }
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError(
                "A saved query with this name already exists".to_string(),
            )
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(updated_query)
}

pub async fn delete_saved_query(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
) -> Result<(), DoubledeckerError> {
    let result = sqlx::query(
        r#"
        DELETE FROM saved_queries
        WHERE id = $1 AND user_id = $2
        "#,
    )
    .bind(id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(DoubledeckerError::NotFound(
            "Saved query not found".to_string(),
        ));
    }

    // Decrement the user's total_saved_queries count
    sqlx::query(
        r#"
        UPDATE users
        SET total_saved_queries = total_saved_queries - 1,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(())
}

// ============================================================================
// Upload Operations
// ============================================================================

pub async fn create_upload(
    pool: &PgPool,
    user_id: Uuid,
    file_name: String,
    s3_key: String,
    file_size: i64,
    file_type: String,
    table_name: String,
) -> Result<Upload, DoubledeckerError> {
    let upload = sqlx::query_as::<_, Upload>(
        r#"
        INSERT INTO uploads (user_id, file_name, s3_key, file_size, file_type, table_name)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, user_id, file_name, s3_key, file_size, file_type, table_name, created_at, updated_at
        "#,
    )
    .bind(user_id)
    .bind(&file_name)
    .bind(&s3_key)
    .bind(file_size)
    .bind(&file_type)
    .bind(&table_name)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError(
                "A file with this table name already exists for this user".to_string(),
            )
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(upload)
}

pub async fn get_uploads_by_user(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<crate::db::models::Upload>, DoubledeckerError> {
    let uploads = sqlx::query_as::<_, crate::db::models::Upload>(
        r#"
        SELECT id, user_id, file_name, s3_key, file_size, file_type, table_name, created_at, updated_at
        FROM uploads
        WHERE user_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(uploads)
}

pub async fn get_uploads_by_user_paginated(
    pool: &PgPool,
    user_id: Uuid,
    page: i64,
    page_size: i64,
) -> Result<(Vec<crate::db::models::Upload>, i64), DoubledeckerError> {
    // Get total count
    let total: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) as count
        FROM uploads
        WHERE user_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    // Calculate offset
    let offset = (page - 1) * page_size;

    // Get paginated uploads
    let uploads = sqlx::query_as::<_, crate::db::models::Upload>(
        r#"
        SELECT id, user_id, file_name, s3_key, file_size, file_type, table_name, created_at, updated_at
        FROM uploads
        WHERE user_id = $1
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(user_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok((uploads, total.0))
}

pub async fn get_upload_by_table_name(
    pool: &PgPool,
    table_name: &str,
    user_id: Uuid,
) -> Result<crate::db::models::Upload, DoubledeckerError> {
    let upload = sqlx::query_as::<_, crate::db::models::Upload>(
        r#"
        SELECT id, user_id, file_name, s3_key, file_size, file_type, table_name, created_at, updated_at
        FROM uploads
        WHERE table_name = $1 AND user_id = $2
        "#,
    )
    .bind(table_name)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            DoubledeckerError::NotFound("Upload not found or access denied".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(upload)
}

pub async fn delete_upload(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
) -> Result<(), DoubledeckerError> {
    let result = sqlx::query(
        r#"
        DELETE FROM uploads
        WHERE id = $1 AND user_id = $2
        "#,
    )
    .bind(id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(DoubledeckerError::NotFound("Upload not found".to_string()));
    }

    Ok(())
}
