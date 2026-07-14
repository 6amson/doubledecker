use crate::db::models::{Album, Artist, PaginatedResponse, Track};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use chrono::NaiveDate;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_artist(
    pool: &PgPool,
    owner_user_id: Uuid,
    name: String,
) -> Result<Artist, DoubledeckerError> {
    let artist = sqlx::query_as::<_, Artist>(
        r#"
        INSERT INTO artists (owner_user_id, name)
        VALUES ($1, $2)
        RETURNING id, owner_user_id, name, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(&name)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError("Artist already exists in your master catalog".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(artist)
}

pub async fn upsert_artist(
    pool: &PgPool,
    owner_user_id: Uuid,
    name: String,
) -> Result<Artist, DoubledeckerError> {
    let artist = sqlx::query_as::<_, Artist>(
        r#"
        INSERT INTO artists (owner_user_id, name)
        VALUES ($1, $2)
        ON CONFLICT (owner_user_id, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id, owner_user_id, name, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(&name)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(artist)
}

pub async fn get_artists(
    pool: &PgPool,
    owner_user_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Artist>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Artist>(
        r#"
        SELECT id, owner_user_id, name, created_at
        FROM artists
        WHERE owner_user_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM artists WHERE id = $2))
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(owner_user_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn update_artist(
    pool: &PgPool,
    artist_id: Uuid,
    name: &str,
) -> Result<Artist, DoubledeckerError> {
    let artist = sqlx::query_as::<_, Artist>(
        r#"
        UPDATE artists
        SET name = $2
        WHERE id = $1
        RETURNING id, owner_user_id, name, created_at
        "#,
    )
    .bind(artist_id)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    artist.ok_or_else(|| DoubledeckerError::NotFound("Artist not found".to_string()))
}

pub async fn delete_artist(
    pool: &PgPool,
    artist_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let res = sqlx::query(
        r#"
        DELETE FROM artists
        WHERE id = $1
        "#,
    )
    .bind(artist_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(res.rows_affected())
}

pub async fn create_album(
    pool: &PgPool,
    owner_user_id: Uuid,
    artist_id: Uuid,
    title: String,
    upc: Option<String>,
    release_date: Option<NaiveDate>,
) -> Result<Album, DoubledeckerError> {
    let album = sqlx::query_as::<_, Album>(
        r#"
        INSERT INTO albums (owner_user_id, artist_id, title, upc, release_date)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, owner_user_id, artist_id, title, upc, release_date, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(artist_id)
    .bind(&title)
    .bind(upc)
    .bind(release_date)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(album)
}

pub async fn upsert_album(
    pool: &PgPool,
    owner_user_id: Uuid,
    artist_id: Uuid,
    title: String,
    upc: Option<String>,
    release_date: Option<NaiveDate>,
) -> Result<Album, DoubledeckerError> {
    if let Some(ref upc_val) = upc {
        if !upc_val.trim().is_empty() {
            let album = sqlx::query_as::<_, Album>(
                r#"
                INSERT INTO albums (owner_user_id, artist_id, title, upc, release_date)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (owner_user_id, upc) DO UPDATE SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id
                RETURNING id, owner_user_id, artist_id, title, upc, release_date, created_at
                "#,
            )
            .bind(owner_user_id)
            .bind(artist_id)
            .bind(&title)
            .bind(upc)
            .bind(release_date)
            .fetch_one(pool)
            .await
            .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;
            return Ok(album);
        }
    }

    if let Ok(existing) = sqlx::query_as::<_, Album>(
        r#"
        SELECT id, owner_user_id, artist_id, title, upc, release_date, created_at
        FROM albums
        WHERE owner_user_id = $1 AND artist_id = $2 AND LOWER(title) = LOWER($3)
        LIMIT 1
        "#,
    )
    .bind(owner_user_id)
    .bind(artist_id)
    .bind(&title)
    .fetch_one(pool)
    .await {
        Ok(existing)
    } else {
        create_album(pool, owner_user_id, artist_id, title, upc, release_date).await
    }
}

pub async fn get_albums(
    pool: &PgPool,
    owner_user_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Album>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Album>(
        r#"
        SELECT id, owner_user_id, artist_id, title, upc, release_date, created_at
        FROM albums
        WHERE owner_user_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM albums WHERE id = $2))
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(owner_user_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn update_album(
    pool: &PgPool,
    album_id: Uuid,
    title: Option<&str>,
    upc: Option<&str>,
    release_date: Option<NaiveDate>,
) -> Result<Album, DoubledeckerError> {
    let album = sqlx::query_as::<_, Album>(
        r#"
        UPDATE albums
        SET title = COALESCE($2, title),
            upc = COALESCE($3, upc),
            release_date = COALESCE($4, release_date)
        WHERE id = $1
        RETURNING id, owner_user_id, artist_id, title, upc, release_date, created_at
        "#,
    )
    .bind(album_id)
    .bind(title)
    .bind(upc)
    .bind(release_date)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    album.ok_or_else(|| DoubledeckerError::NotFound("Album not found".to_string()))
}

pub async fn delete_album(
    pool: &PgPool,
    album_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let res = sqlx::query(
        r#"
        DELETE FROM albums
        WHERE id = $1
        "#,
    )
    .bind(album_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(res.rows_affected())
}

pub async fn create_track(
    pool: &PgPool,
    owner_user_id: Uuid,
    artist_id: Uuid,
    album_id: Option<Uuid>,
    isrc: String,
    title: String,
) -> Result<Track, DoubledeckerError> {
    let track = sqlx::query_as::<_, Track>(
        r#"
        INSERT INTO tracks (owner_user_id, artist_id, album_id, isrc, title)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, owner_user_id, artist_id, album_id, isrc, title, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(artist_id)
    .bind(album_id)
    .bind(&isrc)
    .bind(&title)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError("ISRC already exists in your catalog".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(track)
}

pub async fn upsert_track(
    pool: &PgPool,
    owner_user_id: Uuid,
    artist_id: Uuid,
    album_id: Option<Uuid>,
    isrc: String,
    title: String,
) -> Result<Track, DoubledeckerError> {
    let track = sqlx::query_as::<_, Track>(
        r#"
        INSERT INTO tracks (owner_user_id, artist_id, album_id, isrc, title)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_user_id, isrc) DO UPDATE SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id, album_id = COALESCE(EXCLUDED.album_id, tracks.album_id)
        RETURNING id, owner_user_id, artist_id, album_id, isrc, title, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(artist_id)
    .bind(album_id)
    .bind(&isrc)
    .bind(&title)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(track)
}

pub async fn get_tracks(
    pool: &PgPool,
    owner_user_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Track>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Track>(
        r#"
        SELECT id, owner_user_id, artist_id, album_id, isrc, title, created_at
        FROM tracks
        WHERE owner_user_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM tracks WHERE id = $2))
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(owner_user_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn update_track(
    pool: &PgPool,
    track_id: Uuid,
    title: Option<&str>,
    isrc: Option<&str>,
) -> Result<Track, DoubledeckerError> {
    let track = sqlx::query_as::<_, Track>(
        r#"
        UPDATE tracks
        SET title = COALESCE($2, title),
            isrc = COALESCE($3, isrc)
        WHERE id = $1
        RETURNING id, owner_user_id, artist_id, album_id, isrc, title, created_at
        "#,
    )
    .bind(track_id)
    .bind(title)
    .bind(isrc)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    track.ok_or_else(|| DoubledeckerError::NotFound("Track not found".to_string()))
}

pub async fn delete_track(
    pool: &PgPool,
    track_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let res = sqlx::query(
        r#"
        DELETE FROM tracks
        WHERE id = $1
        "#,
    )
    .bind(track_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(res.rows_affected())
}
