use crate::db::models::{Album, Artist, PaginatedResponse, PaginationParams, Track};
use crate::db::queries::{
    create_album, create_artist, create_track, delete_album, delete_artist, delete_track,
    get_albums, get_artists, get_tracks, update_album, update_artist, update_track,
};
use crate::server::dtos::DeleteResponse;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use axum::extract::{Path, Query, State};
use axum::Json;
use crate::server::dtos::catalog::*;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/api/v1/catalog/artists",
    request_body = CreateArtistRequest,
    responses(
        (status = 200, description = "Artist created", body = Artist)
    ),
    tag = "catalog"
)]
pub async fn create_artist_handler(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateArtistRequest>,
) -> Result<Json<Artist>, DoubledeckerError> {
    if payload.name.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Artist name cannot be empty".to_string(),
        ));
    }

    let artist = create_artist(&state.db_pool, auth_user.user_id, payload.name).await?;
    Ok(Json(artist))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/artists",
    params(PaginationParams),
    responses(
        (status = 200, description = "List artists", body = PaginatedArtists)
    ),
    tag = "catalog"
)]
pub async fn list_artists_handler(
    auth_user: AuthenticatedUser,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<Artist>>, DoubledeckerError> {
    let limit = pagination.effective_limit();
    let artists = get_artists(&state.db_pool, auth_user.user_id, pagination.cursor, limit).await?;
    Ok(Json(artists))
}

#[utoipa::path(
    put,
    path = "/api/v1/catalog/artists/{artist_id}",
    params(
        ("artist_id" = Uuid, Path, description = "Artist ID")
    ),
    request_body = UpdateArtistRequest,
    responses(
        (status = 200, description = "Artist updated successfully", body = Artist)
    ),
    tag = "catalog"
)]
pub async fn update_artist_handler(
    _auth_user: AuthenticatedUser,
    Path(artist_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<UpdateArtistRequest>,
) -> Result<Json<Artist>, DoubledeckerError> {
    let updated = update_artist(&state.db_pool, artist_id, &payload.name).await?;
    Ok(Json(updated))
}

#[utoipa::path(
    delete,
    path = "/api/v1/catalog/artists/{artist_id}",
    params(
        ("artist_id" = Uuid, Path, description = "Artist ID")
    ),
    responses(
        (status = 200, description = "Artist deleted successfully", body = DeleteResponse)
    ),
    tag = "catalog"
)]
pub async fn delete_artist_handler(
    _auth_user: AuthenticatedUser,
    Path(artist_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    let _ = delete_artist(&state.db_pool, artist_id).await?;
    Ok(Json(DeleteResponse {
        message: "Artist deleted successfully".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/catalog/albums",
    request_body = CreateAlbumRequest,
    responses(
        (status = 200, description = "Album created", body = Album)
    ),
    tag = "catalog"
)]
pub async fn create_album_handler(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateAlbumRequest>,
) -> Result<Json<Album>, DoubledeckerError> {
    if payload.title.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Album title cannot be empty".to_string(),
        ));
    }

    let album = create_album(
        &state.db_pool,
        auth_user.user_id,
        payload.artist_id,
        payload.title,
        payload.upc,
        payload.release_date,
    )
    .await?;
    Ok(Json(album))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/albums",
    params(PaginationParams),
    responses(
        (status = 200, description = "List albums", body = PaginatedAlbums)
    ),
    tag = "catalog"
)]
pub async fn list_albums_handler(
    auth_user: AuthenticatedUser,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<Album>>, DoubledeckerError> {
    let limit = pagination.effective_limit();
    let albums = get_albums(&state.db_pool, auth_user.user_id, pagination.cursor, limit).await?;
    Ok(Json(albums))
}

#[utoipa::path(
    put,
    path = "/api/v1/catalog/albums/{album_id}",
    params(
        ("album_id" = Uuid, Path, description = "Album ID")
    ),
    request_body = UpdateAlbumRequest,
    responses(
        (status = 200, description = "Album updated successfully", body = Album)
    ),
    tag = "catalog"
)]
pub async fn update_album_handler(
    _auth_user: AuthenticatedUser,
    Path(album_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<UpdateAlbumRequest>,
) -> Result<Json<Album>, DoubledeckerError> {
    let updated = update_album(
        &state.db_pool,
        album_id,
        payload.title.as_deref(),
        payload.upc.as_deref(),
        payload.release_date,
    )
    .await?;
    Ok(Json(updated))
}

#[utoipa::path(
    delete,
    path = "/api/v1/catalog/albums/{album_id}",
    params(
        ("album_id" = Uuid, Path, description = "Album ID")
    ),
    responses(
        (status = 200, description = "Album deleted successfully", body = DeleteResponse)
    ),
    tag = "catalog"
)]
pub async fn delete_album_handler(
    _auth_user: AuthenticatedUser,
    Path(album_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    let _ = delete_album(&state.db_pool, album_id).await?;
    Ok(Json(DeleteResponse {
        message: "Album deleted successfully".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/catalog/tracks",
    request_body = CreateTrackRequest,
    responses(
        (status = 200, description = "Track created", body = Track)
    ),
    tag = "catalog"
)]
pub async fn create_track_handler(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateTrackRequest>,
) -> Result<Json<Track>, DoubledeckerError> {
    if payload.title.trim().is_empty() || payload.isrc.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Track title and ISRC cannot be empty".to_string(),
        ));
    }

    let track = create_track(
        &state.db_pool,
        auth_user.user_id,
        payload.artist_id,
        payload.album_id,
        payload.isrc,
        payload.title,
    )
    .await?;
    Ok(Json(track))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/tracks",
    params(PaginationParams),
    responses(
        (status = 200, description = "List tracks", body = PaginatedTracks)
    ),
    tag = "catalog"
)]
pub async fn list_tracks_handler(
    auth_user: AuthenticatedUser,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<Track>>, DoubledeckerError> {
    let limit = pagination.effective_limit();
    let tracks = get_tracks(&state.db_pool, auth_user.user_id, pagination.cursor, limit).await?;
    Ok(Json(tracks))
}

#[utoipa::path(
    put,
    path = "/api/v1/catalog/tracks/{track_id}",
    params(
        ("track_id" = Uuid, Path, description = "Track ID")
    ),
    request_body = UpdateTrackRequest,
    responses(
        (status = 200, description = "Track updated successfully", body = Track)
    ),
    tag = "catalog"
)]
pub async fn update_track_handler(
    _auth_user: AuthenticatedUser,
    Path(track_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<UpdateTrackRequest>,
) -> Result<Json<Track>, DoubledeckerError> {
    let updated = update_track(
        &state.db_pool,
        track_id,
        payload.title.as_deref(),
        payload.isrc.as_deref(),
    )
    .await?;
    Ok(Json(updated))
}

#[utoipa::path(
    delete,
    path = "/api/v1/catalog/tracks/{track_id}",
    params(
        ("track_id" = Uuid, Path, description = "Track ID")
    ),
    responses(
        (status = 200, description = "Track deleted successfully", body = DeleteResponse)
    ),
    tag = "catalog"
)]
pub async fn delete_track_handler(
    _auth_user: AuthenticatedUser,
    Path(track_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    let _ = delete_track(&state.db_pool, track_id).await?;
    Ok(Json(DeleteResponse {
        message: "Track deleted successfully".to_string(),
    }))
}
