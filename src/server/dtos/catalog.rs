use chrono::NaiveDate;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateArtistRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateArtistRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateAlbumRequest {
    pub artist_id: Uuid,
    pub title: String,
    pub upc: Option<String>,
    pub release_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateAlbumRequest {
    pub title: Option<String>,
    pub upc: Option<String>,
    pub release_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateTrackRequest {
    pub artist_id: Uuid,
    pub album_id: Option<Uuid>,
    pub isrc: String,
    pub title: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateTrackRequest {
    pub title: Option<String>,
    pub isrc: Option<String>,
}
