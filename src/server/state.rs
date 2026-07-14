use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub db_pool: PgPool,
    pub engine: Arc<crate::engine::EngineProvider>,
    pub uploader: Arc<crate::utils::s3::S3Uploader>,
    pub inngest_client: Arc<inngest::client::Inngest>,
}
