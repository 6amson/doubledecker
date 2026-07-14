#![allow(dead_code)]

use crate::{
    db::pool::{init_pool, run_migrations},
    engine::EngineProvider,
    server::{
        analytics::{
            download_query_csv_handler, download_query_history_csv_handler, execute_query_handler,
            get_analytics_summary_handler, get_query_history_handler,
        },
        auth::{get_profile, login, signup},
        catalog::{
            create_album_handler, create_artist_handler, create_track_handler,
            delete_album_handler, delete_artist_handler, delete_track_handler,
            list_albums_handler, list_artists_handler, list_tracks_handler,
            update_album_handler, update_artist_handler, update_track_handler,
        },
        openapi::ApiDoc,
        payees::{
            create_payee_handler, delete_payee_handler, list_payees_handler, update_payee_handler,
        },
        splits::{
            create_split_handler, delete_split_handler, list_splits_handler, update_split_handler,
        },
        
        uploads::{
            confirm_upload_handler, generate_presigned_url_handler, list_datasets_handler,
            upload_dataset_direct,
        },
        workspaces::{
            add_workspace_member_handler, create_workspace_handler, delete_workspace_handler,
            list_workspace_members_handler, list_workspaces_handler, update_workspace_handler,
        },
    },
    utils::s3::S3Uploader,
    server::state::AppState,
    workers::register_ingestion_workflow,
};
use axum::http::{Method, header};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post, put},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod db;
mod engine;
mod normalization;
mod server;
mod utils;
mod workers;

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Initialize database connection pool
    let db_pool = init_pool().await.expect("Failed to create database pool");

    // Run database migrations
    run_migrations(&db_pool)
        .await
        .expect("Failed to run database migrations");

    eprintln!("✓ Database connected and migrations completed");

    // Initialize uploader, engine provider, and inngest client
    let uploader = Arc::new(S3Uploader::new().await);
    let engine = Arc::new(EngineProvider::new(db_pool.clone()));
    let inngest_client = Arc::new(inngest::client::Inngest::new("doubledecker"));

    // Register Inngest background workflow handler
    let mut inngest_handler = inngest::handler::Handler::new(&inngest_client);
    inngest_handler.register_fn(
        register_ingestion_workflow(&inngest_client, db_pool.clone(), uploader.clone()),
    );
    let inngest_state = Arc::new(inngest_handler);

    let inngest_router = Router::new()
        .route(
            "/api/inngest",
            get(inngest::serve::axum::introspect)
                .put(inngest::serve::axum::register)
                .post(inngest::serve::axum::invoke),
        )
        .with_state(inngest_state);

    let state = AppState {
        db_pool,
        engine,
        uploader,
        inngest_client,
    };

    let app = Router::new()
        // Authentication routes
        .route("/auth/signup", post(signup))
        .route("/auth/login", post(login))
        .route("/profile", get(get_profile))
        // Workspace routes
        .route("/api/workspaces", post(create_workspace_handler).get(list_workspaces_handler))
        .route(
            "/api/workspaces/:workspace_id",
            put(update_workspace_handler).delete(delete_workspace_handler),
        )
        .route(
            "/api/workspaces/:workspace_id/members",
            post(add_workspace_member_handler).get(list_workspace_members_handler),
        )
        // Global User Master Catalog routes (no workspace required)
        .route("/api/v1/catalog/artists", post(create_artist_handler).get(list_artists_handler))
        .route(
            "/api/v1/catalog/artists/:artist_id",
            put(update_artist_handler).delete(delete_artist_handler),
        )
        .route("/api/v1/catalog/albums", post(create_album_handler).get(list_albums_handler))
        .route(
            "/api/v1/catalog/albums/:album_id",
            put(update_album_handler).delete(delete_album_handler),
        )
        .route("/api/v1/catalog/tracks", post(create_track_handler).get(list_tracks_handler))
        .route(
            "/api/v1/catalog/tracks/:track_id",
            put(update_track_handler).delete(delete_track_handler),
        )
        // Global Payee Contact Book routes
        .route("/api/v1/payees", post(create_payee_handler).get(list_payees_handler))
        .route(
            "/api/v1/payees/:payee_id",
            put(update_payee_handler).delete(delete_payee_handler),
        )
        // Cascading Splits routes
        .route("/api/workspaces/:workspace_id/splits", post(create_split_handler).get(list_splits_handler))
        .route(
            "/api/workspaces/:workspace_id/splits/:split_id",
            put(update_split_handler).delete(delete_split_handler),
        )
        // Dataset Ingestion routes
        .route("/api/workspaces/:workspace_id/datasets", get(list_datasets_handler))
        .route("/api/workspaces/:workspace_id/datasets/upload", post(upload_dataset_direct))
        .route("/api/workspaces/:workspace_id/datasets/presigned_url", post(generate_presigned_url_handler))
        .route("/api/workspaces/:workspace_id/datasets/confirm", post(confirm_upload_handler))
        // Analytical Engine & Royalty Analytics routes
        .route("/api/workspaces/:workspace_id/analytics/query", post(execute_query_handler))
        .route("/api/workspaces/:workspace_id/analytics/download", post(download_query_csv_handler))
        .route("/api/workspaces/:workspace_id/analytics/summary", get(get_analytics_summary_handler))
        .route("/api/workspaces/:workspace_id/analytics/history", get(get_query_history_handler))
        .route("/api/workspaces/:workspace_id/analytics/history/:query_id/download", get(download_query_history_csv_handler))
        .route("/", get(|| async { "Hello from doubledecker angels." }))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(inngest_router)
        .layer(
            CorsLayer::new()
                .allow_origin([
                    "http://localhost:8080"
                        .parse::<header::HeaderValue>()
                        .unwrap(),
                    "https://doubledecker.vercel.app"
                        .parse::<header::HeaderValue>()
                        .unwrap(),
                ])
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]),
        )
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024)) // 50 MB limit for Path A
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    eprintln!("✓ Server listening on http://0.0.0.0:3000");
    eprintln!("  Access from Windows: http://localhost:3000");
    eprintln!("  Swagger UI available at: http://localhost:3000/swagger-ui");
    axum::serve(listener, app).await.unwrap();
}
