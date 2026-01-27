#![allow(dead_code)]

use crate::{
    db::pool::{init_pool, run_migrations},
    server::{
        auth::{get_profile, login, signup},
        core::{download_query_csv, execute_query, upload_csv},
        saved_queries::{
            create_saved_query_handler, delete_saved_query_handler, get_saved_query_handler,
            list_saved_queries_handler, update_saved_query_handler,
        },
        uploads::{delete_upload_handler, list_uploads_handler},
    },
    utils::statics::AppState,
};
use axum::http::{Method, header};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{delete, get, post, put},
};
use tokio::net::TcpListener;

use tower_http::cors::CorsLayer;

mod db;
mod server;
mod utils;

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

    let state = AppState { db_pool };

    let app = Router::new()
        // Authentication routes
        .route("/auth/signup", post(signup))
        .route("/auth/login", post(login))
        .route("/profile", get(get_profile))
        // Saved queries routes
        .route("/saved_queries", post(create_saved_query_handler))
        .route("/saved_queries", get(list_saved_queries_handler))
        .route("/saved_queries/:id", get(get_saved_query_handler))
        .route("/saved_queries/:id", put(update_saved_query_handler))
        .route("/saved_queries/:id", delete(delete_saved_query_handler))
        // Uploads routes
        .route("/uploads", get(list_uploads_handler))
        .route("/uploads/:id", delete(delete_upload_handler))
        // CSV and query routes
        .route("/upload", post(upload_csv))
        .route("/query", post(execute_query))
        .route("/query/download", post(download_query_csv))
        .route("/", get(|| async { "Hello from doubledecker angels." }))
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
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024)) // 50 MB limit
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    eprintln!("✓ Server listening on http://0.0.0.0:3000");
    eprintln!("  Access from Windows: http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}
