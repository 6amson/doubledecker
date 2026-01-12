#![allow(dead_code)]

use crate::{
    server::{
        core::{describe_table, execute_query, upload_csv},
        executor::QueryExecutor,
    },
    utils::statics::AppState,
};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

mod server;
mod utils;

#[tokio::main]
async fn main() {
    let executor = Arc::new(QueryExecutor::new());
    let state = AppState {
        executor,
        current_table: Arc::new(RwLock::new(None)),
    };

    let app = Router::new()
        .route("/upload", post(upload_csv))
        .route("/query", post(execute_query))
        .route("/describe_table", get(describe_table))
        .route("/", get(|| async { "Hello from doubledecker angels." }))
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024)) // 50 MB limit
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    eprintln!("Server listening on http://0.0.0.0:3000");
    eprintln!("Access from Windows: http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}
