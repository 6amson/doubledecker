use axum::{
    Json,
    extract::multipart::MultipartError,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub enum DoubledeckerError {
    // File upload errors
    FileUpload(String),
    MultipartError(String),
    InvalidFilePath,
    S3Error(String),

    // DataFusion/DataFrame errors
    DataFusionError(String),
    ColumnNotFound(String),
    TableNotFound(String),

    // Query errors
    QueryExecution(String),
    InvalidQuery(String),

    // Database errors
    DatabaseError(String),
    AuthenticationError(String),
    NotFound(String),
    Unauthorized,

    // General errors
    Internal(String),
    BadRequest(String),
}

impl DoubledeckerError {
    /// Get the HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            DoubledeckerError::FileUpload(_) => StatusCode::BAD_REQUEST,
            DoubledeckerError::MultipartError(_) => StatusCode::BAD_REQUEST,
            DoubledeckerError::InvalidFilePath => StatusCode::BAD_REQUEST,
            DoubledeckerError::S3Error(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DoubledeckerError::DataFusionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DoubledeckerError::ColumnNotFound(_) => StatusCode::NOT_FOUND,
            DoubledeckerError::TableNotFound(_) => StatusCode::NOT_FOUND,
            DoubledeckerError::QueryExecution(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DoubledeckerError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            DoubledeckerError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DoubledeckerError::AuthenticationError(_) => StatusCode::UNAUTHORIZED,
            DoubledeckerError::NotFound(_) => StatusCode::NOT_FOUND,
            DoubledeckerError::Unauthorized => StatusCode::UNAUTHORIZED,
            DoubledeckerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DoubledeckerError::BadRequest(_) => StatusCode::BAD_REQUEST,
        }
    }

    /// Get the error message
    pub fn message(&self) -> String {
        match self {
            DoubledeckerError::FileUpload(msg) => format!("File upload error: {}", msg),
            DoubledeckerError::InvalidFilePath => "Invalid file path".to_string(),
            DoubledeckerError::S3Error(msg) => format!("S3 error: {}", msg),
            DoubledeckerError::DataFusionError(msg) => format!("DataFrame error: {}", msg),
            DoubledeckerError::ColumnNotFound(col) => format!("Column not found: {}", col),
            DoubledeckerError::TableNotFound(table) => format!("Table not found: {}", table),
            DoubledeckerError::QueryExecution(msg) => format!("Query execution error: {}", msg),
            DoubledeckerError::InvalidQuery(msg) => format!("Invalid query: {}", msg),
            DoubledeckerError::DatabaseError(msg) => format!("Database error: {}", msg),
            DoubledeckerError::AuthenticationError(msg) => format!("Authentication error: {}", msg),
            DoubledeckerError::NotFound(msg) => format!("Not found: {}", msg),
            DoubledeckerError::Unauthorized => "Unauthorized".to_string(),
            DoubledeckerError::Internal(msg) => format!("Internal error: {}", msg),
            DoubledeckerError::BadRequest(msg) => format!("Bad request: {}", msg),
            DoubledeckerError::MultipartError(msg) => format!("Multipart error: {}", msg),
        }
    }
}

// Implement IntoResponse so Axum can convert errors to HTTP responses
impl IntoResponse for DoubledeckerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let message = self.message();

        let body = Json(json!({
            "error": message,
            "status": status.as_u16(),
        }));

        (status, body).into_response()
    }
}

// Convert from datafusion::error::DataFusionError
impl From<datafusion::error::DataFusionError> for DoubledeckerError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        DoubledeckerError::DataFusionError(err.to_string())
    }
}

// Convert from std::io::Error
impl From<std::io::Error> for DoubledeckerError {
    fn from(err: std::io::Error) -> Self {
        DoubledeckerError::Internal(err.to_string())
    }
}

// Convert from String (for simple error messages)
impl From<String> for DoubledeckerError {
    fn from(err: String) -> Self {
        DoubledeckerError::Internal(err)
    }
}

impl From<MultipartError> for DoubledeckerError {
    fn from(err: MultipartError) -> Self {
        DoubledeckerError::FileUpload(err.to_string())
    }
}

// Implement Display for better error messages
impl std::fmt::Display for DoubledeckerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

// Implement std::error::Error trait
impl std::error::Error for DoubledeckerError {}
