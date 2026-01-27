use crate::utils::error::DoubledeckerError;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use std::env;
use uuid::Uuid;

pub struct S3Uploader {
    client: S3Client,
    bucket: String,
}

impl S3Uploader {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = S3Client::new(&config);
        let bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "dd-query-csv-bucket".to_string());

        Self { client, bucket }
    }

    /// Upload CSV content to S3 and return the S3 key
    pub async fn upload_csv(&self, content: Vec<u8>) -> Result<String, DoubledeckerError> {
        let key = format!("{}.csv", Uuid::new_v4());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(content))
            .content_type("text/csv")
            .send()
            .await
            .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?;

        Ok(key)
    }

    /// Download CSV from S3 by key
    pub async fn download_csv(&self, key: &str) -> Result<Vec<u8>, DoubledeckerError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?;

        let data = response
            .body
            .collect()
            .await
            .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?;
        Ok(data.into_bytes().to_vec())
    }

    /// Get S3 URI for a key
    pub fn get_s3_uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    /// Generate a presigned URL for downloading a file from S3
    /// Default expiration: 1 hour (3600 seconds)
    pub async fn generate_presigned_url(
        &self,
        key: &str,
        expiration_secs: Option<u64>,
    ) -> Result<String, DoubledeckerError> {
        let expiration = expiration_secs.unwrap_or(3600);

        let presigned_request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(
                aws_sdk_s3::presigning::PresigningConfig::expires_in(
                    std::time::Duration::from_secs(expiration),
                )
                .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?,
            )
            .await
            .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?;

        Ok(presigned_request.uri().to_string())
    }

    /// Delete file from S3 by key
    pub async fn delete_file(&self, key: &str) -> Result<(), DoubledeckerError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| DoubledeckerError::S3Error(e.to_string()))?;

        Ok(())
    }
}
