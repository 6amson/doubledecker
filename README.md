# Doubledecker

A cloud-based CSV query engine built with Rust that enables users to upload CSV files and query them using SQL without traditional database setup.

## Overview

Doubledecker helps small to medium business owners analyze their spreadsheet data using SQL commands. Upload your CSV files and immediately run powerful queries to find insights, generate reports, and make data-driven decisions faster.

## Features

- **Instant CSV Querying**: Upload CSV files and query them immediately via REST API
- **Cloud Storage**: Automatic AWS S3 storage with local fallback
- **Secure Authentication**: JWT-based authentication for multi-tenant security
- **Advanced SQL Operations**: Filtering, grouping, aggregations, transformations, sorting, and pagination
- **Query Management**: Save and reuse frequently used queries
- **Export Results**: Download query results as CSV files

## Tech Stack

- **Language**: Rust (2024 edition)
- **Web Framework**: Axum with async Tokio runtime
- **Query Engine**: Apache DataFusion for high-performance in-memory SQL processing
- **Database**: PostgreSQL (via SQLx) for metadata storage
- **Storage**: AWS S3 for file storage
- **Authentication**: JWT + bcrypt password hashing

## Prerequisites

- Rust 1.75+ (2024 edition)
- PostgreSQL database
- AWS account with S3 access (optional, falls back to local storage)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/6amson/doubledecker.git
cd doubledecker
```

2. Create a `.env` file with the following variables:
```env
DATABASE_URL=postgresql://user:password@localhost/doubledecker
JWT_SECRET=your-secret-key-here
AWS_REGION=us-east-1
S3_BUCKET_NAME=query-bucket-name
```

3. Run database migrations:
```bash
cargo install sqlx-cli
sqlx migrate run
```

4. Build and run:
```bash
cargo build --release
cargo run
```

The server will start on `http://0.0.0.0:3000`

## API Endpoints

### Authentication
- `POST /auth/signup` - Register a new user
- `POST /auth/login` - Login and receive JWT token
- `GET /profile` - Get user profile (requires authentication)

### CSV Operations
- `POST /upload` - Upload a CSV file
- `POST /query` - Execute SQL query on uploaded data
- `POST /query/download` - Download query results as CSV

### Saved Queries
- `POST /saved_queries` - Create a saved query
- `GET /saved_queries` - List all saved queries
- `GET /saved_queries/:id` - Get a specific saved query
- `PUT /saved_queries/:id` - Update a saved query
- `DELETE /saved_queries/:id` - Delete a saved query

### Uploads Management
- `GET /uploads` - List all uploaded files
- `DELETE /uploads/:id` - Delete an uploaded file

## Usage Example

1. **Sign up and login**:
```bash
curl -X POST http://localhost:3000/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"password123","name":"John Doe"}'

curl -X POST http://localhost:3000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"password123"}'
```

2. **Upload a CSV file**:
```bash
curl -X POST http://localhost:3000/upload \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -F "file=@sales_data.csv"
```

3. **Query the data**:
```bash
curl -X POST http://localhost:3000/query \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "sales_data",
    "operations": {
      "filter": "revenue > 1000",
      "group_by": ["region"],
      "select": ["region", "SUM(revenue) as total_revenue"],
      "sort": [{"column": "total_revenue", "direction": "DESC"}],
      "limit": 10
    }
  }'
```

## Query Operations

Doubledecker supports the following SQL-like operations:

- **Filter**: WHERE clause filtering (`"filter": "age > 25 AND city = 'Lagos'"`)
- **GroupBy**: Group results by columns (`"group_by": ["category", "region"]`)
- **Transform**: Create calculated columns (`"transform": [{"name": "profit", "expression": "revenue - cost"}]`)
- **Sort**: Order results (`"sort": [{"column": "revenue", "direction": "DESC"}]`)
- **Select**: Choose specific columns (`"select": ["name", "SUM(revenue) as total"]`)
- **Limit/Offset**: Pagination (`"limit": 20, "offset": 0`)

## Architecture

```
doubledecker/
├── src/
│   ├── main.rs              # Application entry point
│   ├── server/
│   │   ├── auth.rs          # Authentication handlers
│   │   ├── core.rs          # Core API handlers (upload, query, download)
│   │   ├── executor.rs      # DataFusion query execution engine
│   │   ├── saved_queries.rs # Saved queries CRUD
│   │   └── uploads.rs       # Upload management
│   ├── db/
│   │   ├── models.rs        # Database models
│   │   ├── operations.rs    # Database operations
│   │   └── pool.rs          # Connection pool setup
│   └── utils/
│       ├── error.rs         # Custom error types
│       ├── helpers.rs       # Helper functions
│       ├── jwt.rs           # JWT utilities
│       └── s3.rs            # S3 integration
├── migrations/              # Database migrations
└── Cargo.toml              # Dependencies
```

## Development

Run in development mode with auto-reload:
```bash
cargo watch -x run
```

Run tests:
```bash
cargo test
```

## Deployment

The project includes a `Procfile` for AWS deployment and a `dockerfile` for containerized deployments.

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## Author

Built by [6amson](https://github.com/6amson).