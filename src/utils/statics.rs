use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::server::executor::QueryExecutor;

#[derive(Debug, Deserialize)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Contains,
}

#[derive(Debug, Deserialize)]
pub enum AggFunc {
    Sum,
    Avg,
    Max,
    Min,
    Count,
}

#[derive(Debug, Deserialize)]
pub struct Aggregation {
    pub function: AggFunc,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Operations {
    Select {
        columns: Vec<String>,
    },
    Filter {
        column: String,
        operator: FilterOp,
        value: String,
    },
    GroupBy {
        columns: Vec<String>,
        aggregations: Vec<Aggregation>,
    },
    Sort {
        column: String,
        ascending: bool,
    },
    Limit {
        count: usize,
    },
}

#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<QueryExecutor>,
    pub current_table: Arc<RwLock<Option<String>>>,
}

#[derive(Deserialize)]
pub struct QueryRequest {
   pub operations: Vec<Operations>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}
