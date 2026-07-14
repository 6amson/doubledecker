use crate::db::models::PaginatedResponse;

pub fn paginate_rows<T, F>(mut rows: Vec<T>, limit: usize, get_cursor: F) -> PaginatedResponse<T>
where
    F: Fn(&T) -> String,
{
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        rows.last().map(get_cursor)
    } else {
        None
    };

    PaginatedResponse::new(rows, next_cursor, has_more, limit)
}
