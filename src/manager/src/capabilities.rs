use axum::{Json, response::IntoResponse};

pub async fn get_syntax_capabilities_handler() -> impl IntoResponse {
    Json(parser::syntax_capabilities_owned())
}
