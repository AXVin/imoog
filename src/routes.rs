use axum::body::Body;
use axum::extract::Path;
use axum::http::{Response, StatusCode};

use std::sync::Arc;

use crate::{
    db::{DatabaseDriver, DatabaseImpl, MongoImoogDocument},
    options::MongoOptions,
};
use mongodb::Collection;

pub async fn index() -> &'static str {
    "Welcome to imoog"
}

type DBDriver = DatabaseDriver<MongoOptions, Collection<MongoImoogDocument>>;

pub async fn deliver_file(
    database: Arc<DBDriver>,
    Path(identifier): Path<String>,
) -> Response<Body> {
    let result = database.fetch(identifier).await;

    let resp;

    if let Some(r) = result {
        let (_, image_data, mime) = r;

        let decompressed = inflate::inflate_bytes_zlib(image_data.as_slice()).unwrap();

        resp = Response::builder()
            .header("content-type", &mime)
            .body(Body::from(decompressed))
            .unwrap();
    } else {
        resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    resp
}
