use axum::routing::{get, post, Router};
use std::{fs, net::SocketAddr, sync::Arc};

mod db;
mod options;
mod routes;
mod cache;

use db::DatabaseImpl;

use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_str =
        fs::read_to_string("imoog.config.toml").expect("Failed to read imoog.config.toml file.");
    let config: options::ImoogOptions = toml::from_str(&config_str).unwrap();

    let db = db::DatabaseDriver::connect(config.database).await;
    println!("connected to the database");
    info!("Connected to database");
    let shared_db = Arc::new(db);

    let deliver_closure = move |p| {
        let database = Arc::clone(&shared_db);

        routes::deliver_file(database, p)
    };

    let router = Router::new()
        .route("/", get(routes::index))
        .route("/image/:identifier", get(deliver_closure));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));

    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .unwrap();
}
