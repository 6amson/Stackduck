use std::time::Duration;

use crate::{
    error::StackDuckError,
    types::{Job, JobStatus},
};

use sqlx::{
    pool, postgres::{PgPoolOptions, Postgres},
    Pool,
};

pub type DbPool = Pool<Postgres>;

pub async fn connect_to_db(database_url: &str) -> Result<DbPool, StackDuckError> {
 let pool = PgPoolOptions::new()
 .max_connections(10)
 .acquire_timeout(Duration::from_secs(5))
 .connect(database_url)
 .await
 .map_err(|e| StackDuckError::DbConnectionError(e.to_string()))?;

 sqlx::migrate!("./migrations").run(&pool).await
 .map_err(|e| StackDuckError::MigrationError(e.to_string()))?;
 Ok(pool)
}


