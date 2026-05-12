#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}
