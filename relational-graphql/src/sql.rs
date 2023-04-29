//! Relational GraphQL backend based on a SQL database (specifically PostgreSQL).

pub mod data_source;
pub mod db;
mod ops;

pub use data_source::*;
