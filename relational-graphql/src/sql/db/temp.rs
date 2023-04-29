//! A temporary database which is dropped on [`Drop`].

use super::{Connection, Error, SchemaColumn, SelectColumn};
use crate::{Array, Length};
use async_trait::async_trait;
use rand::RngCore;
use std::borrow::Cow;

/// A [`Connection`] implemented using a temporary databse in a SQL database cluster.
///
/// The temporary database will be droppe when this struct is dropped.
#[derive(Clone, Debug)]
pub struct TempDatabase<Db: Connection> {
    db: Db,
    name: String,
}

impl<Db: Connection> TempDatabase<Db> {
    /// Create a temporary database with a random name.
    pub async fn new(mut db: Db) -> Result<Self, Db::Error> {
        let name = format!("tempdb{}", rand::thread_rng().next_u64());
        db.create_db(&name).await?;
        Ok(Self { db, name })
    }
}

impl<Db: Connection> Drop for TempDatabase<Db> {
    fn drop(&mut self) {
        if let Err(err) = async_std::task::block_on(self.db.drop_db(&self.name)) {
            tracing::error!("error dropping temp DB {}: {err}", self.name);
        }
    }
}

#[async_trait]
impl<Db: Connection + Send + Sync> Connection for TempDatabase<Db> {
    type Error = Db::Error;

    type CreateTable<'a, N: Length> = Db::CreateTable<'a, N>
    where
        Self: 'a;

    type AlterTable<'a> = Db::AlterTable<'a>
    where
        Self: 'a;

    type Select<'a> = Db::Select<'a>
    where
        Self: 'a;

    type Insert<'a, N: Length> = Db::Insert<'a, N>
    where
        Self: 'a;

    async fn create_db(&mut self, _name: &str) -> Result<(), Self::Error> {
        Err(Self::Error::custom(
            "TempDatabase does not support creating more databases",
        ))
    }

    async fn drop_db(&mut self, _name: &str) -> Result<(), Self::Error> {
        Err(Self::Error::custom(
            "TempDatabase does not support dropping databases",
        ))
    }

    fn create_table<'a, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<SchemaColumn<'a>, N>,
    ) -> Self::CreateTable<'a, N> {
        self.db.create_table(table, columns)
    }

    fn alter_table<'a>(&'a self, table: impl Into<Cow<'a, str>> + Send) -> Self::AlterTable<'a> {
        self.db.alter_table(table)
    }

    fn select<'a>(
        &'a self,
        columns: &'a [SelectColumn<'a>],
        table: impl Into<Cow<'a, str>> + Send,
    ) -> Self::Select<'a> {
        self.db.select(columns, table)
    }

    fn insert<'a, C, N>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<C, N>,
    ) -> Self::Insert<'a, N>
    where
        C: Into<String>,
        N: Length,
    {
        self.db.insert(table, columns)
    }
}
