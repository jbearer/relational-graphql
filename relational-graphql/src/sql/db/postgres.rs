//! Instantiation of the abstract [`db`](super) interface for PostgreSQL.
//!
//! This instantiation is built on [`async-postgres`].
#![cfg(feature = "postgres")]

use super::{
    escape_ident, Clause, ConstraintKind, JoinClause, SchemaColumn, SelectColumn, Value,
    WhereClause,
};
use crate::{Array, Length};
use async_std::task::spawn;
use async_trait::async_trait;
use bytes::BytesMut;
use derive_more::From;
use futures::{
    future,
    stream::{self, BoxStream},
    StreamExt, TryFutureExt, TryStreamExt,
};
use itertools::Itertools;
use snafu::Snafu;
use std::borrow::Cow;
use std::fmt::{Debug, Display};
use tokio_postgres::types::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

pub use async_postgres::{Config, Row};

/// Errors returned by a PostgreSQL database.
#[derive(Debug, Snafu, From)]
pub enum Error {
    #[from]
    Sql {
        source: async_postgres::Error,
    },
    Connect {
        source: std::io::Error,
    },
    OutOfRange {
        ty: &'static str,
        value: String,
    },
    UnsupportedType {
        ty: Type,
    },
    Custom {
        message: String,
    },
}

impl super::Error for Error {
    fn custom(msg: impl Display) -> Self {
        Self::Custom {
            message: msg.to_string(),
        }
    }
}

/// A connection to a PostgreSQL databsae.
pub struct Connection(tokio_postgres::Client);

impl Connection {
    /// Establish a new connection with the given [`Config`].
    pub async fn new(config: Config) -> Result<Self, Error> {
        let (client, conn) = async_postgres::connect(config)
            .await
            .map_err(|source| Error::Connect { source })?;
        spawn(conn);
        Ok(Self(client))
    }

    async fn query<'a, I>(
        &self,
        statement: &str,
        params: I,
    ) -> Result<BoxStream<'static, Result<Row, Error>>, Error>
    where
        I: Debug + IntoIterator<Item = &'a Value>,
        I::IntoIter: ExactSizeIterator,
    {
        tracing::info!(?params, "{}", statement);
        let params = params.into_iter().map(|param| {
            let param: &dyn ToSql = param;
            param
        });
        let stream = self
            .0
            .query_raw(statement, params)
            .await
            .map_err(Error::from)?;
        Ok(stream.map_err(Error::from).boxed())
    }
}

impl super::Connection for Connection {
    type Error = Error;
    type AlterTable<'a> = AlterTable<'a>;
    type CreateTable<'a, N: Length> = CreateTable<'a, N>;
    type Select<'a> = Select<'a>;
    type Insert<'a, N: Length> = Insert<'a, N>;

    fn alter_table<'a>(&'a self, table: impl Into<Cow<'a, str>> + Send) -> Self::AlterTable<'a> {
        AlterTable::new(self, table.into())
    }

    fn create_table<'a, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<SchemaColumn<'a>, N>,
    ) -> Self::CreateTable<'a, N> {
        CreateTable::new(self, table.into(), columns)
    }

    fn select<'a>(
        &'a self,
        select: &'a [SelectColumn<'a>],
        table: impl Into<Cow<'a, str>> + Send,
    ) -> Self::Select<'a> {
        Select::new(self, select, table.into())
    }

    fn insert<'a, C, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<C, N>,
    ) -> Self::Insert<'a, N>
    where
        C: Into<String>,
    {
        Insert::new(self, table.into(), columns)
    }
}

/// A query against a PostgreSQL database.
pub struct Select<'a>(Result<SelectInner<'a>, Error>);

struct SelectInner<'a> {
    conn: &'a Connection,
    select: &'a [SelectColumn<'a>],
    table: Cow<'a, str>,
    joins: Vec<String>,
    filters: Vec<String>,
    params: Vec<Value>,
}

impl<'a> Select<'a> {
    fn new(conn: &'a Connection, select: &'a [SelectColumn<'a>], table: Cow<'a, str>) -> Self {
        Self(Ok(SelectInner {
            conn,
            select,
            table,
            joins: Default::default(),
            filters: Default::default(),
            params: Default::default(),
        }))
    }
}

impl<'a> super::Select<'a> for Select<'a> {
    type Error = Error;
    type Row = Row;
    type Stream = BoxStream<'a, Result<Self::Row, Self::Error>>;

    fn clause(self, clause: Clause) -> Self {
        let Ok(mut query) = self.0 else { return self; };
        match clause {
            Clause::Where(WhereClause { column, op, param }) => {
                query.params.push(param);
                query.filters.push(format!(
                    "WHERE {} {op} ${}",
                    column.escape(),
                    query.params.len()
                ));
            }
            Clause::Join(JoinClause {
                table,
                lhs,
                op,
                rhs,
            }) => {
                query.joins.push(format!(
                    "JOIN {} ON {} {op} {}",
                    escape_ident(table),
                    lhs.escape(),
                    rhs.escape()
                ));
            }
        }
        Self(Ok(query))
    }

    fn stream(self) -> Self::Stream {
        let query = match self.0 {
            Ok(query) => query,
            Err(err) => return stream::once(future::ready(Err(err))).boxed(),
        };

        // The async block is necessary to move data owned by `query` into the future, so we can
        // return the future without returning a reference to the local `query`.
        async move {
            // Format the `SELECT` part of the query.
            let columns = query
                .select
                .iter()
                .map(|col| match col {
                    SelectColumn::Column(col) => col.escape(),
                    SelectColumn::All => "*".to_string(),
                })
                .join(", ");
            let table = escape_ident(query.table);

            // Construct the SQL statement.
            let statement = format!(
                "SELECT {columns} FROM {table} {} {}",
                query.joins.join(" "),
                query.filters.join(" ")
            );

            // Run the query.
            let rows = query.conn.query(statement.as_str(), &query.params).await?;
            Ok(rows)
        }
        .try_flatten_stream()
        .boxed()
    }
}

/// An `INSERT` statement for a PostgreSQL database.
pub struct Insert<'a, N: Length> {
    conn: &'a Connection,
    table: Cow<'a, str>,
    columns: Array<String, N>,
    num_rows: usize,
    params: Vec<Value>,
}

impl<'a, N: Length> Insert<'a, N> {
    fn new<C: Into<String>>(
        conn: &'a Connection,
        table: Cow<'a, str>,
        columns: Array<C, N>,
    ) -> Self {
        Self {
            conn,
            table,
            columns: columns.map(|c| c.into()),
            num_rows: 0,
            params: vec![],
        }
    }
}

#[async_trait]
impl<'a, N: Length> super::Insert<N> for Insert<'a, N> {
    type Error = Error;

    fn rows<R>(mut self, rows: R) -> Self
    where
        R: IntoIterator<Item = Array<Value, N>>,
    {
        for row in rows {
            self.params.extend(row);
            self.num_rows += 1;
        }
        self
    }

    async fn execute(self) -> Result<(), Error> {
        let columns = self.columns.iter().map(escape_ident).join(",");
        let rows = (0..self.num_rows)
            .map(|i| {
                let values = (0..N::USIZE)
                    .map(|j| {
                        // In the query itself, just reference a parameter by number. We will pass
                        // the value itself into the query as a parameter to prevent SQL injection.
                        let param_num = i * N::USIZE + j;
                        // Params are 1-indexed.
                        format!("${}", param_num + 1)
                    })
                    .join(",");
                format!("({values})")
            })
            .join(",");
        self.conn
            .query(
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    escape_ident(self.table),
                    columns,
                    rows
                )
                .as_str(),
                &self.params,
            )
            .await?;
        Ok(())
    }
}

/// A `CREATE TABLE` statement for a PostgreSQL database.
pub struct CreateTable<'a, N: Length> {
    conn: &'a Connection,
    table: Cow<'a, str>,
    columns: Array<SchemaColumn<'a>, N>,
    constraints: Vec<(ConstraintKind, Vec<String>)>,
}

impl<'a, N: Length> CreateTable<'a, N> {
    fn new(conn: &'a Connection, table: Cow<'a, str>, columns: Array<SchemaColumn<'a>, N>) -> Self {
        Self {
            conn,
            table,
            columns,
            constraints: vec![],
        }
    }
}

#[async_trait]
impl<'a, N: Length> super::CreateTable for CreateTable<'a, N> {
    type Error = Error;

    fn constraint<I>(mut self, kind: ConstraintKind, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.constraints
            .push((kind, columns.into_iter().map(|col| col.into()).collect()));
        self
    }

    async fn execute(self) -> Result<(), Self::Error> {
        let table = escape_ident(&self.table);
        let columns = self
            .columns
            .into_iter()
            .map(|col| {
                let ty = match col.ty() {
                    super::Type::Int4 => "int4",
                    super::Type::Int8 => "int8",
                    super::Type::UInt4 => "int8",
                    super::Type::UInt8 => "int8",
                    super::Type::Text => "text",
                    super::Type::Serial => "serial",
                };
                format!("{} {}", col.name(), ty)
            })
            .join(",");
        let constraints = self
            .constraints
            .into_iter()
            .map(|(kind, cols)| format_constraint(&self.table, kind, &cols))
            .join(",");
        self.conn
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {table} ({columns}{}{constraints})",
                    if constraints.is_empty() { "" } else { "," }
                )
                .as_str(),
                [],
            )
            .await?;
        Ok(())
    }
}

/// An `ALTER TABLE` statement for a PostgreSQL database.
pub struct AlterTable<'a> {
    conn: &'a Connection,
    table: Cow<'a, str>,
    constraints: Vec<(ConstraintKind, Vec<String>)>,
}

impl<'a> AlterTable<'a> {
    fn new(conn: &'a Connection, table: Cow<'a, str>) -> Self {
        Self {
            conn,
            table,
            constraints: vec![],
        }
    }
}

#[async_trait]
impl<'a> super::AlterTable for AlterTable<'a> {
    type Error = Error;

    fn add_constraint<I>(mut self, kind: ConstraintKind, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.constraints
            .push((kind, columns.into_iter().map(|col| col.into()).collect()));
        self
    }

    async fn execute(self) -> Result<(), Self::Error> {
        if self.constraints.is_empty() {
            // An `ALTER TABLE` statement with no actions is invalid SQL, but if it had an
            // interpretation, the only reasonable one would be to do nothing.
            return Ok(());
        }

        let table = escape_ident(&self.table);
        let constraints = self
            .constraints
            .into_iter()
            .map(|(kind, cols)| format!("ADD {}", format_constraint(&self.table, kind, &cols)))
            .join("");
        self.conn
            .query(format!("ALTER TABLE {table} {constraints}").as_str(), [])
            .await?;
        Ok(())
    }
}

impl super::Row for Row {
    type Error = Error;

    fn column(&self, column: usize) -> Result<Value, Self::Error> {
        Ok(self.try_get(column)?)
    }
}

impl ToSql for Value {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        Self: Sized,
    {
        match self {
            Self::Text(x) => x.to_sql(ty, out),
            Self::Int4(x) => x.to_sql(ty, out),
            Self::Int8(x) => x.to_sql(ty, out),
            Self::UInt4(x) => x.to_sql(ty, out),
            Self::UInt8(x) => {
                // [`u64`] doesn't implement [`ToSql`], so we have to cast to a [`u32`] first.
                let x = u32::try_from(*x).map_err(|_| {
                    Box::new(Error::OutOfRange {
                        ty: "u32",
                        value: x.to_string(),
                    })
                })?;
                x.to_sql(ty, out)
            }
            Self::Serial(x) => x.to_sql(ty, out),
        }
    }

    accepts!(INT4, INT8, TEXT);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match ty {
            &Type::INT4 => Ok(Self::Int4(i32::from_sql(ty, raw)?)),
            &Type::INT8 => Ok(Self::Int8(i64::from_sql(ty, raw)?)),
            &Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
            ty => Err(Box::new(Error::UnsupportedType { ty: ty.clone() })),
        }
    }

    accepts!(INT4, INT8, TEXT);
}

fn format_constraint(table: impl AsRef<str>, kind: ConstraintKind, cols: &[String]) -> String {
    let table = table.as_ref();
    let cols_ident = cols.iter().join("-");
    let cols = cols.iter().map(escape_ident).join(",");
    match kind {
        ConstraintKind::PrimaryKey => {
            format!(
                "CONSTRAINT {} PRIMARY KEY ({cols})",
                escape_ident(format!("{table}-pk-{cols_ident}"))
            )
        }
        ConstraintKind::Unique => {
            format!(
                "CONSTRAINT {} UNIQUE ({cols})",
                escape_ident(format!("{table}-uq-{cols_ident}"))
            )
        }
        ConstraintKind::ForeignKey { table } => {
            format!(
                "CONSTRAINT {} FOREIGN KEY ({cols}) REFERENCES {}",
                escape_ident(format!("{table}-fk-{table}-{cols_ident}")),
                escape_ident(table)
            )
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        graphql::{
            backend::DataSource,
            type_system::{Id, IntCmpOp, Resource, Type, Value},
        },
        init_logging,
        sql::data_source::SqlDataSource,
    };
    use rand::RngCore;
    use std::env;
    use std::process::Command;
    use std::str;

    struct Db {
        name: String,
        port: u16,
        password: String,
    }

    impl Db {
        fn create() -> Option<Self> {
            if env::var("POSTGRES_TESTS").is_err() {
                tracing::warn!("skipping postgres test since POSTGRES_TESTS are not enabled");
                return None;
            }

            let name = format!("db{}", rand::thread_rng().next_u64());
            let port = env::var("POSTGRES_TESTS_PORT")
                .map(|port| port.parse().unwrap())
                .unwrap_or(5432);
            let password = env::var("POSTGRES_TESTS_PASSWORD").unwrap_or("password".to_string());

            tracing::info!("Creating test DB {name} on port {port}");
            let output = Command::new("createdb")
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(&port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg(&name)
                .env("PGPASSWORD", &password)
                .output()
                .unwrap();
            if !output.status.success() {
                panic!(
                    "createdb failed: {}",
                    str::from_utf8(&output.stderr).unwrap()
                );
            }

            Some(Self {
                name,
                port,
                password,
            })
        }

        async fn connect(&self) -> Connection {
            let mut config = Config::default();
            config
                .dbname(&self.name)
                .host("127.0.0.1")
                .user("postgres")
                .password(&self.password)
                .host("localhost")
                .port(self.port);
            Connection::new(config).await.unwrap()
        }
    }

    impl Drop for Db {
        fn drop(&mut self) {
            tracing::info!("Dropping test DB {}", self.name);
            let output = Command::new("dropdb")
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(&self.port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg(&self.name)
                .env("PGPASSWORD", &self.password)
                .output()
                .unwrap();
            if !output.status.success() {
                tracing::error!("dropdb failed: {}", str::from_utf8(&output.stderr).unwrap());
            }
        }
    }

    macro_rules! postgres_test {
        () => {
            match Db::create() {
                Some(db) => db,
                None => return,
            }
        };
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Simple {
        id: Id,
        field: i32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Reference {
        id: Id,
        simple: Simple,
    }

    #[async_std::test]
    async fn test_postgres_data_source() {
        init_logging();
        let db = postgres_test!();
        let mut conn = SqlDataSource::from(db.connect().await);

        let simples = [Simple { id: 1, field: 0 }, Simple { id: 2, field: 1 }];

        conn.register::<Reference>().await.unwrap();
        conn.insert::<Simple, _>([
            simple::SimpleInput { field: 0 },
            simple::SimpleInput { field: 1 },
        ])
        .await
        .unwrap();

        // Select all elements.
        let results = conn.query::<Simple>(None).await.unwrap();
        let page = conn
            .load_page(&results, Default::default())
            .await
            .unwrap()
            .into_iter()
            .map(|edge| edge.into_node())
            .collect::<Vec<_>>();
        assert_eq!(page, simples);

        // Select with a filter.
        let results = conn
            .query::<Simple>(Some(
                Simple::has()
                    .field(<i32 as Type>::Predicate::cmp(IntCmpOp::EQ, Value::Lit(1)))
                    .into(),
            ))
            .await
            .unwrap();
        let page = conn
            .load_page(&results, Default::default())
            .await
            .unwrap()
            .into_iter()
            .map(|edge| edge.into_node())
            .collect::<Vec<_>>();
        assert_eq!(page, &simples[1..]);

        // Insert an object with a relation.
        conn.insert::<Reference, _>([reference::ReferenceInput { simple: 2 }])
            .await
            .unwrap();

        // Query it back, filling in the relationship.
        let results = conn.query::<Reference>(None).await.unwrap();
        let page = conn
            .load_page(&results, Default::default())
            .await
            .unwrap()
            .into_iter()
            .map(|edge| edge.into_node())
            .collect::<Vec<_>>();
        assert_eq!(
            page,
            [Reference {
                id: 1,
                simple: simples[1].clone()
            }]
        );
    }
}
