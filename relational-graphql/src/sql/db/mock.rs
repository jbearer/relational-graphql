//! Mock instantiation of the abstract [`db`](super) interface for PostgreSQL.
//!
//! This instantiation is built on a simple in-memory database. It is useful for testing in
//! isolation from an actual database.
#![cfg(any(test, feature = "mocks"))]

use super::{
    Boolean, Clause, Column, ConstraintKind, Error as _, FromItem, JoinClause, SchemaColumn,
    SelectColumn, Type, Value, WhereClause,
};
use crate::{
    typenum::{Sub1, B1},
    Array, Length,
};
use async_std::sync::{Arc, RwLock};
use async_trait::async_trait;
use derive_more::From;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryFutureExt,
};
use itertools::Itertools;
use snafu::Snafu;
use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt::Display;
use std::iter;
use std::ops::Sub;

/// Errors returned by the in-memory database.
#[derive(Debug, Snafu, From)]
#[snafu(display("mock DB error: {}", message))]
pub struct Error {
    message: String,
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}

impl super::Error for Error {
    fn custom(msg: impl Display) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

/// The in-memory database.
#[derive(Debug, Default)]
struct Db {
    tables: HashMap<String, Table>,
}

/// An in-memory table.
#[derive(Debug)]
struct Table {
    name: String,
    serial_cols: Vec<SchemaColumn<'static>>,
    explicit_cols: Vec<SchemaColumn<'static>>,
    rows: Vec<Row>,
}

impl Table {
    fn new<N: Length>(name: String, schema: Array<SchemaColumn<'static>, N>) -> Self {
        // Separate the auto-incrementing columns from the columns that require explicit values.
        let (serial_cols, explicit_cols) =
            schema.into_iter().partition(|col| col.ty() == Type::Serial);
        Self {
            name,
            serial_cols,
            explicit_cols,
            rows: vec![],
        }
    }

    fn append<N: Length>(
        &mut self,
        rows: impl IntoIterator<Item = Array<Value, N>>,
    ) -> Result<(), Error> {
        // We require a value for all columns except the serial columns (which are auto-incremented).
        if N::USIZE != self.explicit_cols.len() {
            return Err(Error::from(format!(
                "incorrect width for table {} (found {}, expected {})",
                self.name,
                self.explicit_cols.len(),
                N::USIZE
            )));
        }

        for row in rows {
            // Auto-increment the serial columns.
            let auto_values =
                iter::repeat((self.rows.len() as i32 + 1).into()).take(self.serial_cols.len());

            // Take the rest of the values from the input.
            let columns = auto_values.chain(row);

            self.rows.push(Row::new(columns.collect()));
        }

        Ok(())
    }

    fn schema(&self) -> impl '_ + Iterator<Item = SchemaColumn<'static>> {
        self.serial_cols.iter().chain(&self.explicit_cols).cloned()
    }
}

/// A connection to the in-memory database.
#[derive(Clone, Debug)]
pub struct Connection(Arc<RwLock<Db>>);

impl Connection {
    /// Create a new database and connect to it.
    ///
    /// This will create a connection to a fresh, empty database. It will not be connected or
    /// related to any previous connection or database. Once the database is created, this
    /// connection can be [cloned](Clone) in order to create multiple simultaneous connections to
    /// the same database.
    pub fn create() -> Self {
        Self(Default::default())
    }

    /// Create a table with the given column names.
    pub async fn create_table<N: Length>(
        &self,
        table: impl Into<String>,
        columns: Array<SchemaColumn<'static>, N>,
    ) -> Result<(), Error> {
        let mut db = self.0.write().await;
        let table = table.into();
        if let Entry::Vacant(e) = db.tables.entry(table.clone()) {
            e.insert(Table::new(table, columns));
        }
        Ok(())
    }

    /// Create a table with the given column names and row values.
    ///
    /// It is assumed that the schema contains exactly 1 auto-increment ID column, so the values
    /// specified for each row must be 1 less than the size of the schema.
    pub async fn create_table_with_rows<N: Length + Sub<B1>>(
        &self,
        table: impl Into<String>,
        columns: Array<SchemaColumn<'static>, N>,
        rows: impl IntoIterator<Item = Array<Value, Sub1<N>>>,
    ) -> Result<(), Error>
    where
        Sub1<N>: Length,
    {
        let table = table.into();
        self.create_table(&table, columns).await?;

        let mut db = self.0.write().await;
        let table = db.tables.get_mut(&table).unwrap();
        table.append(rows)
    }

    /// The schema of this database.
    ///
    /// The schema maps table names to the schema for each table. Each table schema consists of a
    /// list of column schemas.
    pub async fn schema(&self) -> HashMap<String, Vec<SchemaColumn<'static>>> {
        self.0
            .read()
            .await
            .tables
            .iter()
            .map(|(name, table)| (name.clone(), table.schema().collect()))
            .collect()
    }
}

#[async_trait]
impl super::Connection for Connection {
    type Error = Error;
    type CreateTable<'a, N: Length> = CreateTable<'a, N>;
    type AlterTable<'a> = AlterTable;
    type Select<'a> = Select<'a>;
    type Insert<'a, N: Length> = Insert<'a, N>;
    type Update<'a> = Update<'a>;

    async fn create_db(&mut self, _name: &str) -> Result<(), Self::Error> {
        Err(Self::Error::custom(
            "Mock database does not support creating more databases",
        ))
    }

    async fn drop_db(&mut self, _name: &str) -> Result<(), Self::Error> {
        Err(Self::Error::custom(
            "Mock database does not support dropping databases",
        ))
    }

    fn create_table<'a, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<SchemaColumn<'a>, N>,
    ) -> Self::CreateTable<'a, N> {
        CreateTable {
            db: self,
            table: table.into(),
            columns,
        }
    }

    fn alter_table<'a>(&'a self, _table: impl Into<Cow<'a, str>> + Send) -> Self::AlterTable<'a> {
        AlterTable
    }

    fn select<'a>(
        &'a self,
        select: &'a [SelectColumn<'a>],
        table: impl Into<Cow<'a, str>> + Send,
    ) -> Self::Select<'a> {
        Select {
            db: &self.0,
            table: table.into(),
            columns: select,
            joins: vec![],
            filters: vec![],
        }
    }

    fn insert<'a, C, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<C, N>,
    ) -> Self::Insert<'a, N>
    where
        C: Into<String>,
    {
        Insert {
            db: &self.0,
            table: table.into(),
            columns: columns.map(|c| c.into()),
            rows: vec![],
        }
    }

    fn update<'a>(&'a self, table: impl Into<Cow<'a, str>> + Send) -> Self::Update<'a> {
        Update {
            db: &self.0,
            table: table.into(),
            columns: Default::default(),
            filters: Default::default(),
            from: Default::default(),
        }
    }
}

/// A query against an in-memory database.
pub struct Select<'a> {
    db: &'a RwLock<Db>,
    table: Cow<'a, str>,
    columns: &'a [SelectColumn<'a>],
    joins: Vec<JoinClause<'a>>,
    filters: Vec<WhereClause<'a>>,
}

impl<'a> super::Select<'a> for Select<'a> {
    type Error = Error;
    type Row = Row;
    type Stream = BoxStream<'a, Result<Self::Row, Self::Error>>;

    fn clause(mut self, clause: Clause<'a>) -> Self {
        match clause {
            Clause::Join(join) => self.joins.push(join),
            Clause::Where(filter) => self.filters.push(filter),
        }
        self
    }

    fn stream(self) -> Self::Stream {
        async move {
            let db = self.db.read().await;
            let table = db
                .tables
                .get(&*self.table)
                .ok_or_else(|| Error::from(format!("no such table {}", self.table)))?;

            tracing::info!("SELECT {:?} FROM {}", self.columns, self.table);
            let mut rows = table.rows.clone();
            let mut schema = table
                .schema()
                .map(|col| Column::qualified(self.table.clone(), col.name()))
                .collect::<Vec<_>>();
            for JoinClause {
                table,
                lhs,
                op,
                rhs,
            } in self.joins
            {
                tracing::info!("JOIN {table} ON {lhs} {op} {rhs}");
                let join_table = db
                    .tables
                    .get(&*table)
                    .ok_or_else(|| Error::from(format!("no such table {}", table)))?;
                schema.extend(
                    join_table
                        .schema()
                        .map(|col| Column::qualified(table.clone(), col.name())),
                );
                rows = rows
                    .into_iter()
                    .cartesian_product(join_table.rows.clone())
                    .filter_map(|(l, r)| l.join(r, &schema, &lhs, &op, &rhs).transpose())
                    .try_collect()?;
            }
            for clause in self.filters {
                tracing::info!("WHERE {clause:?}");
                rows = rows
                    .into_iter()
                    .filter_map(|row| match row.test(&schema, &clause) {
                        Ok(true) => Some(Ok(row)),
                        Ok(false) => None,
                        Err(err) => Some(Err(err)),
                    })
                    .try_collect()?;
            }

            let rows = rows
                .into_iter()
                .map(|row| row.select(&schema, self.columns))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(stream::iter(rows).map(Ok))
        }
        .try_flatten_stream()
        .boxed()
    }
}

/// An insert statement for an in-memory database.
pub struct Insert<'a, N: Length> {
    db: &'a RwLock<Db>,
    table: Cow<'a, str>,
    columns: Array<String, N>,
    rows: Vec<Array<Value, N>>,
}

#[async_trait]
impl<'a, N: Length> super::Insert<N> for Insert<'a, N> {
    type Error = Error;

    fn rows<R>(mut self, rows: R) -> Self
    where
        R: IntoIterator<Item = Array<Value, N>>,
    {
        self.rows.extend(rows);
        self
    }

    async fn execute(mut self) -> Result<(), Error> {
        let mut db = self.db.write().await;
        let table = db
            .tables
            .get_mut(&*self.table)
            .ok_or_else(|| Error::from(format!("no such table {}", self.table)))?;
        tracing::info!(
            "INSERT INTO {} ({:?}) VALUES {:?}",
            self.table,
            self.columns,
            self.rows
        );

        // A permutation of column indices mapping positions in the input rows to the positions of
        // the corresponding rows in the table schema.
        let mut column_permutation = Array::<usize, N>::default();
        for (i, name) in self.columns.into_iter().enumerate() {
            let col = table
                .explicit_cols
                .iter()
                .position(|col| col.name() == name)
                .ok_or_else(|| Error::from(format!("table {} has no column {name}", self.table)))?;
            column_permutation[i] = col;
        }

        for row in &mut self.rows {
            row.permute(&column_permutation);
        }

        table.append(self.rows)
    }
}

/// An update statement for an in-memory databse.
pub struct Update<'a> {
    db: &'a RwLock<Db>,
    table: Cow<'a, str>,
    columns: Vec<(Cow<'a, str>, Column<'a>)>,
    filters: Vec<(Column<'a>, Cow<'a, str>, Column<'a>)>,
    from: Vec<FromItem<'a>>,
}

#[async_trait]
impl<'a> super::Update<'a> for Update<'a> {
    type Error = Error;

    fn set(mut self, set: impl Into<Cow<'a, str>>, to: Column<'a>) -> Self {
        self.columns.push((set.into(), to));
        self
    }

    fn filter(mut self, lhs: Column<'a>, op: impl Into<Cow<'a, str>>, rhs: Column<'a>) -> Self {
        self.filters.push((lhs, op.into(), rhs));
        self
    }

    fn from(mut self, item: FromItem<'a>) -> Self {
        self.from.push(item);
        self
    }

    async fn execute(self) -> Result<(), Self::Error> {
        tracing::info!(
            "UPDATE {} SET ({:?}) WHERE {:?} FROM {:?}",
            self.table,
            self.columns,
            self.filters,
            self.from
        );

        let mut db = self.db.write().await;
        let table = db
            .tables
            .get_mut(&*self.table)
            .ok_or_else(|| Error::from(format!("no such table {}", self.table)))?;
        let schema = table
            .schema()
            .map(|col| Column::qualified(self.table.clone(), col.name()))
            .collect::<Vec<_>>();

        // Compute the cartesian product of all the `FROM` items -- that is, one big table
        // containing all combinations of rows from the `FROM` list. This is like joining all the
        // `FROM` items together on the condition `true`.
        let (schemas, views): (Vec<_>, Vec<_>) = self.from.into_iter().map(Row::view).unzip();
        let from_schema = schemas.into_iter().flatten().collect::<Vec<_>>();
        let from_table = views
            .into_iter()
            .multi_cartesian_product()
            .map(|rows| Row {
                columns: rows.into_iter().flat_map(|row| row.columns).collect(),
            })
            .collect::<Vec<_>>();
        assert!(from_table
            .iter()
            .all(|row| row.columns.len() == from_schema.len()));

        // Go through each row in the table to update and check if there are any combinations rows
        // in the `FROM` list for which the `WHERE` clause is satisfied on those rows and the row to
        // be updated.
        for row in &mut table.rows {
            let mut satisfying_row = None;
            'from_rows: for from_row in &from_table {
                for (lhs, op, rhs) in &self.filters {
                    // Both sides of the filter must exist in either this table or the `FROM` table.
                    let lhs = row
                        .get(&schema, lhs)
                        .or_else(|_| from_row.get(&from_schema, lhs))?;
                    let rhs = row
                        .get(&schema, rhs)
                        .or_else(|_| from_row.get(&from_schema, rhs))?;
                    if !Row::cmp(lhs, op, rhs) {
                        continue 'from_rows;
                    }
                }
                satisfying_row = Some(from_row);
            }

            let Some(satisfying_row) = satisfying_row else {
                continue;
            };
            tracing::info!("matched row {row:?} with {satisfying_row:?}");

            // Set the value of each column from `self.columns` in this row.
            for (column, value) in &self.columns {
                tracing::info!("SET {column} = {value}");
                let index = schema
                    .iter()
                    .position(|col| col.name == *column)
                    .ok_or_else(|| Error::from(format!("no such column {column} to update")))?;
                // The value comes from this table, if it exists, or else from the `FROM` table.
                let value = row
                    .get(&schema, value)
                    .or_else(|_| satisfying_row.get(&from_schema, value))?;
                row.columns[index] = value.clone();
            }
        }

        Ok(())
    }
}

/// A create table statement for an in-memory database.
pub struct CreateTable<'a, N: Length> {
    db: &'a Connection,
    table: Cow<'a, str>,
    columns: Array<SchemaColumn<'a>, N>,
}

#[async_trait]
impl<'a, N: Length> super::CreateTable for CreateTable<'a, N> {
    type Error = Error;

    fn constraint<I>(self, _kind: ConstraintKind, _columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        // The mock database doesn't enforce constraints.
        self
    }

    async fn execute(self) -> Result<(), Self::Error> {
        tracing::info!("CREATE TABLE {} ({:?})", self.table, self.columns);
        self.db
            .create_table(self.table, self.columns.map(|col| col.into_static()))
            .await
    }
}

/// An alter table statement for an in-memory database.
pub struct AlterTable;

#[async_trait]
impl super::AlterTable for AlterTable {
    type Error = Error;

    fn add_constraint<I>(self, _kind: ConstraintKind, _columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        // The mock database doesn't enforce constraints.
        self
    }

    async fn execute(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A row in an in-memory table.
#[derive(Clone, Debug, Default)]
pub struct Row {
    columns: Vec<Value>,
}

macro_rules! test_int_val {
    ($l:expr, $op:expr, $r:expr, $($t:ident),+) => {
        match ($l, $r) {
            $(
                (Value::$t(l), Value::$t(r)) => match $op {
                    ">" => l > r,
                    ">=" => l >= r,
                    "<" => l < r,
                    "<=" => l <= r,
                    op => panic!("unsupported int op {op}"),
                }
            ),+
            (l, r) => panic!("type mismatch for op {}: {:?}, {:?}", $op, l, r),
        }
    };
    ($l:expr, $op:expr, $r:expr) => {
        test_int_val!($l, $op, $r, Int4, Int8, UInt4, UInt8)
    };
}

impl Row {
    /// Create a row with the given entries.
    fn new(columns: Vec<Value>) -> Self {
        Self { columns }
    }

    /// Create a schema and set of rows corresponding to a temporary view or virtual table.
    fn view(item: FromItem) -> (Vec<Column>, Vec<Self>) {
        match item {
            FromItem::Alias {
                table,
                columns,
                item,
            } => {
                let rows = Row::view(*item).1;
                let schema = columns
                    .into_iter()
                    .map(|column| Column::qualified(table.clone(), column))
                    .collect();
                (schema, rows)
            }
            FromItem::Values { rows } => {
                let schema = (0..rows[0].len())
                    .map(|i| Column::named(i.to_string()))
                    .collect();
                let rows = rows.into_iter().map(Row::new).collect();
                (schema, rows)
            }
        }
    }

    /// Test if this row should be included based on the given condition..
    fn test(&self, schema: &[Column], clause: &WhereClause) -> Result<bool, Error> {
        match clause {
            WhereClause::Any(clauses) => clauses
                .iter()
                .map(|clause| self.test(schema, clause))
                .fold_ok(false, |x, y| x || y),
            WhereClause::All(clauses) => clauses
                .iter()
                .map(|clause| self.test(schema, clause))
                .fold_ok(true, |x, y| x && y),
            WhereClause::Predicate(pred) => match pred {
                Boolean::Cmp { column, op, param } => {
                    Ok(Self::cmp(self.get(schema, column)?, op, param))
                }
                Boolean::OneOf { column, params } => Ok(params.contains(self.get(schema, column)?)),
                Boolean::Matches { .. } => unimplemented!("text search in mock database"),
            },
        }
    }

    /// Join this row with another row if the joined pair matches a condition.
    ///
    /// `schema` should be the concatenated schemas of `self` and `other`.
    ///
    /// If the joined pair matches, a new row will be returned which consists of all of the columns
    /// of this row, in order, followed by all of the columns of the other row, in order.
    fn join(
        mut self,
        other: Row,
        schema: &[Column],
        lhs: &Column,
        op: &str,
        rhs: &Column,
    ) -> Result<Option<Self>, Error> {
        self.columns.extend(other.columns);
        Ok(
            if Self::cmp(self.get(schema, lhs)?, op, self.get(schema, rhs)?) {
                Some(self)
            } else {
                None
            },
        )
    }

    /// Create a new row with just the specified columns, in the specified order.
    fn select(self, schema: &[Column], columns: &[SelectColumn]) -> Result<Self, Error> {
        let mut selected = vec![];
        for col in columns {
            match col {
                SelectColumn::All => return Ok(self),
                SelectColumn::Column(col) => {
                    selected.push(self.get(schema, col)?.clone());
                }
            }
        }
        Ok(Self { columns: selected })
    }

    /// Compare to values.
    fn cmp(lhs: &Value, op: &str, rhs: &Value) -> bool {
        match op {
            "=" => lhs == rhs,
            "!=" => lhs != rhs,
            int_op => test_int_val!(lhs, int_op, rhs),
        }
    }

    /// Get the value of the named column.
    fn get(&self, schema: &[Column], col: &Column) -> Result<&Value, Error> {
        let index = schema
            .iter()
            .position(|schema_col| {
                if col.table.is_some() {
                    // Every column in the schema is qualified, so if `col` is also qualified, we
                    // want an exact match.
                    col == schema_col
                } else {
                    // Otherwise find the first column whose name matches `col`.
                    col.name == schema_col.name
                }
            })
            .ok_or_else(|| Error::from(format!("no such column {col}")))?;
        Ok(&self.columns[index])
    }
}

impl super::Row for Row {
    type Error = Error;

    fn column(&self, column: usize) -> Result<Value, Self::Error> {
        self.columns
            .get(column)
            .cloned()
            .ok_or_else(|| format!("column index {column} out of range").into())
    }
}
