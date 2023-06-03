//! Abstract interface to a SQL database.

use crate::{Array, Length};
use async_trait::async_trait;
use derive_more::{Display, From, TryInto};
use futures::{Stream, StreamExt, TryStreamExt};
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};

pub mod mock;
pub mod postgres;
pub mod temp;

/// Errors returned by the database.
pub trait Error: Sized + Send + std::error::Error {
    /// Wrap a custom message into this error type.
    fn custom(msg: impl Display) -> Self;

    /// An error indicating that a query returned more than the `expected` number of rows.
    fn too_many_rows(expected: usize) -> Self {
        Self::custom(format!(
            "query result has more rows than the expected {expected}"
        ))
    }

    /// An error indicating that a query which was expected to return some rows did not.
    fn empty_rows() -> Self {
        Self::custom("query result is empty")
    }
}

/// A column in a list of columns selected from a query.
#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectColumn<'a> {
    /// A single named column.
    #[display(fmt = "{}", _0)]
    Column(Column<'a>),
    /// Select all columns.
    #[display(fmt = "*")]
    All,
}

/// A column in a schema.
///
/// This describes the structure and format of each entry in the column, along with column-level
/// metadata like the name and constraints.
#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[display(fmt = "{name} {ty}")]
pub struct SchemaColumn<'a> {
    name: Cow<'a, str>,
    ty: Type,
}

impl<'a> SchemaColumn<'a> {
    /// Create a column given a name and type.
    pub fn new(name: impl Into<Cow<'a, str>>, ty: Type) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }

    /// The name of this column
    pub fn name(&self) -> Cow<'a, str> {
        self.name.clone()
    }

    /// The type of this column
    pub fn ty(&self) -> Type {
        self.ty
    }

    /// Remove the lifetime requirement from `self` by cloning and taking ownership of borrowed
    /// data.
    pub fn into_static(self) -> SchemaColumn<'static> {
        SchemaColumn {
            name: Cow::Owned(self.name.into_owned()),
            ty: self.ty,
        }
    }
}

/// A connection to the database.
#[async_trait]
pub trait Connection {
    /// Errors returned from queries.
    type Error: Error;

    /// A `CREATE TABLE` statement which can be executed against the database.
    type CreateTable<'a, N: Length>: CreateTable<Error = Self::Error>
    where
        Self: 'a;

    /// An `ALTER TABLE` statement which can be executed against the database.
    type AlterTable<'a>: AlterTable<Error = Self::Error>
    where
        Self: 'a;

    /// A `SELECT` query which can be executed against the database.
    type Select<'a>: Select<'a, Error = Self::Error>
    where
        Self: 'a;

    /// An `INSERT` statement which can be executed against the database.
    type Insert<'a, N: Length>: Insert<N, Error = Self::Error>
    where
        Self: 'a;

    /// An `UPDATE` statement which can be executed against the database.
    type Update<'a>: Update<'a, Error = Self::Error>
    where
        Self: 'a;

    /// Create a new database and connect to it.
    async fn create_db(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Drop the named database.
    async fn drop_db(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Start a `CREATE TABLE` statement.
    ///
    /// `table` and `columns` describe the name and the basic structure of the table. More
    /// fine-grained control over the table (such as adding constraints) is available via the
    /// methods on the [`CreateTable`] object.
    fn create_table<'a, N: Length>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<SchemaColumn<'a>, N>,
    ) -> Self::CreateTable<'a, N>;

    /// Start an `ALTER TABLE` statement.
    ///
    /// The statement will affect `table`. Actions to perform on the table can be specified using
    /// the methods on the [`AlterTable`] object before executing the statement.
    fn alter_table<'a>(&'a self, table: impl Into<Cow<'a, str>> + Send) -> Self::AlterTable<'a>;

    /// Start a `SELECT` query.
    ///
    /// `columns` indicates the columns to include in the query results. The resulting [`Select`]
    /// represents a statement of the form `SELECT columns FROM table`. The query can be refined,
    /// for example by adding a `WHERE` clause, using the approriate methods on the [`Select`]
    /// object before running it.
    fn select<'a>(
        &'a self,
        columns: &'a [SelectColumn<'a>],
        table: impl Into<Cow<'a, str>> + Send,
    ) -> Self::Select<'a>;

    /// Start an `INSERT` query.
    ///
    /// `table` indicates the table to insert into and `columns` the names of the columns in that
    /// table into which values should be inserted.
    fn insert<'a, C, N>(
        &'a self,
        table: impl Into<Cow<'a, str>> + Send,
        columns: Array<C, N>,
    ) -> Self::Insert<'a, N>
    where
        C: Into<String>,
        N: Length;

    /// Start an `UPDATE` statement.
    ///
    /// `table indicates the table to update. You can set the values of columns and refine the
    /// statement with a `WHERE` clause and such using the methods on the [`Update`] object.
    fn update<'a>(&'a self, table: impl Into<Cow<'a, str>> + Send) -> Self::Update<'a>;
}

/// A SQL primitive data type.
#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Type {
    #[display(fmt = "text")]
    Text,
    #[display(fmt = "int4")]
    Int4,
    #[display(fmt = "int8")]
    Int8,
    #[display(fmt = "uint4")]
    UInt4,
    #[display(fmt = "uint8")]
    UInt8,
    #[display(fmt = "serial")]
    Serial,
}

/// A primitive value supported by a SQL database.
#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, From)]
pub enum Value {
    /// A text string.
    #[display(fmt = "{}", _0)]
    Text(String),
    /// A 4-byte signed integer.
    #[display(fmt = "{}", _0)]
    Int4(i32),
    /// An 8-byte signed integer.
    #[display(fmt = "{}", _0)]
    Int8(i64),
    /// A 4-byte unsigned integer.
    #[display(fmt = "{}", _0)]
    UInt4(u32),
    /// An 8-byte unsigned integer.
    #[display(fmt = "{}", _0)]
    UInt8(u64),
    /// An auto-incrementing integer.
    #[display(fmt = "{}", _0)]
    #[from(ignore)]
    Serial(u32),
}

impl TryFrom<Value> for String {
    type Error = String;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Text(s) => Ok(s),
            v => Err(format!("type mismatch (expected string, got {})", v.ty())),
        }
    }
}

macro_rules! try_from_int_value {
    ($t:ty) => {
        impl TryFrom<Value> for $t {
            type Error = String;

            fn try_from(v: Value) -> Result<Self, Self::Error> {
                match v {
                    Value::Int4(x) => x
                        .try_into()
                        .map_err(|err| format!("out of range for type {}: {err}", stringify!($t))),
                    Value::Int8(x) => x
                        .try_into()
                        .map_err(|err| format!("out of range for type {}: {err}", stringify!($t))),
                    Value::UInt4(x) => x
                        .try_into()
                        .map_err(|err| format!("out of range for type {}: {err}", stringify!($t))),
                    Value::UInt8(x) => x
                        .try_into()
                        .map_err(|err| format!("out of range for type {}: {err}", stringify!($t))),
                    _ => Err(format!(
                        "type mismatch (expected {}, got {})",
                        stringify!($t),
                        v.ty()
                    )),
                }
            }
        }
    };
}

try_from_int_value!(i32);
try_from_int_value!(i64);
try_from_int_value!(u32);
try_from_int_value!(u64);

impl Value {
    pub fn ty(&self) -> Type {
        match self {
            Self::Text(_) => Type::Text,
            Self::Int4(_) => Type::Int4,
            Self::Int8(_) => Type::Int8,
            Self::UInt4(_) => Type::UInt4,
            Self::UInt8(_) => Type::UInt8,
            Self::Serial(_) => Type::Serial,
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Text(s.into())
    }
}

/// An identifier of a column in a SQL query.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Column<'a> {
    table: Option<Cow<'a, str>>,
    name: Cow<'a, str>,
}

impl<'a> Column<'a> {
    /// A named column.
    pub fn named(name: impl Into<Cow<'a, str>>) -> Self {
        Self {
            name: name.into(),
            table: None,
        }
    }

    /// A named column, qualified by a table name.
    pub fn qualified(table: impl Into<Cow<'a, str>>, name: impl Into<Cow<'a, str>>) -> Self {
        Self {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    /// Escape this column name for interpolation into a SQL query.
    pub fn escape(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", escape_ident(table), escape_ident(&self.name)),
            None => escape_ident(&self.name),
        }
    }
}

impl<'a> Display for Column<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{table}.")?;
        }
        write!(f, "{}", self.name)
    }
}

impl<'a> From<Cow<'a, str>> for Column<'a> {
    fn from(name: Cow<'a, str>) -> Self {
        Self::named(name)
    }
}

impl<'a> From<&'a str> for Column<'a> {
    fn from(name: &'a str) -> Self {
        Self::named(name)
    }
}

impl<'a> From<String> for Column<'a> {
    fn from(name: String) -> Self {
        Self::named(name)
    }
}

/// A clause modifying a SQL statement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, From, TryInto)]
pub enum Clause<'a> {
    /// A `WHERE` clause.
    Where(WhereClause<'a>),
    /// A `JOIN` clause.
    Join(JoinClause<'a>),
}

/// A `WHERE` clause.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WhereClause<'a> {
    /// A `WHERE` clause which holds on any row where all of the sub-clauses hold.
    All(Box<Vec<WhereClause<'a>>>),
    /// A `WHERE` clause which holds on any row where any of the sub-clauses hold.
    Any(Box<Vec<WhereClause<'a>>>),
    /// A `WHERE` clause which holds on any row where a boolean expression is true.
    Predicate(Boolean<'a>),
}

impl<'a> From<Boolean<'a>> for WhereClause<'a> {
    fn from(b: Boolean<'a>) -> Self {
        Self::Predicate(b)
    }
}

impl<'a> WhereClause<'a> {
    /// A `WHERE` clause which holds on any row where all of the sub-clauses hold.
    pub fn all<I>(clauses: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<WhereClause<'a>>,
    {
        let mut clauses = clauses
            .into_iter()
            .map(|clause| clause.into())
            .collect::<Vec<_>>();
        if clauses.len() == 1 {
            clauses.remove(0)
        } else {
            Self::All(Box::new(clauses))
        }
    }

    /// A `WHERE` clause which holds on any row where any of the sub-clauses hold.
    pub fn any<I>(clauses: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<WhereClause<'a>>,
    {
        let mut clauses = clauses
            .into_iter()
            .map(|clause| clause.into())
            .collect::<Vec<_>>();
        if clauses.len() == 1 {
            clauses.remove(0)
        } else {
            Self::Any(Box::new(clauses))
        }
    }
}

/// A boolean expression in a `WHERE` clause.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Boolean<'a> {
    Cmp {
        /// The column to filter.
        column: Column<'a>,
        /// The operation used to filter values of `column`.
        op: Cow<'a, str>,
        /// Parameter to `op`.
        param: Value,
    },
    OneOf {
        /// The column to filter.
        column: Column<'a>,
        /// Values to match `column` against.
        params: Vec<Value>,
    },
}

impl<'a> Boolean<'a> {
    /// A boolean expression which compares the value of a column to a constant.
    pub fn cmp(
        column: impl Into<Column<'a>>,
        op: impl Into<Cow<'a, str>>,
        param: impl Into<Value>,
    ) -> Self {
        Self::Cmp {
            column: column.into(),
            op: op.into(),
            param: param.into(),
        }
    }

    /// A boolean expression which checks if the value of a column is one of a list of constants.
    pub fn one_of<I>(column: impl Into<Column<'a>>, params: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
    {
        Self::OneOf {
            column: column.into(),
            params: params.into_iter().map(|p| p.into()).collect(),
        }
    }
}

/// A `JOIN` clause.
#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[display(fmt = "JOIN {table} ON {lhs} {op} {rhs}")]
pub struct JoinClause<'a> {
    /// The table to join with.
    pub table: Cow<'a, str>,
    /// The LHS in the join condition.
    pub lhs: Column<'a>,
    /// The operation in the join condition.
    pub op: Cow<'a, str>,
    /// The RHS in the join condition.
    pub rhs: Column<'a>,
}

/// A constraint on a set of columns in a table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    ForeignKey { table: String },
}

/// A `CREATE TABLE` statement which can be executed against the database.
#[async_trait]
pub trait CreateTable: Send {
    /// Errors returned by this statement.
    type Error: Error;

    /// Add a constraint to the table.
    fn constraint<I>(self, kind: ConstraintKind, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>;

    /// Create the table.
    ///
    /// This will execute a statement of the form
    /// `CREATE TABLE IF NOT EXISTS table (columns constraints)`.
    ///
    /// # Errors
    ///
    /// This method will fail if any of the specified constraints were invalid.s
    async fn execute(self) -> Result<(), Self::Error>;
}

/// An extension trait for [`CreateTable`] that provides some higher-level functions.
pub trait CreateTableExt: CreateTable {
    /// Add a list of constraints to the table.
    fn constraints<I, C>(self, constraints: I) -> Self
    where
        I: IntoIterator<Item = (ConstraintKind, C)>,
        C: IntoIterator,
        C::Item: Into<String>;
}

impl<T: CreateTable> CreateTableExt for T {
    fn constraints<I, C>(mut self, constraints: I) -> Self
    where
        I: IntoIterator<Item = (ConstraintKind, C)>,
        C: IntoIterator,
        C::Item: Into<String>,
    {
        for (kind, columns) in constraints {
            self = self.constraint(kind, columns);
        }
        self
    }
}

/// An `ALTER TABLE` statement which can be executed against the database.
#[async_trait]
pub trait AlterTable: Send {
    /// Errors returned by this statement.
    type Error: Error;

    /// Add a constraint to the table.
    fn add_constraint<I>(self, kind: ConstraintKind, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>;

    /// Do the table alteration.
    ///
    /// This will execute a statement of the form `ALTER TABLE table actions...`.
    ///
    /// # Errors
    ///
    /// This method will fail if the table to alter does not exist or if any of the specified
    /// actions were invalid.
    async fn execute(self) -> Result<(), Self::Error>;
}

/// An extension trait for [`AlterTable`] that provides some higher-level functions.
pub trait AlterTableExt: AlterTable {
    /// Add a list of constraints to the table.
    fn add_constraints<I, C>(self, constraints: I) -> Self
    where
        I: IntoIterator<Item = (ConstraintKind, C)>,
        C: IntoIterator,
        C::Item: Into<String>;
}

impl<T: AlterTable> AlterTableExt for T {
    fn add_constraints<I, C>(mut self, constraints: I) -> Self
    where
        I: IntoIterator<Item = (ConstraintKind, C)>,
        C: IntoIterator,
        C::Item: Into<String>,
    {
        for (kind, columns) in constraints {
            self = self.add_constraint(kind, columns);
        }
        self
    }
}

/// A `SELECT` query which can be executed against the database.
pub trait Select<'a>: Send {
    /// Errors returned by this query.
    type Error: Error;
    /// Rows returned by this query.
    type Row: Row<Error = Self::Error>;
    /// An asynchronous stream of rows.
    type Stream: Stream<Item = Result<Self::Row, Self::Error>> + Unpin + Send;

    /// Add a clause to the query.
    fn clause(self, clause: Clause<'a>) -> Self;

    /// Run the query and get a stream of results.
    fn stream(self) -> Self::Stream;
}

/// An extension trait for [`Select`] that provides some higher-level functions.
#[async_trait]
pub trait SelectExt<'a>: Select<'a> {
    /// Add a `WHERE` clause to the query.
    fn filter(self, clause: impl Into<WhereClause<'a>>) -> Self;

    /// Add a `WHERE` clause based on a column comparison.
    fn cmp(self, column: impl Into<Column<'a>>, op: impl Into<Cow<'a, str>>, param: Value) -> Self;

    /// Add a `WHERE` clause based on an `IN` operator.
    fn one_of(self, column: impl Into<Column<'a>>, params: impl IntoIterator<Item = Value>)
        -> Self;

    /// Add a `JOIN` clause to the query.
    fn join(
        self,
        table: impl Into<Cow<'a, str>>,
        lhs: impl Into<Column<'a>>,
        op: impl Into<Cow<'a, str>>,
        rhs: impl Into<Column<'a>>,
    ) -> Self;

    /// Add multiple clauses to the query.
    fn clauses<I>(self, clauses: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Clause<'a>>;

    /// Run a query which is expected to return a single row.
    ///
    /// # Errors
    ///
    /// This method will fail if the query does not return exactly one row.
    async fn one(self) -> Result<Self::Row, Self::Error>;

    /// Run a query and collect the results.
    async fn many(self) -> Result<Vec<Self::Row>, Self::Error>;

    /// Run a query which is expected to return either 0 or 1 rows.
    ///
    /// # Errors
    ///
    /// This method will fail if the query does not return exactly 0 or 1 rows.
    async fn opt(self) -> Result<Option<Self::Row>, Self::Error>;
}

#[async_trait]
impl<'a, T: Select<'a>> SelectExt<'a> for T {
    fn filter(self, clause: impl Into<WhereClause<'a>>) -> Self {
        self.clause(clause.into().into())
    }

    fn cmp(self, column: impl Into<Column<'a>>, op: impl Into<Cow<'a, str>>, param: Value) -> Self {
        self.filter(Boolean::Cmp {
            column: column.into(),
            op: op.into(),
            param,
        })
    }

    fn one_of(
        self,
        column: impl Into<Column<'a>>,
        params: impl IntoIterator<Item = Value>,
    ) -> Self {
        self.filter(Boolean::OneOf {
            column: column.into(),
            params: params.into_iter().collect(),
        })
    }

    fn join(
        self,
        table: impl Into<Cow<'a, str>>,
        lhs: impl Into<Column<'a>>,
        op: impl Into<Cow<'a, str>>,
        rhs: impl Into<Column<'a>>,
    ) -> Self {
        self.clause(Clause::Join(JoinClause {
            table: table.into(),
            lhs: lhs.into(),
            op: op.into(),
            rhs: rhs.into(),
        }))
    }

    fn clauses<I>(mut self, clauses: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Clause<'a>>,
    {
        for clause in clauses {
            self = self.clause(clause.into());
        }
        self
    }

    async fn opt(self) -> Result<Option<Self::Row>, Self::Error> {
        let mut rows = self.stream();
        let Some(row) = rows.next().await else { return Ok(None); };
        if rows.next().await.is_some() {
            return Err(Self::Error::too_many_rows(1));
        }
        row.map(Some)
    }

    async fn one(self) -> Result<Self::Row, Self::Error> {
        self.opt().await?.ok_or_else(Self::Error::empty_rows)
    }

    async fn many(self) -> Result<Vec<Self::Row>, Self::Error> {
        self.stream().try_collect().await
    }
}

/// An `INSERT` statement which can be executed against the database.
///
/// The parameter `N` indicates the number of columns in each row to be inserted.
#[async_trait]
pub trait Insert<N: Length>: Send {
    /// Errors returned by this statement.
    type Error: Error;

    /// Add rows to insert.
    fn rows<R>(self, rows: R) -> Self
    where
        R: IntoIterator<Item = Array<Value, N>>;

    /// Do the insertion.
    ///
    /// This will execute a statement of the form `INSERT INTO table (columns) VALUES (rows)`.
    ///
    /// # Errors
    ///
    /// This method will fail if any of the items in `rows` conflict with an existing row in `table`
    /// at a column which is defined as a unique or primary key.
    async fn execute(self) -> Result<(), Self::Error>;
}

/// A table or view which can be referenced in the `FROM` clause of another query.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FromItem<'a> {
    /// A reference to a table or view, renaming the table and its columns for the purposes of this
    /// query.
    Alias {
        table: Cow<'a, str>,
        columns: Vec<Cow<'a, str>>,
        item: Box<FromItem<'a>>,
    },
    /// A `VALUES` expression, listing rows of literal values to treat as a temporary table.
    Values { rows: Vec<Vec<Value>> },
}

impl<'a> FromItem<'a> {
    /// A reference to a table or view, renaming the table and its columns for the purposes of this
    /// query.
    pub fn alias<C>(table: impl Into<Cow<'a, str>>, columns: C, item: FromItem<'a>) -> Self
    where
        C: IntoIterator,
        C::Item: Into<Cow<'a, str>>,
    {
        Self::Alias {
            table: table.into(),
            columns: columns.into_iter().map(|c| c.into()).collect(),
            item: Box::new(item),
        }
    }

    /// A `VALUES` expression, listing rows of literal values to treat as a temporary table.
    pub fn values<I>(rows: I) -> Self
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = Value>,
    {
        Self::Values {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().collect())
                .collect(),
        }
    }
}

/// An `UPDATE` statement which can be executed against the database.
#[async_trait]
pub trait Update<'a>: Send {
    /// Errors returned by this statement.
    type Error: Error;

    /// Set the value of a column in this table to the value of another column mentioned in the
    /// statement.
    fn set(self, set: impl Into<Cow<'a, str>>, to: Column<'a>) -> Self;

    /// Add a `WHERE` clause to the statement.
    ///
    /// `lhs` and `rhs` can reference any column on the table being updated or in a table joined
    /// into the query via a [`from`](Self::from) clause. The statement will only apply to rows
    /// where `lhs` and `rhs` satisfy the comparison operator `op`.
    fn filter(self, lhs: Column<'a>, op: impl Into<Cow<'a, str>>, rhs: Column<'a>) -> Self;

    /// Join another table or view into the query.
    ///
    /// `item` indicates a table to include in the operation in addition to the table being updated.
    /// This auxiliary table can be referenced in [`set`](Self::set) and [`filter`](Self::filter),
    /// so that you can use it to compute new values for columns in the main table or to filter the
    /// rows which should be updated.
    fn from(self, item: FromItem<'a>) -> Self;

    /// Do the update.
    ///
    /// This will execute a statement of the form
    /// `UPDATE table SET set = to WHERE filter FROM from`.
    async fn execute(self) -> Result<(), Self::Error>;
}

/// A row in a database table.
pub trait Row: Sized + Send {
    /// Errors returned by row operations.
    type Error: Error;

    /// Get the value of `column` in this row.
    ///
    /// `column` is an index corresponding to the order in which columns were requested in the
    /// `SELECT` statement.
    ///
    /// # Errors
    ///
    /// This method will fail if the specified column does not exist.
    fn column(&self, column: usize) -> Result<Value, Self::Error>;
}

/// Escape an identifier (table name, column name, etc.) for inclusion in a SQL query.
pub fn escape_ident(s: impl AsRef<str>) -> String {
    format!("\"{}\"", s.as_ref().replace('"', "\"\""))
}
