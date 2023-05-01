//! Instantiation of a GraphQL [`DataSource`](gql::DataSource) for a SQL database.

use super::{db, ops};
use crate::graphql::{
    backend::{self as gql, Edge, PageRequest, Paginated},
    type_system::{Relation, Resource, Type},
    EmptyFields, ObjectType,
};
use async_trait::async_trait;
use derive_more::From;
use std::cmp::min;
use std::fmt::Debug;

#[cfg(feature = "postgres")]
/// A data source implemented using a PostgreSQL database.
pub type PostgresDataSource = SqlDataSource<db::postgres::Connection>;

/// A data source implemented using a SQL database.
#[derive(Clone, Debug, From)]
pub struct SqlDataSource<Db>(Db);

impl<Db: db::Connection> SqlDataSource<Db> {
    /// The underlying connection to the database.
    pub fn inner(&self) -> &Db {
        &self.0
    }

    /// Unwrap this data source to get at the underlying connection.
    pub fn into_inner(self) -> Db {
        self.0
    }
}

#[async_trait]
impl<Db: 'static + db::Connection + Send + Sync> gql::DataSource for SqlDataSource<Db> {
    type Connection<T: Type, C: ObjectType, E: ObjectType> = SqlConnection<T, C, E>;
    type Error = ops::Error;

    async fn load_relation<R: Relation>(
        &self,
        owner: &R::Owner,
        filter: Option<<R::Target as Type>::Predicate>,
    ) -> Result<Paginated<Self, R::Target>, Self::Error> {
        let objects = ops::select::load_relation::<Db, R>(&self.0, owner, filter).await?;
        Ok(SqlConnection::new(objects))
    }

    async fn load_page<T: Type, C: ObjectType, E: Clone + ObjectType>(
        &self,
        conn: &Paginated<Self, T, C, E>,
        page: PageRequest<usize>,
    ) -> Result<Vec<Edge<usize, T, E>>, Self::Error> {
        Ok(conn.load(page))
    }

    async fn register<T: Resource>(&mut self) -> Result<(), Self::Error> {
        ops::register::execute::<_, T>(&self.0).await
    }

    async fn query<T: Resource>(
        &self,
        filter: Option<T::ResourcePredicate>,
    ) -> Result<Paginated<Self, T>, Self::Error> {
        let objects = ops::select::execute(&self.0, filter).await?;
        Ok(SqlConnection::new(objects))
    }

    async fn insert<T: Resource, I>(&mut self, inputs: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = T::ResourceInput> + Send,
        I::IntoIter: Send,
    {
        ops::insert::execute::<Db, T>(&self.0, inputs).await
    }
}

/// A paginated connection to a set of rows.
#[derive(Clone, Debug)]
pub struct SqlConnection<T: Type, C, E: ObjectType> {
    fields: C,
    // For now we just keep all items in the connection in memory. Later we will add pagination.
    edges: Vec<SqlEdge<T, E>>,
}

impl<T: Type> SqlConnection<T, EmptyFields, EmptyFields> {
    fn new<I>(objects: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self {
            fields: EmptyFields,
            edges: objects
                .into_iter()
                .map(|obj| SqlEdge {
                    node: obj,
                    fields: EmptyFields,
                })
                .collect(),
        }
    }
}

impl<T: Type, C, E: ObjectType> gql::Connection<C> for SqlConnection<T, C, E> {
    type Cursor = usize;

    fn empty(fields: C) -> Self {
        Self {
            fields,
            edges: Default::default(),
        }
    }

    fn has_next(&self, cursor: &usize) -> bool {
        *cursor + 1 < self.edges.len()
    }

    fn has_previous(&self, cursor: &usize) -> bool {
        *cursor > 0
    }

    fn into_fields(self) -> C {
        self.fields
    }
}

impl<T: Type, C, E: Clone + ObjectType> SqlConnection<T, C, E> {
    /// Load a page from a paginated connection.
    pub fn load(&self, page: PageRequest<usize>) -> Vec<Edge<usize, T, E>> {
        let after = min(page.after.unwrap_or(0), self.edges.len());
        let before = min(page.before.unwrap_or(self.edges.len()), self.edges.len());
        let edges = &self.edges[after..before];

        let first = min(page.first.unwrap_or(edges.len()), edges.len());
        let edges = &edges[..first];

        let last = min(edges.len() - page.last.unwrap_or(edges.len()), edges.len());
        let edges = &edges[last..];

        let offset = after + last;
        edges
            .iter()
            .enumerate()
            .map(|(i, edge)| {
                Edge::with_additional_fields(offset + i, edge.node.clone(), edge.fields.clone())
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct SqlEdge<T, E> {
    node: T,
    fields: E,
}
