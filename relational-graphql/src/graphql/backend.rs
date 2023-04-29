//! Interfaces provided by a backend data source consumed by a GraphQL API.
//!
//! The entrypoint to this system of traits is [`DataSource`], which describes the interface by
//! which the GraphQL API interacts with the backend data provider. This is, in particular, the glue
//! between the GraphQL and SQL views of the data model, since the SQL model will implement
//! [`DataSource`] and the GraphQL layer will interact with the SQL layer exclusively through this
//! trait.
//!
//! A number of supporting traits are defined here which can be accessed through the [`DataSource`]
//! trait by means of its associated types.

use super::{
    connection::{self, CursorType},
    type_system::{Relation, Resource, Type},
    EmptyFields, ObjectType, OutputType,
};
use async_trait::async_trait;
use std::error::Error;

/// And edge in a connection, connecting the owner of the connection to another object.
///
/// The edge has
/// * a cursor, which can be used for pagination, of type `C`
/// * a node, of resource type `T`
/// * additional fields of type `E`
#[derive(Clone, Copy, Debug, Default)]
pub struct Edge<C, T, E> {
    cursor: C,
    node: T,
    fields: E,
}

impl<C, T, E> Edge<C, T, E> {
    /// Construct an edge with the given cursor, node, and additional data.
    pub fn with_additional_fields(cursor: C, node: T, fields: E) -> Self {
        Self {
            cursor,
            node,
            fields,
        }
    }

    /// Get the cursor indicating this edge's position in the connection.
    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    /// Get the object this edge connects to.
    pub fn into_node(self) -> T {
        self.node
    }
}

impl<C: CursorType + Send + Sync, T: OutputType, E: ObjectType> From<Edge<C, T, E>>
    for connection::Edge<C, T, E>
{
    fn from(edge: Edge<C, T, E>) -> Self {
        Self::with_additional_fields(edge.cursor, edge.node, edge.fields)
    }
}

/// A Relay-style paginated connection to a collection of objects.
///
/// The objects in the collection are of type `T`. Each object in the collection also represents a
/// relationship, or _edge_, between the object which owns the collection and the object in the
/// collection. These edges may have additional fields of type `E`, beyond the fields specified by
/// Relay. The connection itself may also have additional fields of type `C`, beyond the fields
/// specified by Relay.
pub trait Connection<C> {
    /// An index into this collection.
    type Cursor: CursorType + Send + Sync;

    /// An empty connection.
    fn empty(fields: C) -> Self;

    /// Are there more objects after `cursor`?
    fn has_next(&self, cursor: &Self::Cursor) -> bool;
    /// Are there more objects before `cursor`?
    fn has_previous(&self, cursor: &Self::Cursor) -> bool;

    /// Get the additional connection-level fields.
    fn into_fields(self) -> C;
}

/// A source of data which can be served by the GraphQL API.
#[async_trait]
pub trait DataSource {
    /// A Relay-style paginated connection to a collection of objects.
    ///
    /// THe objects in the collection are of type `T`. Each object in the collection also represents
    /// a relationship, or _edge_, between the object which owns the collection and the object in
    /// the collection. These edges may have additional fields of type `E`, beyond the fields
    /// specified by Relay. The connection itself may also have additional fields of type `C`,
    /// beyond the fields specified by Relay.
    type Connection<T: Type, C: ObjectType, E: ObjectType>: Connection<C>;
    /// Errors reported while attempting to load data.
    type Error: Error;

    /// Load the targets of a [`Relation`].
    async fn load_relation<R: Relation>(
        &self,
        owner: &R::Owner,
        filter: Option<<R::Target as Type>::Predicate>,
    ) -> Result<Paginated<Self, R::Target>, Self::Error>;

    /// Load a page from a paginated connection.
    async fn load_page<T: Type, C: ObjectType, E: Clone + ObjectType>(
        &self,
        conn: &Paginated<Self, T, C, E>,
        page: PageRequest<Cursor<Self, T, C, E>>,
    ) -> Result<Vec<Edge<Cursor<Self, T, C, E>, T, E>>, Self::Error>;

    /// Register the resource `T` in the schema.
    async fn register<T: Resource>(&mut self) -> Result<(), Self::Error>;

    /// Get a paginated stream of items matching `filter`.
    async fn query<T: Resource>(
        &self,
        filter: Option<T::ResourcePredicate>,
    ) -> Result<Paginated<Self, T>, Self::Error>;

    /// Insert new items into the database.
    async fn insert<T: Resource, I>(&mut self, inputs: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = T::ResourceInput> + Send,
        I::IntoIter: Send;
}

/// A specification of a page to load in a paginated connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageRequest<Cursor> {
    /// Limit the results to the first N items that otherwise match the request.
    pub first: Option<usize>,
    /// Start the page at the first item after that indicated by this cursor.
    pub after: Option<Cursor>,
    /// Limit the results to the last N items that otherwise match the request.
    pub last: Option<usize>,
    /// Start the page at the first item before that indicated by this cursor.
    pub before: Option<Cursor>,
}

/// A paginated list of objects.
pub type Paginated<D, T, C = EmptyFields, E = EmptyFields> = <D as DataSource>::Connection<T, C, E>;

/// An index into a [`Paginated`] list of objects.
pub type Cursor<D, T, C = EmptyFields, E = EmptyFields> =
    <Paginated<D, T, C, E> as Connection<C>>::Cursor;

pub mod default {
    #![doc(hidden)]
    //! Default backend type for derived GraphQL schemas.
    //!
    //! The monomorphic nature of GraphQL's type system makes it difficult to parameterize a GraphQL
    //! schema on a backend type. Still, we would like at least some choice in the type of backend
    //! we use when constructing a schema. As a workaround, schemas generated via the derive macros
    //! [`Resource`](macro@crate::prelude::Resource) and [`Query`](macro@crate::prelude::Query) use
    //! a type alias named `GraphQLBackend` as the backend in their resolvers. This module exists so
    //! that the generated code can glob import it, bringing the default [`GraphQLBackend`] into
    //! scope. Since this type alias is brought into scope via a glob import, it can be overridden
    //! if the module where the schema is defined explicitly defines (or imports) its own
    //! `GraphQLBackend` alias, such as with the [`use_backend`](crate::use_backend) macro.

    /// The default [`DataSource`](super::DataSource) used as a backend for GraphQL APIs.
    pub type GraphQLBackend = crate::sql::PostgresDataSource;
}

/// Use a certain [`DataSource`] implementation as the backend in a GraphQL schema.
///
/// By default, GraphQL schemas use [`PostgresDataSource`](crate::sql::PostgresDataSource) as their
/// backend. To use a different backend, use this macro in the module where the schema is defined,
/// as in:
///
/// ```
/// # mod example {
/// use relational_graphql::prelude::*;
///
/// relational_graphql::use_backend!(relational_graphql::sql::PostgresDataSource);
///
/// #[derive(Clone, Resource)]
/// struct MyResource {
///     id: Id,
///     name: String,
/// }
///
/// // Executing queries of this type will use the backend defined above to solve the queries.
/// #[derive(Query)]
/// #[query(resource(my_resources: MyResource))]
/// struct Query;
/// # }
/// ```
#[macro_export]
macro_rules! use_backend {
    ($backend:ty) => {
        type GraphQLBackend = $backend;
    };
}
