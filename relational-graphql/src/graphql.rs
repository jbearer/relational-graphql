//! A general interface for defining relational GraphQL applications.

pub mod backend;
pub mod type_system;

// Re-export commonly used `async_graphql` types.
pub use async_graphql::{
    connection, value, Context, EmptyMutation, EmptySubscription, InputObject, InputType, Object,
    ObjectType, OneofObject, OutputType, Result, Schema, SimpleObject,
};
pub use relational_graphql_derive::Query;

// Re-export `async_graphql` directly as an escape hatch.
pub extern crate async_graphql;

/// Placeholder for connection objects (connections or edges) which have no additional fields.
//
// Note: async_graphql defines its own [`EmptyFields`](async_graphql::connection::EmptyFields)
// struct, but inconveniently, it does not implement [`Clone`], so we use our own version.
#[derive(Clone, Copy, Debug, SimpleObject)]
#[graphql(fake)]
pub struct EmptyFields;
