//! Common items that you will always want in scope when using GraphQL.

pub use crate::graphql::{
    async_graphql::{self, value},
    type_system::{BelongsTo, Id, Many, Resource, Scalar, Type},
    EmptyMutation, EmptySubscription, Object, Query, Schema,
};
