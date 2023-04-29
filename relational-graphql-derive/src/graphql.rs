//! Derive macros for the GraphQL API.

use proc_macro2::TokenStream;
use quote::quote;

pub mod query;
pub mod resource;

/// The path of the `relational_graphql::graphql` module in the scope invoking a procedural macro.
fn graphql_path() -> TokenStream {
    quote!(relational_graphql::graphql)
}
