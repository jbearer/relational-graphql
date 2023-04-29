//! Derive macros for the GraphQL API.

use proc_macro2::{Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::Ident;

pub mod query;
pub mod resource;

/// The path of the `relational_graphql::graphql` module in the scope invoking a procedural macro.
fn graphql_path() -> TokenStream {
    let crate_name = match crate_name("relational-graphql").unwrap() {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(#ident)
        }
    };
    quote!(#crate_name::graphql)
}
