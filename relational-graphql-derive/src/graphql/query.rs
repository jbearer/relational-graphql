//! Derive macro for the top-level GraphQL queries over an ontology of `Resource` types.

use super::graphql_path;
use crate::helpers::{parse_docs, AttrParser};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Field, Ident};

/// Generate a GraphQL entrypoint for a query struct.
pub fn derive(
    DeriveInput {
        ident,
        generics,
        data,
        attrs,
        ..
    }: DeriveInput,
) -> TokenStream {
    if !generics.params.is_empty() {
        panic!("Query cannot be derived on generic types");
    }
    match data {
        Data::Struct(_) => generate_struct(ident, attrs),
        _ => panic!("Query can only be derived for structs"),
    }
}

fn generate_struct(name: Ident, attrs: Vec<Attribute>) -> TokenStream {
    let graphql = graphql_path();
    let p = AttrParser::new("query");

    // We will generate the impl in an anonymous module so we can bring things into scope.
    let mod_name = format_ident!("__query_object_{}", name);

    // Get the documentation from the original struct. We will attach it to the `#[Object]` impl
    // block so it shows up in the exported schema.
    let doc = parse_docs(&attrs);

    // Get the list of resources in the ontology.
    let resources = attrs
        .iter()
        .flat_map(|a| p.parse_arg_with(a, "resource", Field::parse_named))
        .collect::<Vec<_>>();

    // Generate resolvers for each resource.
    let resolvers = resources.iter().map(|resource| {
        let name = resource.ident.as_ref().unwrap();
        let ty = &resource.ty;
        let doc = format!("Search for {}.", name);
        quote! {
            #[doc = #doc]
            async fn #name(
                &self,
                ctx: &Context<'_>,
                #[graphql(name = "where")]
                filter: Option<<#ty as Type>::Predicate>,
                after: Option<String>,
                before: Option<String>,
                first: Option<i32>,
                last: Option<i32>,
            ) -> Result<Connection<Cursor<GraphQLBackend, #ty>, #ty, EmptyFields, EmptyFields>> {
                connection::query(after, before, first, last, |after, before, first, last| async move {
                    // Load the resource from the database.
                    let db = ctx.data::<GraphQLBackend>()?;
                    let resource = db.query::<#ty>(filter).await?;

                    // Load the requested page.
                    let req = PageRequest { after, before, first, last };
                    let page = db.load_page(&resource, req).await?;

                    // Convert it into a GraphQL connection.
                    let has_previous = page
                        .first()
                        .map(|edge| resource.has_previous(edge.cursor()))
                        .unwrap_or(false);
                    let has_next = page
                        .last()
                        .map(|edge| resource.has_next(edge.cursor()))
                        .unwrap_or(false);
                    let mut conn = Connection::with_additional_fields(
                        has_previous,
                        has_next,
                        resource.into_fields(),
                    );
                    conn.edges.extend(page.into_iter().map(|edge| edge.into()));

                    Ok::<_, async_graphql::Error>(conn)
                }).await
            }
        }
    });

    // Generate register statements for each resource.
    let registers = resources.iter().map(|resource| {
        let ty = &resource.ty;
        quote! {
            db.register::<#ty>().await?;
        }
    });

    quote! {
        #[allow(non_snake_case)]
        mod #mod_name {
            use super::*;
            use #graphql::{
                async_graphql, connection::{self, Connection},
                backend::{Connection as _, Cursor, DataSource, PageRequest},
                type_system::Type, Context, EmptyFields, Object, Result,
            };

            #[Object]
            #[doc = #doc]
            impl #name {
                #(#resolvers)*
            }

            impl #name {
                /// Register all resources in the ontology represented by this query object.
                pub async fn register(
                    db: &mut GraphQLBackend,
                ) -> Result<(), <GraphQLBackend as DataSource>::Error> {
                    #(#registers)*
                    Ok(())
                }
            }
        }
    }
}
