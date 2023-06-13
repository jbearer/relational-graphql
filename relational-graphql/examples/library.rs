use clap::Parser;
use relational_graphql::{
    graphql::backend::DataSource,
    prelude::*,
    sql::{
        db::{postgres, temp::TempDatabase},
        SqlDataSource,
    },
};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////
// Schema
//

relational_graphql::use_backend!(SqlDataSource<TempDatabase<postgres::Connection>>);

#[derive(Clone, Debug, Resource)]
pub struct Author {
    id: Id,
    #[resource(searchable)]
    name: String,
    born: u32,
    books: BelongsTo<Book>,
}

#[derive(Clone, Debug, Resource)]
pub struct Book {
    id: Id,
    #[resource(searchable)]
    title: String,
    #[resource(searchable)]
    author: Author,
    pages: BelongsTo<Page>,
}

#[derive(Clone, Debug, Resource)]
pub struct Page {
    id: Id,
    number: u32,
    book: Book,
    #[resource(searchable)]
    text: String,
}

#[derive(Clone, Debug, Query)]
#[query(resource(authors: Author))]
#[query(resource(books: Book))]
#[query(resource(pages: Page))]
pub struct Query;

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test data
//

#[derive(Clone, Debug, Parser)]
pub struct Options {
    #[clap(
        long,
        env = "EXAMPLE_POSTGRES_URL",
        default_value = "http://localhost:5432"
    )]
    db_url: Url,
    #[clap(long, env = "EXAMPLE_POSTGRES_USER", default_value = "postgres")]
    db_user: String,
    #[clap(long, env = "EXAMPLE_POSTGRES_PASSWORD", default_value = "password")]
    db_password: String,
}

pub async fn schema(opt: Options) -> Schema<Query, EmptyMutation, EmptySubscription> {
    Schema::build(Query, EmptyMutation, EmptySubscription)
        .data(create_db(opt).await)
        .finish()
}

pub async fn create_db(opt: Options) -> GraphQLBackend {
    // Create and connect to database.
    let mut config = postgres::Config::default();
    config
        .user(&opt.db_user)
        .password(&opt.db_password)
        .host(opt.db_url.host().unwrap().to_string().as_str());
    if let Some(port) = opt.db_url.port() {
        config.port(port);
    }
    let conn = postgres::Connection::new(config).await.unwrap();
    let mut db = SqlDataSource::from(TempDatabase::new(conn).await.unwrap());

    // Create tables for our data model.
    Query::register(&mut db).await.unwrap();

    // Insert some test data.
    db.insert::<Author, _>([
        author::AuthorInput {
            name: "Jeff".into(),
            born: 1975,
        },
        author::AuthorInput {
            name: "Annie".into(),
            born: 2000,
        },
        author::AuthorInput {
            name: "Abed".into(),
            born: 2001,
        },
    ])
    .await
    .unwrap();
    db.insert::<Book, _>([
        book::BookInput {
            title: "History of Something".into(),
            author: 1, // Jeff
        },
        book::BookInput {
            title: "Principles of Intermediate".into(),
            author: 2, // Annie
        },
        book::BookInput {
            title: "Studyology".into(),
            author: 2, // Annie
        },
        book::BookInput {
            title: "Learning!".into(),
            author: 3, // Abed
        },
    ])
    .await
    .unwrap();
    let dummy_keywords = ["principle", "historical", "learns", "studies"];
    db.insert::<Page, _>(dummy_keywords.into_iter().enumerate().flat_map(|(i, kw)| {
        let book = (i + 1) as i32;
        (0..10 * book).map(move |page| page::PageInput {
            book,
            number: page as u32 + 1,
            text: format!("Sample text about {kw}"),
        })
    }))
    .await
    .unwrap();

    db
}

#[async_std::main]
async fn main() {
    let opt = Options::parse();
    println!("{}", schema(opt).await.sdl());
}

#[cfg(test)]
mod test {
    use super::*;
    use relational_graphql::init_logging;
    use std::env;

    macro_rules! test_options {
        () => {{
            if env::var("POSTGRES_TESTS").is_err() {
                tracing::warn!("skipping postgres test since POSTGRES_TESTS are not enabled");
                return;
            }

            let port = env::var("POSTGRES_TESTS_PORT")
                .map(|port| port.parse().unwrap())
                .unwrap_or(5432);
            let password = env::var("POSTGRES_TESTS_PASSWORD").unwrap_or("password".to_string());

            Options {
                db_url: format!("http://localhost:{port}").parse().unwrap(),
                db_user: "postgres".into(),
                db_password: password,
            }
        }};
    }

    #[async_std::test]
    async fn test_many_to_one_query() {
        init_logging();
        let schema = schema(test_options!()).await;

        // Query a many-to-one relation: get all authors born after 2000 and the titles of books
        // they've written.
        let young_authors = schema
            .execute(
                r#"query {
                    authors(where: {
                        has: {
                            born: {
                                cmp: {
                                    op: GE
                                    value: { lit : 2000 }
                                }
                            }
                        }
                    }) {
                        edges {
                            node {
                                name
                                books {
                                    edges {
                                        node {
                                            title
                                        }
                                    }
                                }
                            }
                        }
                    }
                }"#,
            )
            .await
            .into_result()
            .unwrap();
        assert_eq!(
            young_authors.data,
            value!({
                "authors": {
                    "edges": [
                        {
                            "node": {
                                "name": "Annie",
                                "books": {
                                    "edges": [
                                        {
                                            "node": {
                                                "title": "Principles of Intermediate"
                                            },
                                        },
                                        {
                                            "node": {
                                                "title": "Studyology"
                                            },
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "node": {
                                "name": "Abed",
                                "books": {
                                    "edges": [
                                        {
                                            "node": {
                                                "title": "Learning!"
                                            },
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            })
        );
    }

    #[ignore] // relational queries are not implemented yet
    #[async_std::test]
    async fn test_relational_query() {
        init_logging();
        let schema = schema(test_options!()).await;

        // Filter by the contents of a relation: get all books with at least 30 pages.
        let long_books = schema
            .execute(
                r#"query {
                    books(where: {
                        has: {
                            pages: {
                                any: {
                                    has: {
                                        number: {
                                            cmp: { op: GE, value: { lit: 30 } }
                                        }
                                    }
                                }
                            }
                        }
                    }) {
                        edges {
                            node {
                                title
                            }
                        }
                    }
                }"#,
            )
            .await
            .into_result()
            .unwrap();
        assert_eq!(
            long_books.data,
            value!({
                "books": {
                    "edges": [
                        {
                            "node": {
                                "title": "Studyology"
                            }
                        },
                        {
                            "node": {
                                "title": "Learning!"
                            }
                        }
                    ]
                }
            })
        );
    }

    #[async_std::test]
    async fn test_search() {
        init_logging();
        let schema = schema(test_options!()).await;

        // Search will return an inexact match.
        let matching_books = schema
            .execute(
                r#"query {
                    books(where: {
                        matches: "histories"
                    }) {
                        edges {
                            node {
                                title
                            }
                        }
                    }
                }"#,
            )
            .await
            .into_result()
            .unwrap();
        assert_eq!(
            matching_books.data,
            value!({
                "books": {
                    "edges": [
                        {
                            "node": {
                                "title": "History of Something"
                            }
                        },
                    ]
                }
            })
        );

        // Search can find matches in nested fields, such as author.
        let matching_books = schema
            .execute(
                r#"query {
                    books(where: {
                        matches: "Abed"
                    }) {
                        edges {
                            node {
                                title
                            }
                        }
                    }
                }"#,
            )
            .await
            .into_result()
            .unwrap();
        assert_eq!(
            matching_books.data,
            value!({
                "books": {
                    "edges": [
                        {
                            "node": {
                                "title": "Learning!"
                            }
                        },
                    ]
                }
            })
        );
    }
}
