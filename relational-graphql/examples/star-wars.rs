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
pub struct Character {
    id: Id,
    name: String,
    appears_in: Many<Episode>,
    human: BelongsTo<Human>,
    droid: BelongsTo<Droid>,
}

#[derive(Clone, Debug, Resource)]
pub struct Human {
    id: Id,
    character: Character,
    home_planet: String,
}

#[derive(Clone, Debug, Resource)]
pub struct Droid {
    id: Id,
    character: Character,
    primary_function: String,
}

#[derive(Clone, Debug, Resource)]
pub struct Episode {
    id: Id,
    title: String,
    year: u32,
    hero: Character,
    #[resource(inverse(appears_in))]
    characters: Many<Character>,
}

#[derive(Clone, Debug, Query)]
#[query(resource(humans: Human))]
#[query(resource(droids: Droid))]
#[query(resource(episodes: Episode))]
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
    db.insert::<Character, _>([
        character::CharacterInput {
            name: "Luke Skywalker".into(),
        },
        character::CharacterInput {
            name: "Anakin Skywalker".into(),
        },
        character::CharacterInput {
            name: "Han Solo".into(),
        },
        character::CharacterInput {
            name: "Leia Organa".into(),
        },
        character::CharacterInput {
            name: "Owen Lars".into(),
        },
        character::CharacterInput {
            name: "Beru Lars".into(),
        },
        character::CharacterInput {
            name: "C-3PO".into(),
        },
        character::CharacterInput {
            name: "R2-D2".into(),
        },
    ])
    .await
    .unwrap();
    db.insert::<Human, _>([
        human::HumanInput {
            character: 1,
            home_planet: "Tatooine".into(),
        },
        human::HumanInput {
            character: 2,
            home_planet: "Tatooine".into(),
        },
        human::HumanInput {
            character: 3,
            home_planet: "Corellia".into(),
        },
        human::HumanInput {
            character: 4,
            home_planet: "Alderaan".into(),
        },
        human::HumanInput {
            character: 5,
            home_planet: "Tatooine".into(),
        },
        human::HumanInput {
            character: 6,
            home_planet: "Tatooine".into(),
        },
    ])
    .await
    .unwrap();
    db.insert::<Droid, _>([
        droid::DroidInput {
            character: 7,
            primary_function: "Protocol".into(),
        },
        droid::DroidInput {
            character: 8,
            primary_function: "Astromech".into(),
        },
    ])
    .await
    .unwrap();
    db.insert::<Episode, _>([
        episode::EpisodeInput {
            title: "A New Hope".into(),
            year: 1977,
            hero: 8,
        },
        episode::EpisodeInput {
            title: "The Empire Strikes Back".into(),
            year: 1980,
            hero: 1,
        },
        episode::EpisodeInput {
            title: "Return of the Jedi".into(),
            year: 1983,
            hero: 8,
        },
    ])
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
    async fn test_heroes() {
        init_logging();
        let schema = schema(test_options!()).await;

        // Get the hero of each film, along with their home planet if they are human or their
        // function if they are droids.
        let heroes = schema
            .execute(
                r#"query {
                    episodes {
                        edges {
                            node {
                                title
                                hero {
                                    name
                                    human { edges { node { homePlanet } } }
                                    droid { edges { node { primaryFunction } } }
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
            heroes.data,
            value!({
                "episodes": {
                    "edges": [
                        {
                            "node": {
                                "title": "A New Hope",
                                "hero": {
                                    "name": "R2-D2",
                                    "human": { "edges": [] },
                                    "droid": {
                                        "edges": [
                                            {"node": {"primaryFunction": "Astromech"}}
                                        ]
                                    },
                                }
                            }
                        },
                        {
                            "node": {
                                "title": "The Empire Strikes Back",
                                "hero": {
                                    "name": "Luke Skywalker",
                                    "droid": { "edges": [] },
                                    "human": {
                                        "edges": [
                                            {"node": {"homePlanet": "Tatooine"}}
                                        ]
                                    },
                                }
                            }
                        },
                        {
                            "node": {
                                "title": "Return of the Jedi",
                                "hero": {
                                    "name": "R2-D2",
                                    "human": { "edges": [] },
                                    "droid": {
                                        "edges": [
                                            {"node": {"primaryFunction": "Astromech"}}
                                        ]
                                    },
                                }
                            }
                        }
                    ]
                }
            })
        );
    }

    #[ignore] // We have not implemented the interface required to populate the many-to-many relations.
    #[async_std::test]
    async fn test_many_to_many_query() {
        init_logging();
        let schema = schema(test_options!()).await;

        // Query a many-to-many relation: get all characters from episodes released in the '80s,
        // along with their home planet if they are human or their function if they are droids.
        let survivors = schema
            .execute(
                r#"query {
                    episodes(where: {
                        has: {
                            year: {
                                cmp: {
                                    op: GE
                                    value: { lit : 1980 }
                                }
                            }
                        }
                    }) {
                        edges {
                            node {
                                title
                                characters {
                                    edges {
                                        node {
                                            name
                                            human { edges { node { homePlanet } } }
                                            droid { edges { node { primaryFunction } } }
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
            survivors.data,
            value!({
                "episodes": {
                    "edges": [
                        {
                            "node": {
                                "title": "The Empire Strikes Back",
                                "characters": {
                                    "edges": [
                                        {
                                            "node": {
                                                "name": "Luke Skywalker",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "Han Solo",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "Leia Organa",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "C-3PO",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "R2-D2",
                                            },
                                        },
                                    ]
                                }
                            }
                        },
                        {
                            "node": {
                                "title": "Return of the Jedi",
                                "characters": {
                                    "edges": [
                                        {
                                            "node": {
                                                "name": "Luke Skywalker",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "Anakin Skywalker",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "Han Solo",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "Leia Organa",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "C-3PO",
                                            },
                                        },
                                        {
                                            "node": {
                                                "name": "R2-D2",
                                            },
                                        },
                                    ]
                                }
                            }
                        },
                    ]
                }
            })
        );
    }
}
