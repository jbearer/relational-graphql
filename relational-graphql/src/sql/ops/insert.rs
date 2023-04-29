//! Compilation of insert from high-level GraphQL types into low-level SQL types.

use super::{column_name_of_field, scalar_to_value, table_name, Error};
use crate::Array;
use crate::{
    graphql::type_system::{self as gql, ResourceInput},
    sql::db::{Connection, Insert, Value},
};

/// Insert items of resource `T` into the database.
pub async fn execute<C: Connection, T: gql::Resource>(
    conn: &C,
    inputs: impl IntoIterator<Item = T::ResourceInput>,
) -> Result<(), Error> {
    let table = table_name::<T>();
    let columns = T::input_field_names().map(column_name_of_field);
    let rows = inputs.into_iter().map(build_row::<T>);
    conn.insert(&table, columns)
        .rows(rows)
        .execute()
        .await
        .map_err(Error::sql)
}

fn build_row<T: gql::Resource>(input: T::ResourceInput) -> Array<Value, T::NumInputFields> {
    struct Visitor<'a, T: gql::Resource>(&'a T::ResourceInput);

    impl<'a, T: gql::Resource> gql::InputFieldVisitor<T> for Visitor<'a, T> {
        type Output = Value;

        fn visit<F: gql::InputField<Resource = T>>(&mut self) -> Self::Output {
            scalar_to_value(self.0.get::<F>().clone())
        }
    }

    T::describe_input_fields(&mut Visitor(&input))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        self as relational_graphql, // So the derive macros work in the crate itself.
        array,
        init_logging,
        sql::{
            db::{mock, SchemaColumn, Type},
            ops, SqlDataSource,
        },
        typenum::{U2, U3},
    };
    use gql::{BelongsTo, Id, Resource};

    crate::use_backend!(SqlDataSource<mock::Connection>);

    /// A simple test resource with scalar fields.
    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct TestResource {
        id: Id,
        field1: i32,
        field2: String,
    }

    #[async_std::test]
    async fn test_round_trip_no_relations() {
        init_logging();

        let db = mock::Connection::create();
        db.create_table::<U3>(
            "test_resources",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field1", Type::Int4),
                SchemaColumn::new("field2", Type::Text),
            ],
        )
        .await
        .unwrap();

        ops::insert::execute::<_, TestResource>(
            &db,
            [
                test_resource::TestResourceInput {
                    field1: 0,
                    field2: "foo".into(),
                },
                test_resource::TestResourceInput {
                    field1: 1,
                    field2: "bar".into(),
                },
                test_resource::TestResourceInput {
                    field1: 1,
                    field2: "baz".into(),
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            ops::select::execute::<_, TestResource>(&db, None)
                .await
                .unwrap(),
            [
                TestResource {
                    id: 1,
                    field1: 0,
                    field2: "foo".into(),
                },
                TestResource {
                    id: 2,
                    field1: 1,
                    field2: "bar".into(),
                },
                TestResource {
                    id: 3,
                    field1: 1,
                    field2: "baz".into(),
                },
            ]
        );
    }

    /// A resource that owns another resource (a one-one or one-many relationship).
    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Owner {
        id: Id,
        owned: TestResource,
    }

    #[async_std::test]
    async fn test_insert_owner() {
        init_logging();

        let db = mock::Connection::create();
        db.create_table::<U3>(
            "test_resources",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field1", Type::Int4),
                SchemaColumn::new("field2", Type::Text),
            ],
        )
        .await
        .unwrap();
        db.create_table::<U2>(
            "owners",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("owned", Type::Int4),
            ],
        )
        .await
        .unwrap();

        // First we have to insert something to own.
        ops::insert::execute::<_, TestResource>(
            &db,
            [test_resource::TestResourceInput {
                field1: 0,
                field2: "foo".into(),
            }],
        )
        .await
        .unwrap();

        // Now insert something that owns the first resource.
        ops::insert::execute::<_, Owner>(&db, [owner::OwnerInput { owned: 1 }])
            .await
            .unwrap();

        // Read it back.
        assert_eq!(
            ops::select::execute::<_, Owner>(&db, None).await.unwrap(),
            [Owner {
                id: 1,
                owned: TestResource {
                    id: 1,
                    field1: 0,
                    field2: "foo".into()
                }
            }]
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Parent {
        id: Id,
        name: String,
        children: BelongsTo<Child>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    #[resource(plural(Children))]
    struct Child {
        id: Id,
        parent: Parent,
    }

    #[async_std::test]
    async fn test_many_to_one() {
        let db = mock::Connection::create();
        db.create_table::<U2>(
            "children",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("parent", Type::Int4),
            ],
        )
        .await
        .unwrap();
        db.create_table::<U2>(
            "parents",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text),
            ],
        )
        .await
        .unwrap();

        // First we have to insert something to own.
        ops::insert::execute::<_, Parent>(&db, [parent::ParentInput { name: "foo".into() }])
            .await
            .unwrap();

        // Now insert several things that own the first resource.
        ops::insert::execute::<_, Child>(
            &db,
            [
                child::ChildInput { parent: 1 },
                child::ChildInput { parent: 1 },
            ],
        )
        .await
        .unwrap();

        // Read back the parent.
        let parent = ops::select::execute::<_, Parent>(&db, None)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            parent,
            Parent {
                id: 1,
                name: "foo".into(),
                children: Default::default(),
            }
        );

        // Get its children.
        let children =
            ops::select::load_relation::<_, parent::fields::Children>(&db, &parent, None)
                .await
                .unwrap();
        assert_eq!(
            children,
            [
                Child {
                    id: 1,
                    parent: parent.clone()
                },
                Child {
                    id: 2,
                    parent: parent.clone()
                }
            ]
        )
    }
}
