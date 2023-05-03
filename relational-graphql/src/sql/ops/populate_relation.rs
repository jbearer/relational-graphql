//! Compilation of relation population from high-level GraphQL types into low-level SQL types.

use super::{
    column_name, field_column, join_column_name, join_table_name, scalar_to_value, table_name,
    Error,
};
use crate::{
    array,
    graphql::type_system as gql,
    sql::db::{Column, Connection, FromItem, Insert, Update, Value},
    typenum::U2,
    Array,
};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};

/// Relate pairs of objects by relation `R`.
pub async fn execute<C: Connection, R: gql::Relation>(
    conn: &C,
    pairs: impl IntoIterator<Item = (gql::Id, gql::Id)>,
) -> Result<(), Error> {
    struct Visitor<'a, C> {
        conn: &'a C,
        pairs: Vec<Array<Value, U2>>,
    }

    impl<'a, C: Connection, T: gql::Resource> gql::RelationVisitor<T> for Visitor<'a, C> {
        type Output = BoxFuture<'a, Result<(), Error>>;

        fn visit_many_to_one<R: gql::ManyToOneRelation<Owner = T>>(&mut self) -> Self::Output {
            // For many-to-one relations, we set the owner field of each target object identified by
            // the second item in the pair to the owner ID which is the first item in the pair:
            //
            //      UPDATE {R::Target} SET {R::Inverse} = pairs.owner
            //          WHERE {R::Target::Id} = pairs.target
            //          FROM (VALUES {pairs}) AS pairs (owner, target)
            self.conn
                .update(table_name::<R::Target>())
                .set(
                    column_name::<R::Inverse>(),
                    Column::qualified("pairs", "owner"),
                )
                .filter(
                    field_column::<<R::Target as gql::Resource>::Id>(),
                    "=",
                    Column::qualified("pairs", "target"),
                )
                .from(FromItem::alias(
                    "pairs",
                    ["owner", "target"],
                    FromItem::values(std::mem::take(&mut self.pairs)),
                ))
                .execute()
                .map_err(Error::sql)
                .boxed()
        }

        fn visit_many_to_many<R: gql::ManyToManyRelation<Owner = T>>(&mut self) -> Self::Output {
            // For many-to-many relations, we simply insert the (owner, target) pairs into the join
            // table:
            //
            //      INSERT INTO {join_table_name::<R>()} ({R::Inverse}, {R}) VALUES {pairs}
            //
            self.conn
                .insert::<_, U2>(
                    join_table_name::<R>(),
                    array![String; join_column_name::<R::Inverse>(), join_column_name::<R>()],
                )
                .rows(std::mem::take(&mut self.pairs))
                .execute()
                .map_err(Error::sql)
                .boxed()
        }
    }

    let pairs = pairs
        .into_iter()
        .map(|(owner, target)| array![Value; scalar_to_value(owner), scalar_to_value(target)])
        .collect();
    R::visit(&mut Visitor { conn, pairs }).await
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        self as relational_graphql, // So the derive macros work in the crate itself.
        init_logging,
        sql::{db::mock, ops, SqlDataSource},
    };
    use gql::{BelongsTo, Id, Many, Resource};

    crate::use_backend!(SqlDataSource<mock::Connection>);

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Owner {
        id: Id,
        field: u32,
        targets: BelongsTo<Target>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Target {
        id: Id,
        field: String,
        owner: Owner,
    }

    #[async_std::test]
    async fn test_many_to_one() {
        init_logging();

        let owners = [
            Owner {
                id: 1,
                field: 0,
                targets: Default::default(),
            },
            Owner {
                id: 2,
                field: 1,
                targets: Default::default(),
            },
        ];
        let targets = [
            Target {
                id: 1,
                field: "0".into(),
                owner: owners[0].clone(),
            },
            Target {
                id: 2,
                field: "1".into(),
                owner: owners[1].clone(),
            },
        ];

        // Create and populate a database.
        let db = mock::Connection::create();
        ops::register::execute::<_, Owner>(&db).await.unwrap();
        ops::insert::execute::<_, Owner>(
            &db,
            [
                owner::OwnerInput {
                    field: owners[0].field,
                },
                owner::OwnerInput {
                    field: owners[1].field,
                },
            ],
        )
        .await
        .unwrap();
        ops::insert::execute::<_, Target>(
            &db,
            [
                target::TargetInput {
                    field: targets[0].field.clone(),
                    owner: targets[0].id,
                },
                target::TargetInput {
                    field: targets[1].field.clone(),
                    owner: targets[1].id,
                },
            ],
        )
        .await
        .unwrap();

        // Since each target specifies an owner, the relation is immediately populated.
        assert_eq!(
            ops::select::load_relation::<_, owner::fields::Targets>(&db, &owners[0], None)
                .await
                .unwrap(),
            &targets[0..1]
        );
        assert_eq!(
            ops::select::load_relation::<_, owner::fields::Targets>(&db, &owners[1], None)
                .await
                .unwrap(),
            &targets[1..]
        );

        // We can change the relation by specifying a new owner for one of the targets.
        ops::populate_relation::execute::<_, owner::fields::Targets>(
            &db,
            [(owners[0].id, targets[1].id)],
        )
        .await
        .unwrap();
        assert_eq!(
            ops::select::load_relation::<_, owner::fields::Targets>(&db, &owners[0], None)
                .await
                .unwrap(),
            [
                Target {
                    id: 1,
                    field: "0".into(),
                    owner: owners[0].clone(),
                },
                Target {
                    id: 2,
                    field: "1".into(),
                    owner: owners[0].clone(),
                },
            ]
        );
        assert_eq!(
            ops::select::load_relation::<_, owner::fields::Targets>(&db, &owners[1], None)
                .await
                .unwrap(),
            []
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct ManyToManyA {
        id: Id,
        field: u32,
        #[resource(inverse(target))]
        target: Many<ManyToManyB>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct ManyToManyB {
        id: Id,
        field: String,
        #[resource(inverse(target))]
        target: Many<ManyToManyA>,
    }

    #[async_std::test]
    async fn test_many_to_many() {
        init_logging();

        let a = [
            ManyToManyA {
                id: 1,
                field: 0,
                target: Default::default(),
            },
            ManyToManyA {
                id: 2,
                field: 1,
                target: Default::default(),
            },
        ];
        let b = [
            ManyToManyB {
                id: 1,
                field: "0".into(),
                target: Default::default(),
            },
            ManyToManyB {
                id: 2,
                field: "1".into(),
                target: Default::default(),
            },
        ];

        // Create and populate a database.
        let db = mock::Connection::create();
        ops::register::execute::<_, ManyToManyA>(&db).await.unwrap();
        ops::insert::execute::<_, ManyToManyA>(
            &db,
            [
                many_to_many_a::ManyToManyAInput { field: a[0].field },
                many_to_many_a::ManyToManyAInput { field: a[1].field },
            ],
        )
        .await
        .unwrap();
        ops::insert::execute::<_, ManyToManyB>(
            &db,
            [
                many_to_many_b::ManyToManyBInput {
                    field: b[0].field.clone(),
                },
                many_to_many_b::ManyToManyBInput {
                    field: b[1].field.clone(),
                },
            ],
        )
        .await
        .unwrap();

        // Populate the relation from A to B.
        ops::populate_relation::execute::<_, many_to_many_a::fields::Target>(
            &db,
            [(a[0].id, b[1].id), (a[1].id, b[0].id)],
        )
        .await
        .unwrap();
        // Read it back.
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_a::fields::Target>(&db, &a[0], None)
                .await
                .unwrap(),
            &b[1..],
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_a::fields::Target>(&db, &a[1], None)
                .await
                .unwrap(),
            &b[0..1],
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_b::fields::Target>(&db, &b[0], None)
                .await
                .unwrap(),
            &a[1..],
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_b::fields::Target>(&db, &b[1], None)
                .await
                .unwrap(),
            &a[0..1],
        );

        // Add some more pairs using the inverse relation.
        ops::populate_relation::execute::<_, many_to_many_b::fields::Target>(
            &db,
            [(b[0].id, a[0].id)],
        )
        .await
        .unwrap();
        // Read it back.
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_a::fields::Target>(&db, &a[0], None)
                .await
                .unwrap(),
            &b,
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_a::fields::Target>(&db, &a[1], None)
                .await
                .unwrap(),
            &b[0..1],
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_b::fields::Target>(&db, &b[0], None)
                .await
                .unwrap(),
            &a,
        );
        assert_eq!(
            ops::select::load_relation::<_, many_to_many_b::fields::Target>(&db, &b[1], None)
                .await
                .unwrap(),
            &a[0..1],
        );
    }
}
