//! Compilation of relation population from high-level GraphQL types into low-level SQL types.

use super::{join_column_name, join_table_name, scalar_to_value, Error};
use crate::{
    array,
    graphql::type_system as gql,
    sql::db::{Connection, Insert, Value},
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
            todo!()
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
    use gql::{Id, Many, Resource};

    crate::use_backend!(SqlDataSource<mock::Connection>);

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
