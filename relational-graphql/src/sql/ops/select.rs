//! Compilation of select from high-level GraphQL types into low-level SQL types.

use super::{
    super::db::{
        Boolean, Column, Connection, JoinClause, Row, Select, SelectColumn, SelectExt, WhereClause,
    },
    field_column, join_column_name, join_table_name, scalar_to_value, table_name, value_to_scalar,
    Error,
};
use crate::graphql::type_system::{self as gql, ResourcePredicate, ScalarPredicate, Type};
use itertools::{Either, Itertools};
use std::any::TypeId;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use take_mut::take;

enum Relation<'a> {
    ManyToOne {
        /// The field on the target resource that identifies the owning object.
        inverse: Column<'a>,
        /// The owner whose targets we want.
        owner: gql::Id,
    },
    ManyToMany {
        /// The join table representing this relation.
        join_table: Cow<'a, str>,
        /// The column on the join table that identifies the target objects.
        target: Column<'a>,
        /// The column on the target table that matches the `target` column on the join table.
        target_id: Column<'a>,
        /// The column on the join table that identifiers the owning objects.
        owner: Column<'a>,
        /// The owner whose targets we want.
        owner_id: gql::Id,
    },
}

/// Search for items of resource `T` matching `filter`.
pub async fn execute<C: Connection, T: gql::Resource>(
    conn: &C,
    filter: Option<T::Predicate>,
) -> Result<Vec<T>, Error> {
    execute_and_filter(conn, filter, None).await
}

/// Load the targets of a [`Relation`](gql::Relation).
pub async fn load_relation<C: Connection, R: gql::Relation>(
    conn: &C,
    owner: &R::Owner,
    filter: Option<<R::Target as gql::Type>::Predicate>,
) -> Result<Vec<R::Target>, Error> {
    // Match on the type of the relation. Many-to-one relations are handled via a simple filter.
    // Many-to-many relations are more complicated because we have to go through a join table.
    struct Visitor<'a, T>(&'a T);

    impl<'a, T: gql::Resource> gql::RelationVisitor<T> for Visitor<'a, T> {
        type Output = Relation<'static>;

        fn visit_many_to_one<R: gql::ManyToOneRelation<Owner = T>>(&mut self) -> Self::Output {
            Relation::ManyToOne {
                inverse: field_column::<R::Inverse>(),
                owner: *self.0.get::<T::Id>(),
            }
        }

        fn visit_many_to_many<R: gql::ManyToManyRelation<Owner = T>>(&mut self) -> Self::Output {
            let join_table = join_table_name::<R>();
            Relation::ManyToMany {
                target: Column::qualified(join_table.clone(), join_column_name::<R>()),
                target_id: field_column::<<R::Target as gql::Resource>::Id>(),
                owner: Column::qualified(join_table.clone(), join_column_name::<R::Inverse>()),
                join_table: join_table.into(),
                owner_id: *self.0.get::<T::Id>(),
            }
        }
    }

    let relation = R::visit(&mut Visitor(owner));
    execute_and_filter(conn, filter, Some(relation)).await
}

/// Search for items of resource `T` matching `filter`.
///
/// Optionally, restrict output to items in a relation.
async fn execute_and_filter<C: Connection, T: gql::Resource>(
    conn: &C,
    filter: Option<T::Predicate>,
    relation: Option<Relation<'_>>,
) -> Result<Vec<T>, Error> {
    // Traverse the resource and map its fields to tables and columns.
    let table = table_name::<T>();
    let mut columns = ColumnMap::new::<T>();
    let select = columns.columns.clone();

    // Select the columns we need to reconstruct this resource from the query results and apply the
    // `filter`.
    let mut query = conn.select(&select, &table);
    if let Some(predicate) = filter {
        query = compile_predicate::<_, T>(&mut columns, query, predicate);
    }

    // Filter down to just the relation of interest.
    match relation {
        Some(Relation::ManyToOne { inverse, owner }) => {
            // We want all the objects in the target resource where the inverse of the relation (the
            // field that indicates the owner of the target) matches the ID of the owning object.
            query = query.cmp(inverse, "=", owner.into());
        }
        Some(Relation::ManyToMany {
            join_table,
            target,
            target_id,
            owner,
            owner_id,
        }) => {
            query = query
                .join(join_table, target, "=", target_id)
                .cmp(owner, "=", owner_id.into());
        }
        None => {}
    }

    query = query.clauses(std::mem::take(&mut columns.joins));
    let rows = query.many().await.map_err(Error::sql)?;
    rows.iter().map(|row| columns.parse_row(row)).collect()
}

/// A map from field types to the select column for that field.
#[derive(Clone, Debug, Default)]
struct ColumnMap {
    index: HashMap<TypeId, usize>,
    columns: Vec<SelectColumn<'static>>,
    joins: Vec<JoinClause<'static>>,
    joined_fields: HashSet<TypeId>,
}

impl ColumnMap {
    /// A column map for `T` and all of its nested resources.
    fn new<T: gql::Resource>() -> Self {
        let mut columns = Self::default();
        columns.add_resource::<T>();
        columns
    }

    /// Add columns for `T` and all of its nested resources.
    fn add_resource<T: gql::Resource>(&mut self) {
        // For each field of `T`, get a list of field columns including the column for that field as
        // well as columns for every field on the type of the column, if that type is a resource.
        struct Visitor<'a>(&'a mut ColumnMap);

        impl<'a, T: gql::Resource> gql::FieldVisitor<T> for Visitor<'a> {
            type Output = ();

            fn visit<F: gql::Field<Resource = T>>(&mut self) -> Self::Output {
                // Check if the type of `F` is also a resource, and if so get its columns
                // recursively.
                struct Visitor<'a, F> {
                    columns: &'a mut ColumnMap,
                    _phantom: PhantomData<fn(&F)>,
                }

                impl<'a, F: gql::Field> gql::Visitor<F::Type> for Visitor<'a, F> {
                    type Output = ();

                    fn resource(self) -> Self::Output
                    where
                        F::Type: gql::Resource,
                    {
                        // Add a join clause to bring this table into the result set.
                        self.columns.join::<F>();

                        // Select the fields of the joined table.
                        self.columns.add_resource::<F::Type>();
                    }

                    fn scalar(self) -> Self::Output
                    where
                        F::Type: gql::Scalar,
                    {
                        // Nothing to do if this column is a scalar; all there is is the column
                        // itself, which we add below.
                    }
                }

                F::Type::describe(Visitor::<F> {
                    columns: self.0,
                    _phantom: Default::default(),
                });

                // Add the column for this field.
                self.0.push::<F>();
            }
        }

        T::describe_fields(&mut Visitor(self));
    }

    /// Add a column for the field `F`.
    fn push<F: gql::Field>(&mut self) {
        self.index.insert(TypeId::of::<F>(), self.columns.len());
        self.columns.push(SelectColumn::Column(field_column::<F>()));
    }

    /// Add a relation to the query without including its fields in the results.
    fn join<F: gql::Field>(&mut self)
    where
        F::Type: gql::Resource,
    {
        if self.joined_fields.insert(TypeId::of::<F>()) {
            // We join on the column referencing this resource in the original table (`F`) being
            // equal to the primary key (`Id`) of this resource's table.
            self.joins.push(JoinClause {
                table: table_name::<F::Type>().into(),
                lhs: field_column::<F>(),
                op: "=".into(),
                rhs: field_column::<<F::Type as gql::Resource>::Id>(),
            });
        }
    }

    /// Convert a row of query results into a resource object.
    fn parse_row<R: Row, T: gql::Resource>(&self, row: &R) -> Result<T, Error> {
        T::build_resource(ResourceBuilder::new(self, row))
    }

    /// The index of the column representing field `F`.
    fn index<F: gql::Field>(&self) -> usize {
        self.index[&TypeId::of::<F>()]
    }
}

impl AsRef<[SelectColumn<'static>]> for ColumnMap {
    fn as_ref(&self) -> &[SelectColumn<'static>] {
        &self.columns
    }
}

/// Compiler to turn a scalar predicate into a condition which is part of a `WHERE` clause.
#[derive(Clone)]
struct ScalarWhereCondition<'a> {
    column: Column<'a>,
}

impl<'a, T: gql::Scalar> gql::ScalarPredicateCompiler<T> for ScalarWhereCondition<'a> {
    type Result = WhereClause<'a>;

    fn cmp(self, op: T::Cmp, value: gql::Value<T>) -> Self::Result {
        match value {
            gql::Value::Lit(x) => {
                Boolean::cmp(self.column, op.to_string(), scalar_to_value(x)).into()
            }
            gql::Value::Var(_) => unimplemented!("pattern variables"),
        }
    }

    fn any<I: IntoIterator<Item = T::ScalarPredicate>>(self, preds: I) -> Self::Result {
        let clauses = preds
            .into_iter()
            .map(|pred| pred.compile(self.clone()))
            .collect::<Vec<_>>();

        // Coalesce disjunctions of equality comparisons into a single `IN` operation.
        let (comparisons, mut clauses): (Vec<_>, Vec<_>) =
            clauses.into_iter().partition_map(|clause| match clause {
                WhereClause::Predicate(Boolean::Cmp { column, op, param }) => {
                    if column == self.column && op == "=" {
                        Either::Left(param)
                    } else {
                        Either::Right(Boolean::cmp(column, op, param).into())
                    }
                }
                clause => Either::Right(clause),
            });
        if !comparisons.is_empty() {
            clauses.push(Boolean::one_of(self.column, comparisons).into());
        }

        WhereClause::any(clauses)
    }
}

/// Compiler to turn a predicate into a condition which is part of a `WHERE` clause.
struct WhereCondition<'a, Q, F: gql::Field> {
    columns: &'a mut ColumnMap,
    query: Q,
    predicate: <F::Type as gql::Type>::Predicate,
}

impl<'a, 'b, Q: Select<'a>, F: gql::Field> gql::Visitor<F::Type> for WhereCondition<'b, Q, F> {
    type Output = Q;

    fn resource(self) -> Q
    where
        F::Type: gql::Resource,
    {
        // Join `T` into the query.
        self.columns.join::<F>();
        compile_predicate::<Q, F::Type>(self.columns, self.query, self.predicate)
    }

    fn scalar(self) -> Self::Output
    where
        F::Type: gql::Scalar,
    {
        self.query
            .filter(self.predicate.compile(ScalarWhereCondition {
                column: field_column::<F>(),
            }))
    }
}

/// Compile a predicate on a resource into a `WHERE` clause on a query of that table.
fn compile_predicate<'a, 'b, Q: Select<'a>, T: gql::Resource>(
    columns: &'b mut ColumnMap,
    query: Q,
    pred: T::ResourcePredicate,
) -> Q {
    struct Visitor<'a, Q, T: gql::Resource> {
        columns: &'a mut ColumnMap,
        query: Q,
        pred: T::ResourcePredicate,
    }

    impl<'a, 'b, Q: Select<'a>, T: gql::Resource> gql::ResourceVisitor<T> for Visitor<'b, Q, T> {
        type Output = Q;

        fn visit_field_in_place<F: gql::Field<Resource = T>>(&mut self) {
            if let Some(sub_pred) = self.pred.take::<F>() {
                take(&mut self.query, |query| {
                    F::Type::describe(WhereCondition::<Q, F> {
                        columns: self.columns,
                        query,
                        predicate: sub_pred,
                    })
                });
            }
        }

        fn visit_many_to_one_in_place<R: gql::ManyToOneRelation<Owner = T>>(&mut self) {
            if let Some(_sub_pred) = R::take_predicate(&mut self.pred) {
                unimplemented!("relations predicates")
            }
        }

        fn visit_many_to_many_in_place<R: gql::ManyToManyRelation<Owner = T>>(&mut self) {
            if let Some(_sub_pred) = R::take_predicate(&mut self.pred) {
                unimplemented!("relations predicates")
            }
        }

        fn end(self) -> Q {
            self.query
        }
    }

    T::describe_resource(Visitor {
        columns,
        query,
        pred,
    })
}

/// Builder to help a resource object reconstruct itself from query results.
struct ResourceBuilder<'a, R> {
    row: &'a R,
    columns: &'a ColumnMap,
}

impl<'a, R> ResourceBuilder<'a, R> {
    fn new(columns: &'a ColumnMap, row: &'a R) -> Self {
        Self { row, columns }
    }
}

impl<'a, R: Row, T: gql::Resource> gql::ResourceBuilder<T> for ResourceBuilder<'a, R> {
    type Error = Error;

    fn field<F: gql::Field<Resource = T>>(&self) -> Result<F::Type, Error>
    where
        F: gql::Field<Resource = T>,
    {
        // Builder to reconstruct the type of `F`.
        struct Builder<'a, R> {
            column: usize,
            columns: &'a ColumnMap,
            row: &'a R,
        }

        impl<'a, R: Row, T: 'a + gql::Type> gql::Builder<T> for Builder<'a, R> {
            type Error = Error;
            type Resource = ResourceBuilder<'a, R> where T: gql::Resource;

            fn resource(self) -> Self::Resource
            where
                T: gql::Resource,
            {
                ResourceBuilder::new(self.columns, self.row)
            }

            fn scalar(self) -> Result<T, Error>
            where
                T: gql::Scalar,
            {
                value_to_scalar(self.row.column(self.column).map_err(Error::sql)?)
            }
        }

        <F::Type as gql::Type>::build(Builder {
            column: self.columns.index::<F>(),
            columns: self.columns,
            row: self.row,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        self as relational_graphql, // So the derive macros work in the crate itself.
        array,
        init_logging,
        sql::{
            db::{mock, Insert, SchemaColumn, Type, Value},
            SqlDataSource,
        },
        use_backend,
    };
    use generic_array::typenum::{U2, U3};
    use gql::{BelongsTo, Id, Many, Resource};

    use_backend!(SqlDataSource<mock::Connection>);

    /// A simple test resource with scalar fields.
    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct TestResource {
        id: Id,
        field1: i32,
        field2: String,
    }

    #[async_std::test]
    async fn test_resource_predicate() {
        init_logging();

        let resources = [
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
        ];

        let db = mock::Connection::create();
        db.create_table_with_rows::<U3>(
            "test_resources",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field1", Type::Int4),
                SchemaColumn::new("field2", Type::Text),
            ],
            [
                array![Value;
                    Value::from(resources[0].field1),
                    Value::from(resources[0].field2.clone()),
                ],
                array![Value;
                    Value::from(resources[1].field1),
                    Value::from(resources[1].field2.clone()),
                ],
                array![Value;
                    Value::from(resources[2].field1),
                    Value::from(resources[2].field2.clone()),
                ],
            ],
        )
        .await
        .unwrap();

        // Test a single sub-predicate.
        let predicate = TestResource::has()
            .field1(<i32 as gql::Type>::Predicate::cmp(
                gql::IntCmpOp::EQ,
                gql::Value::Lit(1),
            ))
            .into();
        assert_eq!(
            execute::<_, TestResource>(&db, Some(predicate))
                .await
                .unwrap(),
            &resources[1..]
        );

        // Test multiple sub-predicates.
        let predicate = TestResource::has()
            .field1(<i32 as gql::Type>::Predicate::cmp(
                gql::IntCmpOp::EQ,
                gql::Value::Lit(1),
            ))
            .field2(<String as gql::Type>::Predicate::cmp(
                gql::StringCmpOp::NE,
                gql::Value::Lit("baz".into()),
            ))
            .into();
        assert_eq!(
            execute::<_, TestResource>(&db, Some(predicate))
                .await
                .unwrap(),
            &resources[1..2],
        );

        // Test no sub-predicates.
        let predicate = TestResource::has().into();
        assert_eq!(
            execute::<_, TestResource>(&db, Some(predicate))
                .await
                .unwrap(),
            &resources,
        );
    }

    #[async_std::test]
    async fn test_in() {
        init_logging();

        let resources = [
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
                field1: 2,
                field2: "baz".into(),
            },
        ];

        let db = mock::Connection::create();
        db.create_table_with_rows::<U3>(
            "test_resources",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field1", Type::Int4),
                SchemaColumn::new("field2", Type::Text),
            ],
            [
                array![Value;
                    Value::from(resources[0].field1),
                    Value::from(resources[0].field2.clone()),
                ],
                array![Value;
                    Value::from(resources[1].field1),
                    Value::from(resources[1].field2.clone()),
                ],
                array![Value;
                    Value::from(resources[2].field1),
                    Value::from(resources[2].field2.clone()),
                ],
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            execute::<_, TestResource>(
                &db,
                Some(
                    TestResource::has()
                        .field1(<i32 as gql::Type>::Predicate::one_of([
                            gql::Value::Lit(0),
                            gql::Value::Lit(1),
                            gql::Value::Lit(2),
                        ]))
                        .into()
                )
            )
            .await
            .unwrap(),
            resources
        );
        assert_eq!(
            execute::<_, TestResource>(
                &db,
                Some(
                    TestResource::has()
                        .field1(<i32 as gql::Type>::Predicate::one_of([
                            gql::Value::Lit(0),
                            gql::Value::Lit(1),
                        ]))
                        .into()
                )
            )
            .await
            .unwrap(),
            &resources[..2]
        );
        assert_eq!(
            execute::<_, TestResource>(
                &db,
                Some(
                    TestResource::has()
                        .field1(<i32 as gql::Type>::Predicate::one_of([gql::Value::Lit(0),]))
                        .into()
                )
            )
            .await
            .unwrap(),
            &resources[..1]
        );
        assert_eq!(
            execute::<_, TestResource>(
                &db,
                Some(
                    TestResource::has()
                        .field1(<i32 as gql::Type>::Predicate::one_of([]))
                        .into()
                )
            )
            .await
            .unwrap(),
            []
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Left {
        id: Id,
        field: i32,
        #[resource(inverse(left))]
        parent: BelongsTo<Node>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Right {
        id: Id,
        field: i32,
        #[resource(inverse(right))]
        parent: BelongsTo<Node>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Node {
        id: Id,
        left: Left,
        right: Right,
    }

    #[async_std::test]
    async fn test_join_filter() {
        init_logging();

        let left = [
            Left {
                id: 1,
                field: 0,
                parent: Default::default(),
            },
            Left {
                id: 2,
                field: 1,
                parent: Default::default(),
            },
        ];
        let right = [
            Right {
                id: 1,
                field: 0,
                parent: Default::default(),
            },
            Right {
                id: 2,
                field: 1,
                parent: Default::default(),
            },
        ];

        let nodes = [
            Node {
                id: 1,
                left: left[0].clone(),
                right: right[1].clone(),
            },
            Node {
                id: 2,
                left: left[1].clone(),
                right: right[1].clone(),
            },
        ];

        let db = mock::Connection::create();
        db.create_table_with_rows::<U2>(
            "lefts",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field", Type::Int4),
            ],
            [
                array![Value; Value::from(left[0].field)],
                array![Value; Value::from(left[1].field)],
            ],
        )
        .await
        .unwrap();
        db.create_table_with_rows::<U2>(
            "rights",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("field", Type::Int4),
            ],
            [
                array![Value; Value::from(right[0].field)],
                array![Value; Value::from(right[1].field)],
            ],
        )
        .await
        .unwrap();
        db.create_table_with_rows::<U3>(
            "nodes",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("left", Type::Int4),
                SchemaColumn::new("right", Type::Int4),
            ],
            [
                array![Value; Value::from(nodes[0].left.id), Value::from(nodes[0].right.id)],
                array![Value; Value::from(nodes[1].left.id), Value::from(nodes[1].right.id)],
            ],
        )
        .await
        .unwrap();

        // Select all.
        assert_eq!(execute::<_, Node>(&db, None).await.unwrap(), &nodes);

        // Select with a WHERE clause.
        assert_eq!(
            execute::<_, Node>(
                &db,
                Some(
                    Node::has()
                        .left(
                            Left::has()
                                .field(<i32 as gql::Type>::Predicate::cmp(
                                    gql::IntCmpOp::EQ,
                                    gql::Value::Lit(0)
                                ))
                                .into()
                        )
                        .into()
                )
            )
            .await
            .unwrap(),
            &nodes[0..1]
        );

        // Load a relation with many targets.
        assert_eq!(
            load_relation::<_, right::fields::Parent>(&db, &right[1], None)
                .await
                .unwrap(),
            &nodes,
        );

        // Load relation with many targets, but some filtered out.
        assert_eq!(
            load_relation::<_, right::fields::Parent>(
                &db,
                &right[1],
                Some(
                    Node::has()
                        .id(<Id as gql::Type>::Predicate::Is(gql::Value::Lit(
                            nodes[0].id
                        )))
                        .into()
                )
            )
            .await
            .unwrap(),
            &nodes[0..1]
        );

        // Load relation with only one target.
        assert_eq!(
            load_relation::<_, left::fields::Parent>(&db, &left[0], None)
                .await
                .unwrap(),
            &nodes[0..1]
        );

        // Load empty relation.
        assert_eq!(
            load_relation::<_, right::fields::Parent>(&db, &right[0], None)
                .await
                .unwrap(),
            []
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Foo {
        id: Id,
        name: String,
        bars: Many<Bar>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Bar {
        id: Id,
        name: String,
        foos: Many<Foo>,
    }

    #[async_std::test]
    async fn test_many_to_many() {
        init_logging();

        let foos = [
            Foo {
                id: 1,
                name: "foo1".into(),
                bars: Default::default(),
            },
            Foo {
                id: 2,
                name: "foo2".into(),
                bars: Default::default(),
            },
        ];
        let bars = [
            Bar {
                id: 1,
                name: "bar1".into(),
                foos: Default::default(),
            },
            Bar {
                id: 2,
                name: "bar2".into(),
                foos: Default::default(),
            },
        ];

        let db = mock::Connection::create();
        db.create_table_with_rows::<U2>(
            "foos",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text),
            ],
            [
                array![Value; Value::from(foos[0].name.clone())],
                array![Value; Value::from(foos[1].name.clone())],
            ],
        )
        .await
        .unwrap();
        db.create_table_with_rows::<U2>(
            "bars",
            array![SchemaColumn;
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text),
            ],
            [
                array![Value; Value::from(bars[0].name.clone())],
                array![Value; Value::from(bars[1].name.clone())],
            ],
        )
        .await
        .unwrap();

        // Create the join table.
        //
        // foo1 <-> bar1
        //      <-> bar2
        db.create_table::<U2>(
            "bars_foos_join_foos_bars",
            array![SchemaColumn;
                SchemaColumn::new("bars_foos", Type::Int4),
                SchemaColumn::new("foos_bars", Type::Int4),
            ],
        )
        .await
        .unwrap();
        db.insert::<_, U2>(
            "bars_foos_join_foos_bars",
            array![&'static str; "bars_foos", "foos_bars"],
        )
        .rows([
            array![Value; Value::from(foos[0].id), Value::from(bars[0].id)],
            array![Value; Value::from(foos[0].id), Value::from(bars[1].id)],
        ])
        .execute()
        .await
        .unwrap();

        // Load a relation with many targets.
        assert_eq!(
            load_relation::<_, foo::fields::Bars>(&db, &foos[0], None)
                .await
                .unwrap(),
            &bars
        );

        // Load relation with many targets, but some filtered out.
        assert_eq!(
            load_relation::<_, foo::fields::Bars>(
                &db,
                &foos[0],
                Some(
                    Bar::has()
                        .name(gql::StringPredicate::Is(gql::Value::Lit(
                            bars[0].name.clone()
                        )))
                        .into()
                )
            )
            .await
            .unwrap(),
            &bars[0..1]
        );

        // Load relation with only one target.
        assert_eq!(
            load_relation::<_, bar::fields::Foos>(&db, &bars[0], None)
                .await
                .unwrap(),
            &foos[0..1]
        );

        // Load empty relation.
        assert_eq!(
            load_relation::<_, foo::fields::Bars>(&db, &foos[1], None)
                .await
                .unwrap(),
            []
        );
    }
}
