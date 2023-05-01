//! Compilation of resource registration from high-level GraphQL types into low-level SQL types.

use super::{column_name, join_column_name, join_table_name, lower_scalar_type, table_name, Error};
use crate::{
    array,
    graphql::type_system::{self as gql, Type as _},
    sql::db::{
        AlterTable, AlterTableExt, Connection, ConstraintKind, CreateTable, CreateTableExt,
        SchemaColumn, Type,
    },
    typenum::U2,
};
use futures::future::{try_join_all, BoxFuture, FutureExt, TryFutureExt};
use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};

/// Register a resource `T` in the database schema.
pub async fn execute<C: Connection, T: gql::Resource>(conn: &C) -> Result<(), Error> {
    let mut dependencies = Dependencies::default();
    register_resource::<C, T>(conn, &mut dependencies);
    dependencies.execute(conn).await
}

fn register_resource<'a, C: Connection, T: gql::Resource>(
    conn: &'a C,
    dependencies: &mut Dependencies<'a>,
) {
    dependencies.register::<T, _>(|table, dependencies| {
        let constraints = T::describe_fields(&mut ColumnBuilder { conn, dependencies });
        let columns = constraints.clone().unzip().0;

        // A relation is implemented as a foreign key on another table referencing this one, so for
        // this table, we have nothing to do. But we still have to traverse the referenced type and
        // make sure all the appropriate tables are created.
        T::describe_relations(&mut ColumnTraverser { conn, dependencies });

        // Separate the constraints which can be added immediately (those which are local to this
        // table) and those which must be deferred (those which reference another table that might
        // not be created yet).
        let (now_constraints, deferred_constraints): (Vec<_>, Vec<_>) = constraints
            .into_iter()
            .filter_map(|(col, constraint)| {
                constraint.map(|constraint| (constraint, vec![col.name()]))
            })
            .partition(|(constraint, _)| !matches!(constraint, ConstraintKind::ForeignKey { .. }));

        // Defer the foreign constraints for later.
        dependencies.defer_constraints(table.to_string(), deferred_constraints);

        // Create a future to register this table.
        conn.create_table(Cow::Owned(table.to_string()), columns)
            .constraints(now_constraints)
            .execute()
            .map_err(Error::sql)
            .boxed()
    });
}

struct ColumnBuilder<'a, 'd, C> {
    conn: &'a C,
    dependencies: &'d mut Dependencies<'a>,
}

impl<'a, 'd, C: Connection, T: gql::Resource> gql::FieldVisitor<T> for ColumnBuilder<'a, 'd, C> {
    type Output = (SchemaColumn<'a>, Option<ConstraintKind>);

    fn visit<F: gql::Field<Resource = T>>(&mut self) -> Self::Output {
        let column_name = Cow::Owned(column_name::<F>());

        // The ID field is special.
        if F::is_id() {
            return (
                SchemaColumn::new(column_name, Type::Serial),
                Some(ConstraintKind::PrimaryKey),
            );
        }

        // All other fields are handled based on their type.
        struct Visitor<'a, 'd, C> {
            conn: &'a C,
            column_name: Cow<'a, str>,
            dependencies: &'d mut Dependencies<'a>,
        }

        impl<'a, 'd, C: Connection, T: gql::Type> gql::Visitor<T> for Visitor<'a, 'd, C> {
            type Output = (SchemaColumn<'a>, Option<ConstraintKind>);

            fn resource(self) -> Self::Output
            where
                T: gql::Resource,
            {
                // If the field is a reference to another table, make sure that table is registered.
                register_resource::<C, T>(self.conn, self.dependencies);

                // Add the corresponding ID as a foreign key on this table.
                (
                    SchemaColumn::new(self.column_name, Type::Int4),
                    Some(ConstraintKind::ForeignKey {
                        table: table_name::<T>(),
                    }),
                )
            }

            fn scalar(self) -> Self::Output
            where
                T: gql::Scalar,
            {
                // If the field is a scalar, just create a column of the corresponding type.
                (
                    SchemaColumn::new(self.column_name, lower_scalar_type::<T>()),
                    None,
                )
            }
        }

        F::Type::describe(Visitor {
            conn: self.conn,
            dependencies: self.dependencies,
            column_name,
        })
    }
}

struct ColumnTraverser<'a, 'd, C> {
    conn: &'a C,
    dependencies: &'d mut Dependencies<'a>,
}

impl<'a, 'd, C: Connection, T: gql::Resource> gql::RelationVisitor<T>
    for ColumnTraverser<'a, 'd, C>
{
    type Output = ();

    fn visit_many_to_one<R: gql::ManyToOneRelation<Owner = T>>(&mut self) -> Self::Output {
        register_resource::<C, R::Target>(self.conn, self.dependencies);
    }

    fn visit_many_to_many<R: gql::ManyToManyRelation<Owner = T>>(&mut self) -> Self::Output {
        register_resource::<C, R::Target>(self.conn, self.dependencies);

        // Create a join table to represent this relation.
        self.dependencies
            .register_join_table::<R, _>(|table, dependencies| {
                // Create a table with just two columns, one for the ID of the target of each
                // direction of the relation. Each of these columns has a foreign key constraint on
                // the target table (deferred until we have created that table), and there is a
                // primary key constraint on the pair.
                let target_column = join_column_name::<R>();
                let owner_column = join_column_name::<R::Inverse>();
                dependencies.defer_constraints(
                    table.to_string(),
                    [
                        (
                            ConstraintKind::ForeignKey {
                                table: table_name::<R::Target>(),
                            },
                            [target_column.clone()],
                        ),
                        (
                            ConstraintKind::ForeignKey {
                                table: table_name::<R::Owner>(),
                            },
                            [owner_column.clone()],
                        ),
                    ],
                );
                self.conn
                    .create_table::<U2>(
                        table.to_string(),
                        array![SchemaColumn;
                            SchemaColumn::new(target_column.clone(), Type::Int4),
                            SchemaColumn::new(owner_column.clone(), Type::Int4),
                        ],
                    )
                    .constraint(ConstraintKind::PrimaryKey, [target_column, owner_column])
                    .execute()
                    .map_err(Error::sql)
                    .boxed()
            });
    }
}

/// Dependencies of a resource being registered.
///
/// As a resource is registered, its type will be traversed and all resources it references will be
/// added as dependencies. In addition, constraints on the resource table which reference dependency
/// tables will also be tracked.
///
/// Once all dependencies have been discovered, all of the necessary tables can be created at once
/// as a batch, and then all of the constraints that depend on those tables can be added as a second
/// batch.
#[derive(Default)]
struct Dependencies<'a> {
    tables: HashMap<String, Option<BoxFuture<'a, Result<(), Error>>>>,
    constraints: HashMap<String, Vec<(ConstraintKind, Vec<String>)>>,
}

impl<'a> Dependencies<'a> {
    /// Register a resource if it is not already added.
    ///
    /// If there is already a table for `T` in the dependencies, this function does nothing.
    /// Otherwise, it invokes the function `create`, which should return a future to create the
    /// table corresponding to `T`. The resulting future is added as a dependency and will be
    /// executed in [`execute`](Self::execute).
    fn register<T, F>(&mut self, create: F)
    where
        T: gql::Resource,
        F: FnOnce(&str, &mut Self) -> BoxFuture<'a, Result<(), Error>>,
    {
        let table = table_name::<T>();
        match self.tables.entry(table.clone()) {
            Entry::Occupied(_) => {
                // If this table is already in the list of tables to register, we have nothing more
                // to do.
                return;
            }
            Entry::Vacant(e) => {
                // Insert a placeholder here so if this type references itself recursively, we won't
                // recursively try to register the same type again.
                e.insert(None);
            }
        }

        let fut = create(&table, self);
        self.tables.insert(table, Some(fut));
    }

    fn register_join_table<R, F>(&mut self, create: F)
    where
        R: gql::ManyToManyRelation,
        F: FnOnce(&str, &mut Self) -> BoxFuture<'a, Result<(), Error>>,
    {
        let table = join_table_name::<R>();
        match self.tables.entry(table.clone()) {
            Entry::Occupied(_) => {
                // If this table is already in the list of tables to register, we have nothing more
                // to do.
                return;
            }
            Entry::Vacant(e) => {
                // Insert a placeholder here so if this relation references itself recursively, we
                // won't recursively try to register the same table again.
                e.insert(None);
            }
        }

        let fut = create(&table, self);
        self.tables.insert(table, Some(fut));
    }

    /// Defer constraints to be added to a table after all the dependency tables are created.
    ///
    /// This is useful if the constraint references a table which might not be created yet.
    fn defer_constraints<I, C>(&mut self, table: String, constraints: I)
    where
        I: IntoIterator<Item = (ConstraintKind, C)>,
        C: IntoIterator,
        C::Item: Into<String>,
    {
        self.constraints.entry(table).or_default().extend(
            constraints
                .into_iter()
                .map(|(kind, cols)| (kind, cols.into_iter().map(|col| col.into()).collect())),
        );
    }

    /// Create all the tables and constraints.
    async fn execute<C: Connection>(self, conn: &C) -> Result<(), Error> {
        try_join_all(self.tables.into_values().flatten()).await?;
        try_join_all(self.constraints.into_iter().map(|(table, constraints)| {
            conn.alter_table(table)
                .add_constraints(constraints)
                .execute()
        }))
        .await
        .map_err(Error::sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        self as relational_graphql, // So the derive macros work in the crate itself.
        sql::{db::mock, SqlDataSource},
    };
    use gql::{BelongsTo, Id, Many, Resource};

    crate::use_backend!(SqlDataSource<mock::Connection>);

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Simple {
        id: Id,
        int_field: i32,
        text_field: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct OneToOne {
        id: Id,
        simple: Simple,
    }

    #[async_std::test]
    async fn test_simple() {
        let db = mock::Connection::create();
        execute::<_, Simple>(&db).await.unwrap();
        assert_eq!(
            db.schema().await["simples"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("int_field", Type::Int4),
                SchemaColumn::new("text_field", Type::Text)
            ]
        );
    }

    #[async_std::test]
    async fn test_one_to_one() {
        let db = mock::Connection::create();
        execute::<_, OneToOne>(&db).await.unwrap();
        let schema = db.schema().await;

        // Ensure the dependency table `simples` was created.
        assert_eq!(
            schema["simples"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("int_field", Type::Int4),
                SchemaColumn::new("text_field", Type::Text)
            ]
        );

        // Check the table with the relation, implemented as a foreign key.
        assert_eq!(
            schema["one_to_ones"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("simple", Type::Int4),
            ]
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
        execute::<_, Parent>(&db).await.unwrap();
        let schema = db.schema().await;

        // Ensure the dependency table `children` was created.
        assert_eq!(
            schema["children"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("parent", Type::Int4),
            ]
        );

        // Check the table with the relation, which does not explicitly reference the children.
        assert_eq!(
            schema["parents"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text)
            ]
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Foo {
        id: Id,
        name: String,
        #[resource(inverse(many_foos))]
        many_bars: Many<Bar>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Resource)]
    struct Bar {
        id: Id,
        name: String,
        #[resource(inverse(many_bars))]
        many_foos: Many<Foo>,
    }

    #[async_std::test]
    async fn test_many_to_many() {
        let db = mock::Connection::create();
        execute::<_, Foo>(&db).await.unwrap();
        let schema = db.schema().await;

        // Ensure both resource tables were created.
        assert_eq!(
            schema["foos"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text),
            ]
        );
        assert_eq!(
            schema["bars"],
            [
                SchemaColumn::new("id", Type::Serial),
                SchemaColumn::new("name", Type::Text),
            ]
        );

        // Check the join table.
        assert_eq!(
            schema["bars_many_foos_join_foos_many_bars"],
            [
                SchemaColumn::new("bars_many_foos", Type::Int4),
                SchemaColumn::new("foos_many_bars", Type::Int4),
            ]
        );

        // Registering the other table is a no-op -- the name of the join table is the same
        // regardless of which direction we are looking at it from.
        execute::<_, Bar>(&db).await.unwrap();
        let mut tables = db.schema().await.into_keys().collect::<Vec<_>>();
        tables.sort();
        assert_eq!(
            tables,
            ["bars", "bars_many_foos_join_foos_many_bars", "foos"]
        );
    }
}
