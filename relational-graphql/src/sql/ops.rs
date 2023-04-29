//! Compilation of high-level GraphQL operations into low-level SQL operations.

use super::db::{Column, Type, Value};
use crate::graphql::type_system as gql;
use convert_case::{Case, Casing};
use is_type::Is;
use snafu::Snafu;
use std::fmt::Display;

pub mod insert;
pub mod register;
pub mod select;

/// Errors encountered when executing GraphQL operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{error}"))]
    Sql { error: String },

    #[snafu(display("error parsing resource {resource}: {error}"))]
    ParseResource {
        resource: &'static str,
        error: String,
    },

    #[snafu(display("type mismatch: {error}"))]
    TypeMismatch { error: String },

    #[snafu(display("error building type {ty}: {error}"))]
    Build { ty: &'static str, error: String },

    #[snafu(display("error building field {ty}::{field}: {error}"))]
    BuildField {
        ty: &'static str,
        field: &'static str,
        error: String,
    },
}

impl gql::BuildError for Error {
    fn custom<T: gql::Type>(msg: impl Display) -> Self {
        Self::Build {
            ty: T::NAME,
            error: msg.to_string(),
        }
    }

    fn field<F: gql::Field>(msg: impl Display) -> Self {
        Self::BuildField {
            ty: <F::Resource as gql::Type>::NAME,
            field: F::NAME,
            error: msg.to_string(),
        }
    }
}

impl Error {
    /// An error in the SQL layer.
    pub fn sql(error: impl Display) -> Self {
        Self::Sql {
            error: error.to_string(),
        }
    }
}

/// The name of the table corresponding to the resource `T`.
fn table_name<T: gql::Resource>() -> String {
    to_snake_case(T::PLURAL_NAME)
}

/// The column corresponding to the field `F`.
fn field_column<F: gql::Field>() -> Column<'static> {
    Column::qualified(
        table_name::<F::Resource>().into(),
        column_name::<F>().into(),
    )
}

/// The name of the column corresponding to the field `F`.
fn column_name<F: gql::Field>() -> String {
    column_name_of_field(F::NAME)
}

/// The name of the column corresponding to the field with name `field_name`.
fn column_name_of_field(field_name: &'static str) -> String {
    to_snake_case(field_name)
}

/// Convert a string to snake case.
fn to_snake_case(s: &str) -> String {
    use convert_case::Boundary::*;
    s.with_boundaries(&[Hyphen, Underscore, Space, LowerUpper])
        .to_case(Case::Snake)
}

/// Convert a [`Scalar`] to a [`Value`].
fn scalar_to_value<T: gql::Scalar>(val: T) -> Value {
    struct Visitor<T: gql::Scalar>(T);

    impl<T: gql::Scalar> gql::ScalarVisitor<T> for Visitor<T> {
        type Output = Value;

        fn visit_i32(self) -> Self::Output
        where
            T: gql::I32Scalar,
        {
            Value::Int4(self.0.into_val())
        }

        fn visit_i64(self) -> Self::Output
        where
            T: gql::I64Scalar,
        {
            Value::Int8(self.0.into_val())
        }

        fn visit_u32(self) -> Self::Output
        where
            T: gql::U32Scalar,
        {
            Value::UInt4(self.0.into_val())
        }

        fn visit_u64(self) -> Self::Output
        where
            T: gql::U64Scalar,
        {
            Value::UInt8(self.0.into_val())
        }

        fn visit_string(self) -> Self::Output
        where
            T: gql::StringScalar,
        {
            Value::Text(self.0.into_val())
        }
    }

    T::visit(Visitor(val))
}

/// Convert a [`Value`] to a [`Scalar`].
fn value_to_scalar<T: gql::Scalar>(val: Value) -> Result<T, Error> {
    use gql::Scalar;

    struct Visitor(Value);

    /// We parse a scalar from the row differently depending on the desired type.
    impl<T: Scalar> gql::ScalarVisitor<T> for Visitor {
        type Output = Result<T, Error>;

        fn visit_i32(self) -> Self::Output
        where
            T: gql::I32Scalar,
        {
            Ok(T::from_val(
                self.0
                    .try_into()
                    .map_err(|error| Error::TypeMismatch { error })?,
            ))
        }

        fn visit_i64(self) -> Self::Output
        where
            T: Is<Type = i64>,
        {
            Ok(T::from_val(
                self.0
                    .try_into()
                    .map_err(|error| Error::TypeMismatch { error })?,
            ))
        }
        fn visit_u32(self) -> Self::Output
        where
            T: Is<Type = u32>,
        {
            Ok(T::from_val(
                self.0
                    .try_into()
                    .map_err(|error| Error::TypeMismatch { error })?,
            ))
        }
        fn visit_u64(self) -> Self::Output
        where
            T: Is<Type = u64>,
        {
            Ok(T::from_val(
                self.0
                    .try_into()
                    .map_err(|error| Error::TypeMismatch { error })?,
            ))
        }
        fn visit_string(self) -> Self::Output
        where
            T: Is<Type = String>,
        {
            Ok(T::from_val(
                self.0
                    .try_into()
                    .map_err(|error| Error::TypeMismatch { error })?,
            ))
        }
    }

    T::visit(Visitor(val))
}

/// Convert a [`Scalar`] to a SQL [`Type`].
fn lower_scalar_type<T: gql::Scalar>() -> Type {
    struct Visitor;

    impl<T: gql::Scalar> gql::ScalarVisitor<T> for Visitor {
        type Output = Type;

        fn visit_i32(self) -> Self::Output
        where
            T: gql::I32Scalar,
        {
            Type::Int4
        }

        fn visit_i64(self) -> Self::Output
        where
            T: gql::I64Scalar,
        {
            Type::Int8
        }

        fn visit_u32(self) -> Self::Output
        where
            T: gql::U32Scalar,
        {
            Type::UInt4
        }

        fn visit_u64(self) -> Self::Output
        where
            T: gql::U64Scalar,
        {
            Type::UInt8
        }

        fn visit_string(self) -> Self::Output
        where
            T: gql::StringScalar,
        {
            Type::Text
        }
    }

    T::visit(Visitor)
}
