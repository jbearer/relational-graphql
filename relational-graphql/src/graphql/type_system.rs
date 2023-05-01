//! Type system for a relational GraphQL API.
//!
//! A relational GraphQL API is one which allows the client to filter results based on complex
//! queries that involve relations between nodes of the subgraph being queried. The queries are
//! solved by compiling the expressive GraphQL query into a backend which can efficiently solve
//! relational queries, like an RDBMS.
//!
//! This module defines the types of objects that can appear in such an API along with a
//! backend-agnostic interface for compiling them into low-level queries. Any API which follows this
//! type system can be implemented by any backend which implements the relevant compilation traits.
//! This is useful for testing (substituting a mock backend for the production one) and for avoiding
//! lock-in, because it makes it easy to switch an existing application from one backend to another.
//!
//! At a high-level the type system consists of the following notions:
//! * Every entity in the application's data model has a [`Type`], including scalars (the leaves of
//!   the graph).
//! * Every [`Type`] has a [`Predicate`], a filter that can be applied to objects of that type in
//!   order to prune query output.
//! * A [`Scalar`] is a subtype of [`Type`] which corresponds to a GraphQL scalar. These are the
//!   primitive types, the leaves of a graph. There is a fixed set of supported scalar types
//!   provided by this module, and these cannot be extended.
//! * A [`Resource`] is a subtype of [`Type`] which corresponds to a GraphQL object type or a table
//!   in a relational database. Unlike a [`Scalar`], whose only observable property is its value, a
//!   [`Resource`] can have many named [`Field`]s as well as [`Relation`]s to other [`Resource`]
//!   types. These relations can be used in queries to easily select objects which relate to a
//!   selected resource and to prune results based on a property of a relationship between resource.

use super::async_graphql as gql;
use derivative::Derivative;
use derive_more::Display;
use is_type::Is;
use sealed::sealed;
use std::any::TypeId;
use std::error::Error;
use std::fmt::Display;
use std::marker::PhantomData;

pub use resource::*;
pub use scalar::*;

/// Needed to work with the [`Array`] type.
pub use crate::{array, typenum, Array, Length};

/// The base type of the whole GraphQL type system.
///
/// All objects in a relational GraphQL API, from scalars to copmlext object types, have a type
/// which is a subtype of [`Type`], and this trait describes the functionality which is common to
/// every entity in an API.
pub trait Type: Clone + gql::OutputType {
    /// The name of this type.
    ///
    /// This will usually be the stringified name of the type implementing this trait.
    const NAME: &'static str;

    /// The plural form of [`Self::NAME`].
    const PLURAL_NAME: &'static str;

    /// A boolean predciate on objects of this type.
    type Predicate: Predicate<Self>;

    /// The representation of values of this type when creating a new object.
    ///
    /// For scalars, the input is just the value of the scalar itself. For resources, the input is
    /// the ID of the referenced resource.
    type Input: Scalar;

    /// Build an object of this type using a builder supplied by the backend.
    ///
    /// This is used to reconstruct an object from a backend-specific query result.
    fn build<B: Builder<Self>>(builder: B) -> Result<Self, B::Error>;

    /// Describe the structure and definition of this [`Type`].
    fn describe<V: Visitor<Self>>(visitor: V) -> V::Output;
}

/// A boolean predicate on a [`Type`] `T`.
pub trait Predicate<T: Type>: gql::InputType {}

/// Visitor which allows a [`Type`] to describe itself to a backend.
///
/// The implementation of this visitor will be backend-specific, but a [`Type`] can use this
/// backend-agnostic interface to describe itself.
pub trait Visitor<T: Type> {
    /// An output summarizing the results of visiting `T`.
    type Output;

    /// Visit a type which is a [`Resource`].
    fn resource(self) -> Self::Output
    where
        T: Resource;

    /// Visit a type which is a [`Scalar`].
    fn scalar(self) -> Self::Output
    where
        T: Scalar;
}

/// An error encountered while reconstructing a GraphQL [`Type`] from query results.
pub trait BuildError: Error + Sized {
    /// Create an error with the given message.
    ///
    /// The error will indicate that it occured while trying to reconstruct an object of type `T`.
    fn custom<T: Type>(err: impl Display) -> Self;

    /// Create an error with the given message.
    ///
    /// The error will indicate that it occured while trying to reconstruct a field `F`.
    fn field<F: Field>(err: impl Display) -> Self {
        Self::custom::<F::Resource>(format!("error reconstructing field {}: {err}", F::NAME))
    }
}

/// A backend specific interface to query results, used to reconstruct a [`Type`].
pub trait Builder<T: Type> {
    /// An error encountered while attempting to reconstruct the object.
    type Error: BuildError;

    /// A builder specifically for [`Resource`] types.
    type Resource: ResourceBuilder<T, Error = Self::Error>
    where
        T: Resource;

    /// Build a [`Resource`].
    fn resource(self) -> Self::Resource
    where
        T: Resource;

    /// Reconstruct a [`Scalar`].
    fn scalar(self) -> Result<T, Self::Error>
    where
        T: Scalar;
}

pub mod scalar {
    //! In GraphQL, primitives are called _scalars_.
    //!
    //! Scalars make up the leaves of a graph-oriented data model. They are primitive types like
    //! integers and strings, upon which more complex object types are recursively built.
    //!
    //! Unlike complex [`Resource`] types, scalars do not have properties or relationships to other
    //! objects. They are simple, atomic objects whose only property is their value. Users also
    //! cannot create new scalar types, so backends are able to rely on there being a known, finite
    //! set of scalar types upon which to build their own data model.

    use super::*;

    /// A primitive type in the relational GraphQL type system.
    #[sealed]
    pub trait Scalar:
        Type<Predicate = Self::ScalarPredicate, Input = Self> + gql::InputType + gql::ScalarType
    {
        /// Boolean predicates on this scalar type.
        ///
        /// This is always the same type as [`Predicate`](Type::Predicate), but the alias
        /// [`ScalarPredicate`](ScalarPredicate) has the more expressive trait bound
        /// [`ScalarPredicate`] instead of the generic [`Predicate`].
        type ScalarPredicate: ScalarPredicate<Self>;

        /// Comparison operators for this type of scalar.
        type Cmp: Display;

        /// Perform a type-level pattern match on this scalar.
        ///
        /// This allows backend implementations to use the fact that there is a known, finite set of
        /// supported scalar types. The backend can thus use backend-specific properties of each
        /// supported type without placing backend-specific constraints on this trait itself.
        ///
        /// The given [`ScalarVisitor`] must handle the case where `Self` is any of the supported
        /// scalar types.
        fn visit<V: ScalarVisitor<Self>>(visitor: V) -> V::Output;
    }

    /// This trait proves that a scalar is one of the supported scalar types.
    ///
    /// In order to implement a backend, it is often essential to have a finite, known set of scalar
    /// types which the backend must represent. For example, an RDBMS backend will need to map
    /// scalar types to the primitive types supported by the database (e.g. `integer`, `text`,
    /// etc.). This trait proves that, given a bound `T: Scalar`, `T` is one of the following
    /// supported scalar types:
    /// * [`i32`]
    /// * [`i64`]
    /// * [`u32`]
    /// * [`u64`]
    /// * [`String`]
    /// and it proves this in a way that makes this fact usable in Rust code. This is stronger than
    /// the [`macro@sealed`] mechanism (which we also use for scalars) which prevents other
    /// implementations of the [`Scalar`] trait, but which does not allow the Rust compiler to _use_
    /// the information that there are no other implementations.
    ///
    /// This trait allows a backend to pattern match on a [`Scalar`] type, providing functions to
    /// handle only the supported cases. This effectively lists Rust's total pattern matching on
    /// enums to the type level. Just like in each arm of a `match` expression you can use the fact
    /// that the variable being matched is a particular variant of an `enum`, in each method of this
    /// trait you can use the fact that `T` is a certain scalar type, by means of the [`Is`] trait
    /// for type-level equality.
    ///
    /// # Examples
    ///
    /// Suppose we want to check if a scalar value is a default value (0 for integer types and empty
    /// for strings). We could write the following function:
    ///
    /// ```
    /// # use relational_graphql::graphql::type_system::Scalar;
    /// fn is_default_strict<T: Scalar + Default + PartialEq>(value: &T) -> bool {
    ///     *value == T::default()
    /// }
    /// ```
    ///
    /// But this function has a strict type bound: it is only callable if `T` satisfies the extra
    /// trait bounds [`Default`] and [`PartialEq`]. We will have to propagate these bounds upward
    /// through the call stack, which may be difficult if, for example, this function is being
    /// called on an arbitrary field of a [`Resource`] type. We can't easily write the constraint
    /// that all fields of the [`Resource`] we are working with implement these extra bounds.
    /// Indeed, take the [`ResourceBuilder::field`] method. It must work for all `F: Field`, not
    /// just `F: Field, F::Type: Default + PartialEq`.
    ///
    /// To remove the extra type bounds, we can leverage the fact that a [`Scalar`] must be one of a
    /// few types, all of which happen to implement [`Default`] and [`PartialEq`], by performing a
    /// total pattern match on the type of the scalar:
    ///
    /// ```
    /// # use relational_graphql::graphql::type_system::scalar::*;
    /// # fn is_default_strict<T: Scalar + Default + PartialEq>(value: &T) -> bool {
    /// #     *value == T::default()
    /// # }
    /// fn is_default<T: Scalar>(value: &T) -> bool {
    ///     struct Visitor<'a, T>(&'a T);
    ///
    ///     impl<'a, T: Scalar> ScalarVisitor<T> for Visitor<'a, T> {
    ///         type Output = bool;
    ///
    ///         fn visit_i32(self) -> bool
    ///         where
    ///             T: I32Scalar
    ///         {
    ///             // Here we can use the fact that `T` is `i32` by casting `self.0` from `&T` to
    ///             // `&i32`, and then use `i32::default()`.
    ///             let value = self.0.into_ref();
    ///             // Now we can call the stricter typed version of the function to handle the
    ///             // rest:
    ///             is_default_strict(value)
    ///         }
    ///
    ///         // The remaining cases are similar:
    ///         fn visit_i64(self) -> bool
    ///         where
    ///             T: I64Scalar
    ///         {
    ///             is_default_strict(self.0.into_ref())
    ///         }
    ///         fn visit_u32(self) -> bool
    ///         where
    ///             T: U32Scalar
    ///         {
    ///             is_default_strict(self.0.into_ref())
    ///         }
    ///         fn visit_u64(self) -> bool
    ///         where
    ///             T: U64Scalar
    ///         {
    ///             is_default_strict(self.0.into_ref())
    ///         }
    ///         fn visit_string(self) -> bool
    ///         where
    ///             T: StringScalar
    ///         {
    ///             is_default_strict(self.0.into_ref())
    ///         }
    ///     }
    ///
    ///     T::visit(Visitor(value))
    /// }
    /// ```
    ///
    /// The more permissive `is_default` requires a lot more code than the stricter
    /// `is_default_strict`, because we cannot treat all cases the same using a single trait bound.
    /// Instead we must explicitly handle each case, proving to the Rust compiler that in each one,
    /// the type in question works in the way we want to use it. Still, this type of pattern
    /// matching may be a crucial function for backend implementations in case they require some
    /// specific extra trait bound on scalar types and don't have a good place to put it.
    pub trait ScalarVisitor<T: Scalar> {
        /// The type of value which is returned by this type-level match.
        type Output;

        /// Handle the case where `T` is [`i32`].
        fn visit_i32(self) -> Self::Output
        where
            T: I32Scalar;

        /// Handle the case where `T` is [`i64`].
        fn visit_i64(self) -> Self::Output
        where
            T: I64Scalar;

        /// Handle the case where `T` is [`u32`].
        fn visit_u32(self) -> Self::Output
        where
            T: U32Scalar;

        /// Handle the case where `T` is [`u64`].
        fn visit_u64(self) -> Self::Output
        where
            T: U64Scalar;

        /// Handle the case where `T` is [`String`].
        fn visit_string(self) -> Self::Output
        where
            T: StringScalar;
    }

    /// A boolean predicate on a scalar type `T`.
    #[sealed]
    pub trait ScalarPredicate<T: Scalar>: Predicate<T> {
        /// Compile this predicate into a form which the backend can execute.
        ///
        /// When a backend data source executes a GraphQL query, it must compile each predicate in
        /// the query into a form which can be applied to data in the backend's particular datda
        /// model. For scalar predicates, the backend implementation will construct a
        /// [`ScalarPredicateCompiler`] which is specific to that backend and pass it to
        /// [`compile`](Self::compile). The predicate will use the backend-agnostic
        /// [`ScalarPredicateCompiler`] to describe the structure of this predicate and instruct the
        /// backend on how to compile it.
        fn compile<C: ScalarPredicateCompiler<T>>(self, compiler: C) -> C::Result;
    }

    /// A generic interface to a backend-specific compiler for predicates on scalars.
    ///
    /// A [`ScalarPredicate`] can use this interface to instruct an arbitrary backend on how to
    /// compile itself into a backend-specific format.
    pub trait ScalarPredicateCompiler<T: Scalar> {
        /// The backend-specific compilation result.
        type Result;

        /// Instruct the backend to compile a comparison-based predicate.
        ///
        /// The predicate will act on scalars of type `T` by comparing a given scalar with a
        /// constant [`Value`], using `op` to do the comparison.
        fn cmp(self, op: T::Cmp, value: Value<T>) -> Self::Result;
    }

    /// A scalar value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, gql::OneofObject)]
    #[graphql(concrete(name = "I32Value", params(i32)))]
    #[graphql(concrete(name = "I64Value", params(i64)))]
    #[graphql(concrete(name = "U32Value", params(u32)))]
    #[graphql(concrete(name = "U64Value", params(u64)))]
    #[graphql(concrete(name = "StringValue", params(String)))]
    pub enum Value<T: Scalar> {
        /// A literal, constant value.
        Lit(T),
        /// A pattern matching variable.
        Var(usize),
    }

    /// Generate an implementation of the [`build`](Type::build) function for scalars.
    macro_rules! build_scalar {
        () => {
            fn build<B: Builder<Self>>(builder: B) -> Result<Self, B::Error> {
                builder.scalar()
            }
        };
    }

    /// Generate an implementation of the [`describe`](Type::describe) function for scalars.
    macro_rules! describe_scalar {
        () => {
            fn describe<V: Visitor<Self>>(visitor: V) -> V::Output {
                visitor.scalar()
            }
        };
    }

    /// Integral scalars.
    #[sealed]
    pub trait IntScalar: Scalar<Cmp = IntCmpOp> + Copy {}
    #[sealed]
    impl<T: Scalar<Cmp = IntCmpOp> + Copy> IntScalar for T {}

    /// Comparison operators for integral scalar types.
    #[derive(Clone, Copy, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, gql::Enum)]
    pub enum IntCmpOp {
        #[display(fmt = "=")]
        EQ,
        #[display(fmt = "!=")]
        NE,
        #[display(fmt = ">=")]
        GE,
        #[display(fmt = ">")]
        GT,
        #[display(fmt = "<=")]
        LE,
        #[display(fmt = "<")]
        LT,
    }

    macro_rules! int_scalars {
        ($((
            $t:ty,
            $visit:ident,
            $mod:ident,
            $pred_name:expr,
            $cmp_name:expr
        )),+ $(,)?) => {
            $(
                pub mod $mod {
                    use super::*;

                    /// A boolean predicate on an integral scalar.
                    #[derive(
                        Clone,
                        Copy,
                        Debug,
                        PartialEq,
                        Eq,
                        PartialOrd,
                        Ord,
                        Hash,
                        gql::OneofObject,
                    )]
                    #[graphql(name = $pred_name)]
                    pub enum Predicate {
                        /// Satisfied if the comparison is true.
                        Cmp(Cmp),
                        /// Satisfied if the integer being filtered matches the given value.
                        Is(Value<$t>),
                    }

                    impl Predicate {
                        /// A predicate which compares integers with `value` using `op`.
                        pub fn cmp(op: IntCmpOp, value: Value<$t>) -> Self {
                            Self::Cmp(Cmp::new(op, value))
                        }

                        /// A predicate which compares integers with `value`.
                        pub fn is(value: Value<$t>) -> Self {
                            Self::Is(value)
                        }
                    }

                    impl super::Predicate<$t> for Predicate {}

                    #[sealed]
                    impl ScalarPredicate<$t> for Predicate {
                        fn compile<C: ScalarPredicateCompiler<$t>>(
                            self,
                            compiler: C,
                        ) -> C::Result {
                            match self {
                                Self::Cmp(cmp) => cmp.compile(compiler),
                                Self::Is(val) => Cmp::new(IntCmpOp::EQ, val).compile(compiler),
                            }
                        }
                    }

                    /// A comparison on an integral scalar.
                    #[derive(
                        Clone,
                        Copy,
                        Debug,
                        PartialEq,
                        Eq,
                        PartialOrd,
                        Ord,
                        Hash,
                        gql::InputObject,
                    )]
                    #[graphql(name = $cmp_name)]
                    pub struct Cmp {
                        /// The type of comparison.
                        op: IntCmpOp,
                        /// The value to compare with.
                        value: Value<$t>,
                    }

                    impl Cmp {
                        /// A predicate which compares integers with `value` using `op`.
                        pub fn new(op: IntCmpOp, value: Value<$t>) -> Self {
                            Self { op, value }
                        }

                        /// Compile this copmarison into a backend-specific format using the given
                        /// compiler.
                        pub fn compile<C: ScalarPredicateCompiler<$t>>(
                            &self,
                            compiler: C,
                        ) -> C::Result {
                            compiler.cmp(self.op, self.value)
                        }
                    }

                    impl Type for $t {
                        type Predicate = Predicate;

                        type Input = Self;

                        const NAME: &'static str = stringify!($t);
                        const PLURAL_NAME: &'static str = stringify!($t, s);

                        build_scalar!();
                        describe_scalar!();
                    }

                    #[sealed]
                    impl Scalar for $t {
                        type ScalarPredicate = Predicate;
                        type Cmp = IntCmpOp;

                        fn visit<V: super::ScalarVisitor<Self>>(visitor: V) -> V::Output {
                            visitor.$visit()
                        }
                    }

                    #[doc = "An integral scalar represented as "]
                    #[doc = stringify!($t)]
                    #[sealed]
                    pub trait Trait: IntScalar<ScalarPredicate = Predicate> + Is<Type = $t>
                    {}
                    #[sealed]
                    impl Trait for $t {}
                }
            )+
        }
    }

    int_scalars! {
        (i32, visit_i32, i32_scalar, "i32Predicate", "i32Cmp"),
        (i64, visit_i64, i64_scalar, "i64Predicate", "i64Cmp"),
        (u32, visit_u32, u32_scalar, "u32Predicate", "u32Cmp"),
        (u64, visit_u64, u64_scalar, "u64Predicate", "u64Cmp"),
    }

    pub use i32_scalar::Trait as I32Scalar;
    pub use i64_scalar::Trait as I64Scalar;
    pub use u32_scalar::Trait as U32Scalar;
    pub use u64_scalar::Trait as U64Scalar;

    /// A type for unique, sequentially increasing IDs.
    pub type Id = i32;

    /// A string scalar.
    ///
    /// There is only one kind of string scalar, [`String`], so this trait simply constrains the
    /// implementor to be equal to [`String`] using [`Is`]. This is how the Rust compiler knows that
    /// there is only one string scalar and how backends can exploit that fact.
    #[sealed]
    pub trait StringScalar: Scalar<Cmp = StringCmpOp> + Is<Type = String> {}

    #[sealed]
    impl StringScalar for String {}

    /// A boolean predicate on strings.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, gql::OneofObject)]
    pub enum StringPredicate {
        /// Satisfied if the comparison is true.
        Cmp(StringCmp),
        /// Satisfied if the integer being filtered matches the given value.
        Is(Value<String>),
    }

    impl StringPredicate {
        /// A predicate which compares strings with `value` using `op`.
        pub fn cmp(op: StringCmpOp, value: Value<String>) -> Self {
            Self::Cmp(StringCmp::new(op, value))
        }

        /// A predicate which compares strings with `value`.
        pub fn is(value: Value<String>) -> Self {
            Self::Is(value)
        }
    }

    impl Predicate<String> for StringPredicate {}

    #[sealed]
    impl ScalarPredicate<String> for StringPredicate {
        fn compile<C: ScalarPredicateCompiler<String>>(self, compiler: C) -> C::Result {
            match self {
                Self::Cmp(cmp) => cmp.compile(compiler),
                Self::Is(val) => StringCmp::new(StringCmpOp::EQ, val).compile(compiler),
            }
        }
    }

    /// A comparison on strings.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, gql::InputObject)]
    pub struct StringCmp {
        /// The type of comparison.
        op: StringCmpOp,
        /// The value to compare with.
        value: Value<String>,
    }

    impl StringCmp {
        /// A predicate which compares strings with `value` using `op`.
        pub fn new(op: StringCmpOp, value: Value<String>) -> Self {
            Self { op, value }
        }

        /// Compile this copmarison into a backend-specific format using the given compiler.
        pub fn compile<C: ScalarPredicateCompiler<String>>(self, compiler: C) -> C::Result {
            compiler.cmp(self.op, self.value)
        }
    }

    /// Comparison operators for strings.
    #[derive(Clone, Copy, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, gql::Enum)]
    pub enum StringCmpOp {
        #[display(fmt = "=")]
        EQ,
        #[display(fmt = "!=")]
        NE,
    }

    impl Type for String {
        type Predicate = StringPredicate;

        type Input = Self;

        const NAME: &'static str = "String";
        const PLURAL_NAME: &'static str = "Strings";

        build_scalar!();
        describe_scalar!();
    }

    #[sealed]
    impl Scalar for String {
        type ScalarPredicate = StringPredicate;
        type Cmp = StringCmpOp;

        fn visit<V: ScalarVisitor<Self>>(visitor: V) -> V::Output {
            visitor.visit_string()
        }
    }
}

pub mod resource {
    //! Resources are complex types in a relational GraphQL API.
    //!
    //! A resource type is akin to an object type in GraphQL or a table in a relational database. It
    //! has its own fields as well as [`Relation`]s to other resources. Collections of items of a
    //! particular resource can be filter down using a [`ResourcePredicate`]. Entire collections of
    //! items of a given resource type can also be filtered in or out using a [`RelationPredicate`].
    //!
    //! Users can define their own resources by implementing the [`Resource`] trait and relatives,
    //! or by using the [`macro@Resource`] derive macro.

    use super::*;

    pub use relational_graphql_derive::Resource;

    /// A complex type in the relational GraphQL type system.
    pub trait Resource: Type<Predicate = Self::ResourcePredicate, Input = Id> {
        // When `generic_const_exprs` stables, these can be converted to associated constants and
        // the various `Array` types can be converted to constant sized arrays. This will
        // clean the interface up considerably. Unfortunately, for now, it is unstable to use an
        // associated constant in an array bound like `[T; Self::NUM_FIELDS]`, so we must settle for
        // the slightly unwield `Array<T, Self::NumFields>`.
        /// The number of singular fields this resource has.
        type NumFields: Length;
        /// The number of input fields this resource has.
        type NumInputFields: Length;
        /// The number of relations this resource has to other resources.
        type NumRelations: Length;

        /// The unique, sequentially increasing ID for this resource.
        type Id: Field<Resource = Self, Type = Id>;

        /// Boolean predicates on this resource type.
        ///
        /// This is always the same type as [`Predicate`](Type::Predicate), but the alias
        /// [`ResourcePredicate`](ResourcePredicate) has the more expressive trait bound
        /// [`ResourcePredicate`] instead of the generic [`Predicate`].
        type ResourcePredicate: ResourcePredicate<Self>;

        /// A boolean predicate on a [`Relation`] targeting items of this type.
        type RelationPredicate: RelationPredicate<Self>;

        /// The input used to specify a new object of this type.
        ///
        /// The input contains input values for each of the input fields of this resource.
        type ResourceInput: ResourceInput<Self>;

        /// Build a resource using a builder supplied by the backend.
        ///
        /// This performs the same operation as [`build`](Type::build), but it can be
        /// called directly with a [`ResourceBuilder`], instead of the more generic
        /// [`Builder`]. This is useful when it is known that a [`Type`] is actually
        /// a [`Resource`].
        ///
        /// It is an invariant that for all `T: Resource`,
        /// `T::build(builder) == T::builder_resource(builder.resource())`.
        fn build_resource<B: ResourceBuilder<Self>>(builder: B) -> Result<Self, B::Error>;

        /// Access the field `F` of this [`Resource`].
        fn get<F: Field<Resource = Self>>(&self) -> &F::Type {
            F::get(self)
        }

        /// Describe the structure and definition of this [`Resource`].
        fn describe_resource<V: ResourceVisitor<Self>>(visitor: V) -> V::Output {
            struct Visitor<V>(V);

            impl<T: Resource, V: ResourceVisitor<T>> FieldVisitor<T> for Visitor<V> {
                type Output = ();

                fn visit<F: Field<Resource = T>>(&mut self) -> Self::Output {
                    self.0.visit_field_in_place::<F>();
                }
            }

            impl<T: Resource, V: ResourceVisitor<T>> RelationVisitor<T> for Visitor<V> {
                type Output = ();

                fn visit_many_to_one<R: ManyToOneRelation<Owner = T>>(&mut self) -> Self::Output {
                    self.0.visit_many_to_one_in_place::<R>();
                }

                fn visit_many_to_many<R: ManyToManyRelation<Owner = T>>(&mut self) -> Self::Output {
                    self.0.visit_many_to_many_in_place::<R>();
                }
            }

            let mut visitor = Visitor(visitor);
            Self::describe_fields(&mut visitor);
            Self::describe_relations(&mut visitor);
            visitor.0.end()
        }

        /// Describe the fields of this resource.
        fn describe_fields<V: FieldVisitor<Self>>(
            visitor: &mut V,
        ) -> Array<V::Output, Self::NumFields>;

        /// Describe the input fields of this resource.
        fn describe_input_fields<V: InputFieldVisitor<Self>>(
            visitor: &mut V,
        ) -> Array<V::Output, Self::NumInputFields>;

        /// Describe the relations that this resource has to other resources.
        fn describe_relations<V: RelationVisitor<Self>>(
            visitor: &mut V,
        ) -> Array<V::Output, Self::NumRelations>;

        /// The names of this resource's singular fields.
        fn field_names() -> Array<&'static str, Self::NumFields> {
            struct Visitor;

            impl<T: Resource> FieldVisitor<T> for Visitor {
                type Output = &'static str;

                fn visit<F: Field<Resource = T>>(&mut self) -> Self::Output {
                    F::NAME
                }
            }

            Self::describe_fields(&mut Visitor)
        }

        /// The names of this resource's input fields.
        fn input_field_names() -> Array<&'static str, Self::NumInputFields> {
            struct Visitor;

            impl<T: Resource> InputFieldVisitor<T> for Visitor {
                type Output = &'static str;

                fn visit<F: InputField<Resource = T>>(&mut self) -> Self::Output {
                    F::NAME
                }
            }

            Self::describe_input_fields(&mut Visitor)
        }
    }

    /// A backend specific interface to query results, used to reconstruct a [`Resource`].
    pub trait ResourceBuilder<T: Resource> {
        /// Error reconstructing the object.
        type Error: BuildError;

        /// Recursively reconstruct the value of a field.
        fn field<F: Field<Resource = T>>(&self) -> Result<F::Type, Self::Error>;
    }

    /// A boolean predicate on a resource type `T`.
    pub trait ResourcePredicate<T: Resource<ResourcePredicate = Self>>: Predicate<T> {
        /// Get the sub-predicate on the field `F` of resource `T`, if there is one.
        fn get<F: Field<Resource = T>>(&self) -> Option<&<F::Type as Type>::Predicate> {
            F::get_predicate(self)
        }

        /// Get the sub-predicate on the field `F` of resource `T`, if there is one.
        fn get_mut<F: Field<Resource = T>>(&mut self) -> Option<&mut <F::Type as Type>::Predicate> {
            F::get_predicate_mut(self)
        }

        /// Take the sub-predicate on the field `F` of resource `T`, if there is one.
        ///
        /// The next time this function or [`get`](Self::get) is called on the same field, it will
        /// return [`None`], since the field has been taken.
        fn take<F: Field<Resource = T>>(&mut self) -> Option<<F::Type as Type>::Predicate> {
            F::take_predicate(self)
        }
    }

    /// The input used to specify a new object of type `T`.
    ///
    /// The input contains input values for each of the input fields of `T`.
    pub trait ResourceInput<T: Resource<ResourceInput = Self>> {
        /// Get the input value for a field from the overall resource input.
        fn get<F: InputField<Resource = T>>(&self) -> &<F::Type as Type>::Input {
            F::get_input(self)
        }
    }

    /// Metadata about a field of a resource.
    pub trait Field: 'static {
        /// The type of the field.
        type Type: Type;

        /// The resource that this field belongs to.
        type Resource: Resource;

        /// The name of the field.
        const NAME: &'static str;

        /// Access this field of a particular [`Resource`].
        fn get(resource: &Self::Resource) -> &Self::Type;

        /// Access the sub-predicate used to filter this field in a [`ResourcePredicate`].
        fn get_predicate(
            predicate: &<Self::Resource as Resource>::ResourcePredicate,
        ) -> Option<&<Self::Type as Type>::Predicate>;

        /// Access the sub-predicate used to filter this field in a [`ResourcePredicate`].
        fn get_predicate_mut(
            predicate: &mut <Self::Resource as Resource>::ResourcePredicate,
        ) -> Option<&mut <Self::Type as Type>::Predicate>;

        /// Take the sub-predicate used to fitler this field in a [`ResourcePredicate`].
        ///
        /// The next time this function, [`get_predicate`](Self::get_predicate), or
        /// [`get_predicate_mut`](Self::get_predicate_mut) is called on the same field, it will
        /// return [`None`], since the field has been taken.
        fn take_predicate(
            predicate: &mut <Self::Resource as Resource>::ResourcePredicate,
        ) -> Option<<Self::Type as Type>::Predicate>;

        /// Is this the unique ID field for its [`Resource`]?
        fn is_id() -> bool {
            TypeId::of::<Self>() == TypeId::of::<<Self::Resource as Resource>::Id>()
        }
    }

    /// A field which must be present when a new object is created.
    pub trait InputField: Field {
        /// Get the input value for this field from the overall input for its resource.
        fn get_input(
            input: &<Self::Resource as Resource>::ResourceInput,
        ) -> &<Self::Type as Type>::Input;
    }

    /// Metadata about a relation of a resource.
    ///
    /// A relation is like a virtual property of an object. Rather than being represented explicitly
    /// in the object itself, like a [`Field`], the value of a relation comes from the relationshp
    /// of the owning object to some other object or objects in the ontology -- the _target_ of the
    /// relation.
    ///
    /// For example, an object of [`Resource`] A might be related to objects of [`Resource`] B by
    /// a field on `B` which stores the ID of type `A`. This would be a one-to-many relation.
    ///
    /// We categorize relations by their _arity_ -- the number of objects on each side of the
    /// relationship. If any number of objects of one resource can be related to any number of
    /// objects of another, it is a many-to-many relationship. If an object of one resource is
    /// related to a unique object of another, but any number of the former can relate to the same
    /// object of the latter, it is a many-to-one relationship. One-to-one relations are not
    /// supported.
    ///
    /// A useful property of a [`Relation`] is its _inverse_ -- the field or relation on the target
    /// resource which relates back to this relation. The type of the inverse depends on the arity
    /// of the relation. In a many-to-many relation, both sides are a relation, so
    /// [`ManyToManyRelation::Inverse`] implements [`Relation`]. But in a many-to-one relation, each
    /// object in the target resource (the "many" side) is related to exactly one object in the
    /// owning resource (the "one" side) by means of a field storing the ID of the owning resource,
    /// so [`ManyToOneRelation::Inverse`] implements [`Field`].
    pub trait Relation: 'static {
        /// The resource owning this relation.
        type Owner: Resource;
        /// The type of objects in this relation.
        type Target: Resource;
        /// The name of this relation.
        const NAME: &'static str;

        /// Describe this relation to a backend.
        fn visit<V: RelationVisitor<Self::Owner>>(visitor: &mut V) -> V::Output;

        /// Access the sub-predicate used to filter this relation in a [`ResourcePredicate`].
        fn get_predicate(
            predicate: &<Self::Owner as Resource>::ResourcePredicate,
        ) -> Option<&<Self::Target as Resource>::RelationPredicate>;

        /// Access the sub-predicate used to filter this relation in a [`ResourcePredicate`].
        fn get_predicate_mut(
            predicate: &mut <Self::Owner as Resource>::ResourcePredicate,
        ) -> Option<&mut <Self::Target as Resource>::RelationPredicate>;

        /// Take the sub-predicate used to fitler this relation in a [`ResourcePredicate`].
        ///
        /// The next time this function, [`get_predicate`](Self::get_predicate), or
        /// [`get_predicate_mut`](Self::get_predicate_mut) is called on the same relation, it will
        /// return [`None`], since the field has been taken.
        fn take_predicate(
            predicate: &mut <Self::Owner as Resource>::ResourcePredicate,
        ) -> Option<<Self::Target as Resource>::RelationPredicate>;
    }

    /// A boolean predicate on a [`Relation`] targeting items of type `T`.
    pub trait RelationPredicate<T: Resource> {
        /// Compile this predicate into a form which the backend can execute.
        ///
        /// When a backend data source executes a GraphQL query, it must compile each predicate in
        /// the query into a form which can be applied to data in the backend's particular data
        /// model. For relation predicates, the backend implementation will construct a
        /// [`RelationPredicateCompiler`] which is specific to that backend and pass it to
        /// [`compile`](Self::compile). The predicate will use the backend-agnostic
        /// [`RelationPredicateCompiler`] to describe the structure of this predicate and instruct
        /// the backend on how to compile it.
        fn compile<C: RelationPredicateCompiler<T>>(self, compiler: C) -> C::Result;
    }

    /// A generic interface to a backend-specific relation predicate compiler.
    ///
    /// A [`RelationPredicate`] can use this interface to instruct an arbitrary backend on how to
    /// compile it into a backend-specific format.
    pub trait RelationPredicateCompiler<T: Type> {
        /// The backend-specific compilation result.
        type Result;

        /// A predicate which requires at least `min` objects in the relation to match `pred`.
        fn at_least(self, min: usize, pred: T::Predicate) -> Self::Result;

        /// A predicate which requires at most `max` objects in the relation to match `pred`.
        fn at_most(self, max: usize, pred: T::Predicate) -> Self::Result;
    }

    /// A many-to-one relation.
    pub trait ManyToOneRelation: Relation {
        /// The field on target objects which unique denotes this object as their owner.
        type Inverse: Field<Type = Self::Owner, Resource = Self::Target>;
    }

    /// A marker type used in the definition of a resource to declare a [`ManyToOneRelation`].
    #[derive(Derivative)]
    #[derivative(
        Clone(bound = ""),
        Copy(bound = ""),
        Debug(bound = ""),
        Default(bound = ""),
        PartialEq(bound = ""),
        Eq(bound = ""),
        PartialOrd(bound = ""),
        Ord(bound = ""),
        Hash(bound = "")
    )]
    pub struct BelongsTo<T: Resource>(PhantomData<fn(&T)>);

    /// A many-to-many relation.
    pub trait ManyToManyRelation: Relation {
        /// The relation on target objects which links them back to this object.
        type Inverse: ManyToManyRelation<Inverse = Self, Target = Self::Owner, Owner = Self::Target>;
    }

    /// A marker type used in the definition of a resource to declare a [`ManyToManyRelation`].
    #[derive(Derivative)]
    #[derivative(
        Clone(bound = ""),
        Copy(bound = ""),
        Debug(bound = ""),
        Default(bound = ""),
        PartialEq(bound = ""),
        Eq(bound = ""),
        PartialOrd(bound = ""),
        Ord(bound = ""),
        Hash(bound = "")
    )]
    pub struct Many<T: Resource>(PhantomData<fn(&T)>);

    /// Visitor which allows a [`Resource`] to describe itself to a backend.
    ///
    /// The implementation of this visitor will be backend-specific, but a [`Resource`] can use this
    /// backend-agnostic interface to describe its structure and fields to any backend.
    pub trait ResourceVisitor<T: Resource>: Sized {
        /// An output summarizing the results of visiting `T`.
        type Output;

        /// Tell the visitor about a field `F` of type `T`.
        fn visit_field<F: Field<Resource = T>>(mut self) -> Self {
            self.visit_field_in_place::<F>();
            self
        }

        /// Tell the visitor about a field `F` of type `T`, mutating the visitor.
        fn visit_field_in_place<F: Field<Resource = T>>(&mut self);

        /// Tell the visitor about a many-to-one relation `R`.
        fn visit_many_to_one<R: ManyToOneRelation<Owner = T>>(mut self) -> Self {
            self.visit_many_to_one_in_place::<R>();
            self
        }

        /// Tell the visitor about a many-to-one relation `R`, mutating the visitor.
        fn visit_many_to_one_in_place<R: ManyToOneRelation<Owner = T>>(&mut self);

        /// Tell the visitor about a many-to-many relation `R`.
        fn visit_many_to_many<R: ManyToManyRelation<Owner = T>>(mut self) -> Self {
            self.visit_many_to_many_in_place::<R>();
            self
        }

        /// Tell the visitor about a many-to-many relation `R`, mutating the visitor.
        fn visit_many_to_many_in_place<R: ManyToManyRelation<Owner = T>>(&mut self);

        /// Finish visiting the type and collect the output.
        fn end(self) -> Self::Output;
    }

    /// Visitor which allows fields of a [`Resource`] to describe themselves to a backend.
    pub trait FieldVisitor<T: Resource> {
        /// An output summarizing the results of visiting `T`s fields.
        type Output;

        /// Tell the visitor about a field `F`.
        fn visit<F: Field<Resource = T>>(&mut self) -> Self::Output;
    }

    /// Visitor which allows a backend to visit the input fields of a [`Resource`].
    pub trait InputFieldVisitor<T: Resource> {
        /// An output summarizing the results of visiting `T`s input fields.
        type Output;

        /// Tell the visitor about a field `F`.
        fn visit<F: InputField<Resource = T>>(&mut self) -> Self::Output;
    }

    /// Visitor which allows relations of a [`Resource`] to describe themselves to a backend.
    pub trait RelationVisitor<T: Resource> {
        /// An output summarizing the results of visiting `T`s relations.
        type Output;

        /// Tell the visitor about a many-to-one relation `R`.
        fn visit_many_to_one<R: ManyToOneRelation<Owner = T>>(&mut self) -> Self::Output;

        /// Tell the visitor about a many-to-many relation `R`.
        fn visit_many_to_many<R: ManyToManyRelation<Owner = T>>(&mut self) -> Self::Output;
    }
}
