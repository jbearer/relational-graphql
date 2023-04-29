//! Relational GraphQL is an ORM designed to efficiently translate GraphQL queries into queries
//! against a relational database. It consists of two sections:
//!
//! * A frontend, which most users will interact with, for defining [graphql] schemas. A [graphql]
//!   schema describes clients' view of the application's data model. It provides an ontology that
//!   clients can use to conceptualize the various entities and their relationships as well as an
//!   expressive language for querying the data.
//! * A [backend](graphql::backend), which is responsible for translating GraphQL queries into
//!   queries against relational databases. The backend implementation is completely agnostic to the
//!   specifics of the applications data model. It uses a generic
//!   [type system](graphql::type_system) describing the GraphQL API in order to automatically
//!   compile GraphQL objects and queries into database operations. This makes it possible to write
//!   one general backend and reuse it with many GraphQL applications.
//!
//! This crate comes with a [sql] backend, which provides a PostgreSQL target as well as a mock
//! database, which is useful for lightweight testing. The library is completely modular, though, so
//! it is possible to implement your own backend to meet your application's specific needs by
//! implementing the [backend](graphql::backend) traits.

use derivative::Derivative;
use derive_more::{Deref, DerefMut, From, Into};
use generic_array::{
    typenum::{UInt, UTerm, Unsigned, B0, B1},
    ArrayLength, GenericArray,
};
use std::fmt::{self, Debug, Formatter};
use std::sync::Once;
use tracing_subscriber::EnvFilter;

pub use generic_array::typenum;

pub mod graphql;
pub mod prelude;
pub mod sql;

/// Initialize tracing.
pub fn init_logging() {
    static ONCE: Once = Once::new();

    ONCE.call_once(|| {
        color_eyre::install().unwrap();
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    });
}

/// A convenience for working with [`GenericArray`].
///
/// The trait [`ArrayLength`] is parameterized by the type of element in the array, which makes it
/// impossible to take as a generic parameter a length which can be used with any type of
/// [`GenericArray`] -- you can't write `N: for<T> ArrayLength<T>`. This is a result of the generic
/// array library predating GATs.
///
/// With GATs, it is perfectly possible to reframe the [`ArrayLength`] trait without parameterizing
/// on the type of array elements, as this trait demonstrates. Now it is possible to write
/// `N: Length` and use like `Array<usize, N>` and `Array<String, N>`.
pub trait Length: Unsigned {
    /// The length of an array of `T`.
    type Of<T>: ArrayLength<T>;
}

impl Length for UTerm {
    type Of<T> = Self;
}

impl<N: Length> Length for UInt<N, B0> {
    type Of<T> = UInt<N::Of<T>, B0>;
}

impl<N: Length> Length for UInt<N, B1> {
    type Of<T> = UInt<N::Of<T>, B1>;
}

/// An array of type `T` with constant length `N`.
#[derive(Derivative, Deref, DerefMut, From, Into, PartialEq, Eq)]
#[derivative(Clone(bound = "T: Clone"), Default(bound = "T: Default"))]
pub struct Array<T, N: Length>(GenericArray<T, N::Of<T>>);

impl<T, N: Length> IntoIterator for Array<T, N> {
    type IntoIter = <GenericArray<T, N::Of<T>> as IntoIterator>::IntoIter;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T, N: Length> IntoIterator for &'a Array<T, N> {
    type IntoIter = <&'a GenericArray<T, N::Of<T>> as IntoIterator>::IntoIter;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}

impl<'a, T, N: Length> IntoIterator for &'a mut Array<T, N> {
    type IntoIter = <&'a mut GenericArray<T, N::Of<T>> as IntoIterator>::IntoIter;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        (&mut self.0).into_iter()
    }
}

impl<T, N, B, const M: usize> From<[T; M]> for Array<T, UInt<N, B>>
where
    UInt<N, B>: Length,
    GenericArray<T, <UInt<N, B> as Length>::Of<T>>: From<[T; M]>,
{
    fn from(arr: [T; M]) -> Self {
        Self(GenericArray::from(arr))
    }
}

impl<T> From<[T; 0]> for Array<T, UTerm> {
    fn from(arr: [T; 0]) -> Self {
        Self::from_exact_iter(arr).unwrap()
    }
}

impl<T, N: Length> Array<T, N> {
    /// Creates a new [`Array`] instance from an iterator with a specific size.
    ///
    /// Returns [`None`] if the size is not equal to the number of elements in the [`Array`].
    pub fn from_exact_iter<I>(iter: I) -> Option<Self>
    where
        I: IntoIterator<Item = T>,
    {
        GenericArray::from_exact_iter(iter).map(Self)
    }

    /// Maps an [`Array`] to another [`Array`] with the same length.
    pub fn map<U, F>(self, f: F) -> Array<U, N>
    where
        F: FnMut(T) -> U,
    {
        Array::from_exact_iter(self.into_iter().map(f)).unwrap()
    }

    /// Permutes the contents of `self` by `permutation`.
    ///
    /// This method will reorder the elements of `self` in place using `permutation`, which maps
    /// indices in `self` to their new positions in the permuted version of self.
    ///
    /// # Panics
    ///
    /// Panics if `permutation` is not a permutation of the integers `0..N`.
    pub fn permute(&mut self, permutation: &Array<usize, N>) {
        // The indices in `permutation` we have visited.
        let mut visited = Array::<bool, N>::default();

        // The lowest index in the next cycle within `permutation`.
        let mut next = 0;

        // Permute cycles in `permutation` until there are none left.
        while next < N::USIZE {
            // Permute the first cycle we haven't permuted yet. We can permute a cycle in place by
            // always swapping the next element in the cycle into the slot vacated by the previous.
            let start = next;
            let mut i = start;
            loop {
                visited[i] = true;

                let j = permutation[i];
                assert!(j < N::USIZE, "not a permutation");

                if j == start {
                    // We finished the cycle.
                    break;
                }

                self.swap(i, j);
                i = j;
            }

            // Find the start of the next cycle.
            while next < N::USIZE && visited[next] {
                next += 1;
            }
        }
    }

    /// Join two arrays of the same length into a single array of pairs of elements from each.
    pub fn zip<U>(self, other: Array<U, N>) -> Array<(T, U), N> {
        Array::from_exact_iter(self.into_iter().zip(other)).unwrap()
    }
}

impl<T, U, N: Length> Array<(T, U), N> {
    /// Split an array of pairs into a pair of arrays.
    pub fn unzip(self) -> (Array<T, N>, Array<U, N>) {
        let (l, r): (Vec<_>, Vec<_>) = self.into_iter().unzip();
        (
            Array::from_exact_iter(l).unwrap(),
            Array::from_exact_iter(r).unwrap(),
        )
    }
}

impl<T: Debug, N: Length> Debug for Array<T, N> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// Create an [`Array`] from a list of items.
///
/// # Examples
///
/// ```
/// # use relational_graphql::{array, typenum::U3, Array};
/// let arr: Array<usize, U3> = array![usize; 3, 2, 1];
/// assert_eq!(format!("{arr:?}"), "[3, 2, 1]");
/// ```
#[macro_export]
macro_rules! array {
    [$t:ty; $($v:expr),* $(,)?] => {
        $crate::Array::from(generic_array::arr![$t; $($v),*])
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::{prelude::*, test_runner::Config};
    use typenum::{U0, U1, U10};

    #[test]
    fn test_array_permute_empty() {
        let mut arr: Array<usize, U0> = array![usize;];
        arr.permute(&array![usize;]);
        assert_eq!(arr, array![usize;]);
    }

    #[test]
    fn test_array_permute_one() {
        let mut arr: Array<usize, U1> = array![usize; 0];
        arr.permute(&array![usize; 0]);
        assert_eq!(arr, array![usize; 0]);
    }

    fn array<T: Arbitrary>() -> impl Strategy<Value = Array<T, U10>> {
        [
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
            any::<T>(),
        ]
        .prop_map(Array::from)
    }

    fn permutation() -> impl Strategy<Value = Array<usize, U10>> {
        any::<Vec<(usize, usize)>>().prop_map(|swaps| {
            let mut perm = array![usize; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            for (i, j) in swaps {
                perm.swap(i % 10, j % 10);
            }
            perm
        })
    }

    proptest! {
        #![proptest_config(Config {
            timeout: 100,
            ..Default::default()
        })]

        #[test]
        fn test_array_permute_nonempty(perm in permutation()) {
            let mut array = array![usize; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            array.permute(&perm);
            prop_assert_eq!(array, perm);
        }

        #[test]
        fn test_zip_unzip_inverse(a in array::<usize>(), b in array::<String>()) {
            prop_assert_eq!(a.clone().zip(b.clone()).unzip(), (a, b));
        }
    }
}
