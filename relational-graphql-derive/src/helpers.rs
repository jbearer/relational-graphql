//! Helper functions for implementing procedural macros.

use itertools::Itertools;
use proc_macro2::TokenStream;
use std::borrow::Borrow;
use syn::{
    parenthesized,
    parse::{Parse, Parser},
    Attribute, Expr, Ident, Lit, Meta,
};

/// Convenient parsing interface for helper attributes.
pub struct AttrParser(String);

impl AttrParser {
    /// Create a new parser for attributes in `scope`, the top-level identifier of attributes parsed
    /// by this parser.
    ///
    /// This parser will parse any attribute of the form `#[scope(name)]` (boolean attributes) or
    /// `#[scope(name(arg))]` (argument attributes).
    pub fn new(scope: impl Into<String>) -> Self {
        Self(scope.into())
    }

    /// Check if a list of attributes has a certain boolean attribute.
    ///
    /// # Panics
    ///
    /// Panics if `attrs` has an attribute in this scope which is malformed (e.g. it's name is
    /// `self.scope()` but its meta attribute does not start with an identifier).
    pub fn has_bool<I>(&self, attrs: I, name: &str) -> bool
    where
        I: IntoIterator,
        I::Item: Borrow<Attribute>,
    {
        attrs
            .into_iter()
            .any(|a| self.is_bool_attr(a.borrow(), name))
    }

    /// Check if a list of attributes has a certain attribute and return its argument.
    ///
    /// # Panics
    ///
    /// Panics if `attrs` has an attribute in this scope which is malformed (e.g. it's name is
    /// `self.scope()` but its meta attribute does not start with an identifier) or if the attribute
    /// exists but its argument does not parse as a `T`.
    pub fn get_arg<T: Parse, I>(&self, attrs: I, name: &str) -> Option<T>
    where
        I: IntoIterator,
        I::Item: Borrow<Attribute>,
    {
        attrs
            .into_iter()
            .find_map(|a| self.parse_arg(a.borrow(), name))
    }

    /// Check if `a` is a certain attribute and return its argument.
    ///
    /// If `a` is a valid attribute in this scope with name `name` and one parameter, this function
    /// parses and returns the parameter. Otherwise, if `a` is not in scope _or_ if `a` is not the
    /// desired attribute, it returns [`None`].
    ///
    /// # Panics
    ///
    /// Panics if `a` is in scope but malformed (e.g. it's name is `self.scope()` but its meta
    /// attribute does not start with an identifier) or if the attribute argument does not parse as
    /// a `T`.
    pub fn parse_arg<T: Parse>(&self, a: &Attribute, name: &str) -> Option<T> {
        self.parse_arg_with(a, name, T::parse)
    }

    /// Check if `a` is a certain attribute and return its argument.
    ///
    /// If `a` is a valid attribute in this scope with name `name` and one parameter, this function
    /// parses and returns the parameter. Otherwise, if `a` is not in scope _or_ if `a` is not the
    /// desired attribute, it returns [`None`].
    ///
    /// # Panics
    ///
    /// Panics if `a` is in scope but malformed (e.g. it's name is `self.scope()` but its meta
    /// attribute does not start with an identifier) or if the attribute argument does not parse
    /// using `p`.
    pub fn parse_arg_with<P: Parser>(&self, a: &Attribute, name: &str, p: P) -> Option<P::Output> {
        let Some((attr_name, Some(param))) = self.parse_attr(a) else { return None };
        if attr_name != name {
            return None;
        }
        Some(p.parse2(param).unwrap_or_else(|_| {
            panic!(
                "argument to {} must be a {}",
                name,
                std::any::type_name::<P::Output>(),
            )
        }))
    }

    /// Check if `a` is a certain boolean attribute in this scope.
    ///
    /// If `a` is a valid attribute in this scope with name `name` and no parameters, this function
    /// returns [`true`]. Otherwise, if `a` is not in scope _or_ if `a` is not the desired boolean
    /// attribute, it returns [`false`].
    ///
    /// # Panics
    ///
    /// Panics if `a` is in scope but malformed (e.g. it's name is `self.scope()` but its meta
    /// attribute does not start with an identifier).
    pub fn is_bool_attr(&self, a: &Attribute, name: &str) -> bool {
        if let Some((attr_name, None)) = self.parse_attr(a) {
            attr_name == name
        } else {
            false
        }
    }

    /// Parse an attribute in this scope, returning its name and parameter.
    ///
    /// If `a` is a valid attribute in this scope, this function returns the attribute name and
    /// parameter. For example, parsing the attribute `#[scope(foo(bar))]` would result in
    /// `Some("foo", Some("bar"))`. Parsing `#[scope(baz)]` would result in `Some("baz", None).`
    ///
    /// If `a` is not in scope, the result is [`None`].
    ///
    /// # Panics
    ///
    /// Panics if `a` is in scope but malformed (e.g. it's name is `self.scope()` but its meta
    /// attribute does not start with an identifier).
    pub fn parse_attr(&self, a: &Attribute) -> Option<(Ident, Option<TokenStream>)> {
        let mut parsed = None;
        if a.path().is_ident(&self.0) {
            a.parse_nested_meta(|meta| {
                let name = meta.path.get_ident().unwrap_or_else(|| {
                    panic!("{} attribute must start with an identifier", self.0)
                });
                let content = if meta.input.is_empty() {
                    None
                } else {
                    let content;
                    parenthesized!(content in meta.input);
                    Some(content.parse().unwrap())
                };
                parsed = Some((name.clone(), content));
                Ok(())
            })
            .unwrap();
        }
        parsed
    }
}

/// Extract documentation from the attributes on an item.
///
/// The documentation is constructed by taking all the `#[doc = "..."]` attributes and concatenating
/// their arguments, separated by newlines.
pub fn parse_docs(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            let Meta::NameValue(nv) = &attr.meta else { return None; };
            if !nv.path.is_ident("doc") {
                return None;
            }
            let Expr::Lit(lit) = &nv.value else { return None; };
            let Lit::Str(s) = &lit.lit else { return None; };
            Some(s.value().trim().to_string())
        })
        .join("\n")
}
