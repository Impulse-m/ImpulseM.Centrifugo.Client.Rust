//! Publication tags filter (docs/spec.md §7.9), mirroring `centrifuge-go`.
//!
//! ```
//! use centrifuge_client_core::filter::Filter;
//!
//! let f = Filter::and([Filter::eq("type", "goal"), Filter::in_("team", ["a", "b"])]);
//! # let _ = f;
//! ```

use crate::proto;

/// A tags filter expression, built with [`Filter`].
#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode(pub(crate) proto::FilterNode);

impl FilterNode {
    /// The wire form.
    #[must_use]
    pub fn to_proto(&self) -> proto::FilterNode {
        self.0.clone()
    }
}

/// Builders for [`FilterNode`].
#[derive(Debug)]
pub struct Filter;

fn leaf(key: impl Into<String>, cmp: &str, val: Option<String>, vals: Vec<String>) -> FilterNode {
    FilterNode(proto::FilterNode {
        key: key.into(),
        cmp: cmp.to_owned(),
        val: val.unwrap_or_default(),
        vals,
        ..Default::default()
    })
}

fn logical(op: &str, nodes: impl IntoIterator<Item = FilterNode>) -> FilterNode {
    FilterNode(proto::FilterNode {
        op: op.to_owned(),
        nodes: nodes.into_iter().map(|n| n.0).collect(),
        ..Default::default()
    })
}

macro_rules! cmp {
    ($($(#[$doc:meta])* $name:ident => $cmp:literal),* $(,)?) => {
        $(
            $(#[$doc])*
            #[must_use]
            pub fn $name(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
                leaf(key, $cmp, Some(val.into()), Vec::new())
            }
        )*
    };
}

impl Filter {
    cmp! {
        /// `tags[key] == val`.
        eq => "eq",
        /// `tags[key] != val`.
        neq => "neq",
        /// `tags[key]` starts with `val`.
        starts_with => "sw",
        /// `tags[key]` ends with `val`.
        ends_with => "ew",
        /// `tags[key]` contains `val`.
        contains => "ct",
        /// `tags[key] > val`, numerically.
        gt => "gt",
        /// `tags[key] >= val`, numerically.
        gte => "gte",
        /// `tags[key] < val`, numerically.
        lt => "lt",
        /// `tags[key] <= val`, numerically.
        lte => "lte",
    }

    /// `tags[key]` is one of `vals`.
    #[must_use]
    pub fn in_<V: Into<String>>(
        key: impl Into<String>,
        vals: impl IntoIterator<Item = V>,
    ) -> FilterNode {
        leaf(key, "in", None, vals.into_iter().map(Into::into).collect())
    }

    /// `tags[key]` is none of `vals`.
    #[must_use]
    pub fn nin<V: Into<String>>(
        key: impl Into<String>,
        vals: impl IntoIterator<Item = V>,
    ) -> FilterNode {
        leaf(key, "nin", None, vals.into_iter().map(Into::into).collect())
    }

    /// `key` is present.
    #[must_use]
    pub fn exists(key: impl Into<String>) -> FilterNode {
        leaf(key, "ex", None, Vec::new())
    }

    /// `key` is absent.
    #[must_use]
    pub fn not_exists(key: impl Into<String>) -> FilterNode {
        leaf(key, "nex", None, Vec::new())
    }

    /// All of `nodes`.
    #[must_use]
    pub fn and(nodes: impl IntoIterator<Item = FilterNode>) -> FilterNode {
        logical("and", nodes)
    }

    /// Any of `nodes`.
    #[must_use]
    pub fn or(nodes: impl IntoIterator<Item = FilterNode>) -> FilterNode {
        logical("or", nodes)
    }

    /// Not `node`.
    #[must_use]
    pub fn not(node: FilterNode) -> FilterNode {
        logical("not", [node])
    }
}
