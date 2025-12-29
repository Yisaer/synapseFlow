use serde::{Deserialize, Serialize};

/// Categorization of SQL-visible functions in SynapseFlow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionKind {
    /// Stateless scalar function evaluated per row (e.g. `concat(a, b)`).
    Scalar,
    /// Aggregate function evaluated over a group/window (e.g. `sum(x)`).
    Aggregate,
    /// Stateful per-row function with evolving internal state (e.g. `lag(x)`).
    Stateful,
}

/// Where a function call is allowed to appear in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionContext {
    Select,
    Where,
    Having,
    GroupBy,
}

/// Type specification used for function signatures in the function catalog.
///
/// This is intentionally lightweight and machine-readable for agents. It is not
/// required to match internal Rust types exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TypeSpec {
    Any,
    /// A concrete type name as exposed by schema introspection (e.g. `int64`, `string`).
    Named {
        name: String,
    },
    /// A high-level type category (e.g. `numeric`, `string`).
    Category {
        name: String,
    },
    List {
        element: Box<TypeSpec>,
    },
    Struct {
        fields: Vec<StructFieldSpec>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructFieldSpec {
    pub name: String,
    pub r#type: TypeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArgSpec {
    pub name: String,
    pub r#type: TypeSpec,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub variadic: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionSignatureSpec {
    pub args: Vec<FunctionArgSpec>,
    pub return_type: TypeSpec,
}

/// Additional constraints/requirements that affect how a function can be used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FunctionRequirement {
    /// The function must be used in an aggregation context (e.g. `SELECT sum(x) ... GROUP BY ...`).
    AggregateContext,
    /// The function call requires deterministic row order.
    DeterministicOrder,
    /// The function call requires `PARTITION BY` to be specified by the user/query.
    RequiresPartitionBy,
    /// The function call requires an event-time column / event-time semantics.
    RequiresEventTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateFunctionSpec {
    /// Whether this aggregate supports incremental (streaming) updates.
    #[serde(default)]
    pub supports_incremental: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulFunctionSpec {
    /// Short description of the state semantics (what state is kept and how it evolves).
    pub state_semantics: String,
}

/// Function catalog entry returned by capability introspection APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDef {
    pub kind: FunctionKind,
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub signature: FunctionSignatureSpec,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub allowed_contexts: Vec<FunctionContext>,
    #[serde(default)]
    pub requirements: Vec<FunctionRequirement>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub examples: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateFunctionSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stateful: Option<StatefulFunctionSpec>,
}
