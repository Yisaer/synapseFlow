use crate::expr::custom_func::CustomFuncRegistry;
use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan, PipelineStateUsage};
use crate::processor::processor_state::ProcessorState;
use sqlparser::ast::Expr;
use std::sync::Arc;

/// Field definition for physical projection
///
/// Contains both the original SQL expression and the compiled ScalarExpr for execution
#[derive(Debug, Clone)]
pub struct PhysicalProjectField {
    /// Output field name
    pub field_name: Arc<str>,
    /// Original SQL expression from parser (for reference and debugging)
    pub original_expr: Expr,
    /// Compiled expression for execution
    pub compiled_expr: ScalarExpr,
}

/// Physical operator for projection operations
///
/// This operator represents the physical execution of projection operations,
/// evaluating expressions and producing output with projected fields.
#[derive(Debug, Clone)]
pub struct PhysicalProject {
    pub base: BasePhysicalPlan,
    pub fields: Arc<[PhysicalProjectField]>,
    /// Processor-local state for pipeline state functions (e.g. `last_hit_count`).
    /// When `Some`, `ProjectProcessor` increments the counter after each projected row.
    pub processor_state: Option<Arc<ProcessorState>>,
    pub pipeline_state_usage: PipelineStateUsage,
}

impl PhysicalProjectField {
    /// Create a new PhysicalProjectField with both original and compiled expressions
    pub fn new(
        field_name: impl Into<Arc<str>>,
        original_expr: Expr,
        compiled_expr: ScalarExpr,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            original_expr,
            compiled_expr,
        }
    }

    /// Create from a logical ProjectField by compiling the expression
    pub fn from_logical(
        field_name: impl Into<Arc<str>>,
        original_expr: Expr,
        bindings: &crate::expr::sql_conversion::SchemaBinding,
        custom_func_registry: &CustomFuncRegistry,
    ) -> Result<Self, String> {
        // Compile the sqlparser expression to ScalarExpr
        let compiled_expr =
            crate::expr::sql_conversion::convert_expr_to_scalar_with_bindings_and_custom_registry(
                &original_expr,
                bindings,
                custom_func_registry,
            )
            .map_err(|e| format!("Failed to compile expression: {}", e))?;

        Ok(Self {
            field_name: field_name.into(),
            original_expr,
            compiled_expr,
        })
    }
}

impl PhysicalProject {
    /// Create a new PhysicalProject
    pub fn new(
        fields: Vec<PhysicalProjectField>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            fields: fields.into(),
            processor_state: None,
            pipeline_state_usage: PipelineStateUsage::default(),
        }
    }

    /// Create a new PhysicalProject with a single child
    pub fn with_single_child(
        fields: Vec<PhysicalProjectField>,
        child: Arc<PhysicalPlan>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(vec![child], index);
        Self {
            base,
            fields: fields.into(),
            processor_state: None,
            pipeline_state_usage: PipelineStateUsage::default(),
        }
    }
}
