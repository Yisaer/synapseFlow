use crate::expr::{ProcStateField, ScalarExpr};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PipelineStateUsage {
    pub last_hit_count: bool,
    pub last_agg_hit_count: bool,
    pub last_hit_time_unix_ms: bool,
}

impl PipelineStateUsage {
    pub fn from_expr(expr: &ScalarExpr) -> Self {
        let mut usage = Self::default();
        usage.collect_expr(expr);
        usage
    }

    pub fn is_empty(self) -> bool {
        !self.last_hit_count && !self.last_agg_hit_count && !self.last_hit_time_unix_ms
    }

    pub fn merge(&mut self, other: Self) {
        self.last_hit_count |= other.last_hit_count;
        self.last_agg_hit_count |= other.last_agg_hit_count;
        self.last_hit_time_unix_ms |= other.last_hit_time_unix_ms;
    }

    fn collect_expr(&mut self, expr: &ScalarExpr) {
        match expr {
            ScalarExpr::PipelineState { field } | ScalarExpr::ProcessorState { field, .. } => {
                self.collect_field(field);
            }
            ScalarExpr::CallUnary { expr, .. } => self.collect_expr(expr),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                self.collect_expr(expr1);
                self.collect_expr(expr2);
            }
            ScalarExpr::FieldAccess { expr, .. } => self.collect_expr(expr),
            ScalarExpr::ListIndex { expr, index_expr } => {
                self.collect_expr(expr);
                self.collect_expr(index_expr);
            }
            ScalarExpr::CallFunc { args, .. } => {
                for arg in args {
                    self.collect_expr(arg);
                }
            }
            ScalarExpr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(expr) = operand {
                    self.collect_expr(expr);
                }
                for (when, then) in when_then {
                    self.collect_expr(when);
                    self.collect_expr(then);
                }
                if let Some(expr) = else_expr {
                    self.collect_expr(expr);
                }
            }
            _ => {}
        }
    }

    fn collect_field(&mut self, field: &ProcStateField) {
        match field {
            ProcStateField::LastHitCount => self.last_hit_count = true,
            ProcStateField::LastAggHitCount => self.last_agg_hit_count = true,
            ProcStateField::LastHitTimeUnixMs => self.last_hit_time_unix_ms = true,
        }
    }
}
