use crate::expr::custom_func::CustomFunc;
use crate::expr::func::{BinaryFunc, EvalError, UnaryFunc};
use crate::model::{Collection, Tuple};
use datatypes::{ConcreteDatatype, StructField, StructType, StructValue, Value};
use std::sync::Arc;

/// A scalar expression, which can be evaluated to a value.
#[derive(Clone)]
pub enum ScalarExpr {
    /// A column reference by source name and column name
    Column {
        source_name: String,
        column_name: String,
    },
    /// Wildcard reference (`*` or `source.*`)
    Wildcard {
        /// Optional source/table qualifier
        source_name: Option<String>,
    },
    /// A literal value with its type
    Literal(Value, ConcreteDatatype),
    /// A unary function call
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A binary function call
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    /// A field access expression (e.g., a.b where a is a struct)
    FieldAccess {
        /// The expression that evaluates to a struct value
        expr: Box<ScalarExpr>,
        /// The name of the field to access
        field_name: String,
    },
    /// A list indexing expression (e.g., a[0] where a is a list)
    ListIndex {
        /// The expression that evaluates to a list value
        expr: Box<ScalarExpr>,
        /// The index expression (can be any scalar expression)
        index_expr: Box<ScalarExpr>,
    },
    /// A call to a custom user-implemented function
    CallFunc {
        /// The custom function implementation
        func: Arc<dyn CustomFunc>,
        /// The arguments to the function
        args: Vec<ScalarExpr>,
    },
}

impl ScalarExpr {
    /// Evaluate this expression against a collection row by row.
    pub fn eval_with_collection(
        &self,
        collection: &dyn Collection,
    ) -> Result<Vec<Value>, EvalError> {
        let mut results = Vec::with_capacity(collection.num_rows());
        for row in collection.rows() {
            results.push(self.eval_with_tuple(row)?);
        }
        Ok(results)
    }

    /// Evaluate this expression against a single tuple (row).
    pub fn eval_with_tuple(&self, tuple: &Tuple) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column {
                source_name,
                column_name,
            } => {
                if let Some(value) = tuple.value_by_name(source_name, column_name) {
                    Ok(value.clone())
                } else if let Some(value) = tuple.value_by_column(column_name) {
                    Ok(value.clone())
                } else {
                    Err(EvalError::ColumnNotFound {
                        source: source_name.clone(),
                        column: column_name.clone(),
                    })
                }
            }
            ScalarExpr::Wildcard { source_name } => {
                let selected: Vec<_> = tuple
                    .columns
                    .iter()
                    .zip(tuple.values.iter())
                    .filter(|((src, _), _)| match source_name {
                        Some(prefix) => src == prefix,
                        None => true,
                    })
                    .collect();

                if selected.is_empty() && source_name.is_some() {
                    return Err(EvalError::ColumnNotFound {
                        source: source_name.clone().unwrap_or_default(),
                        column: "*".to_string(),
                    });
                }

                let mut fields = Vec::with_capacity(selected.len());
                let mut values = Vec::with_capacity(selected.len());
                for ((_, column_name), value) in selected {
                    fields.push(StructField::new(
                        column_name.clone(),
                        value.datatype(),
                        true,
                    ));
                    values.push(value.clone());
                }

                Ok(Value::Struct(StructValue::new(
                    values,
                    StructType::new(Arc::new(fields)),
                )))
            }
            ScalarExpr::Literal(val, _) => Ok(val.clone()),
            ScalarExpr::CallUnary { func, expr } => {
                let arg = expr.eval_with_tuple(tuple)?;
                func.eval_unary(arg)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let left = expr1.eval_with_tuple(tuple)?;
                let right = expr2.eval_with_tuple(tuple)?;
                func.eval_binary(left, right)
            }
            ScalarExpr::FieldAccess { expr, field_name } => {
                let struct_val = expr.eval_with_tuple(tuple)?;
                if let Value::Struct(struct_val) = struct_val {
                    struct_val.get_field(field_name).cloned().ok_or_else(|| {
                        EvalError::FieldNotFound {
                            field_name: field_name.clone(),
                            struct_type: format!("{:?}", struct_val.fields()),
                        }
                    })
                } else {
                    Err(EvalError::TypeMismatch {
                        expected: "Struct".to_string(),
                        actual: format!("{:?}", struct_val),
                    })
                }
            }
            ScalarExpr::ListIndex { expr, index_expr } => {
                let list_val = expr.eval_with_tuple(tuple)?;
                let index_val = index_expr.eval_with_tuple(tuple)?;

                match (list_val, index_val) {
                    (Value::List(list), Value::Int64(index)) => {
                        if index < 0 || (index as usize) >= list.len() {
                            return Err(EvalError::ListIndexOutOfBounds {
                                index: index as usize,
                                list_length: list.len(),
                            });
                        }
                        list.get(index as usize)
                            .cloned()
                            .ok_or(EvalError::ListIndexOutOfBounds {
                                index: index as usize,
                                list_length: list.len(),
                            })
                    }
                    (Value::List(_), other) => Err(EvalError::InvalidIndexType {
                        expected: "Int64".to_string(),
                        actual: format!("{:?}", other),
                    }),
                    (other, _) => Err(EvalError::TypeMismatch {
                        expected: "List".to_string(),
                        actual: format!("{:?}", other),
                    }),
                }
            }
            ScalarExpr::CallFunc { func, args } => {
                let mut arg_vectors = Vec::with_capacity(args.len());
                for arg_expr in args {
                    arg_vectors.push(vec![arg_expr.eval_with_tuple(tuple)?]);
                }
                func.validate_vectorized(&arg_vectors)?;
                let results = func.eval_vectorized(&arg_vectors)?;
                results.into_iter().next().ok_or(EvalError::NotImplemented {
                    feature: "Custom function returned no result".to_string(),
                })
            }
        }
    }

    /// Create a column reference expression by source and column name
    pub fn column(source_name: impl Into<String>, column_name: impl Into<String>) -> Self {
        ScalarExpr::Column {
            source_name: source_name.into(),
            column_name: column_name.into(),
        }
    }

    /// Create a literal expression
    pub fn literal(value: Value, typ: ConcreteDatatype) -> Self {
        ScalarExpr::Literal(value, typ)
    }

    /// Create an unqualified wildcard expression (`*`)
    pub fn wildcard_all() -> Self {
        ScalarExpr::Wildcard { source_name: None }
    }

    /// Create a qualified wildcard expression (`source.*`)
    pub fn wildcard_for(source_name: impl Into<String>) -> Self {
        ScalarExpr::Wildcard {
            source_name: Some(source_name.into()),
        }
    }

    /// Create a unary function call expression
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    /// Create a binary function call expression
    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    /// Create a custom function call expression
    pub fn call_func(func: Arc<dyn CustomFunc>, args: Vec<ScalarExpr>) -> Self {
        ScalarExpr::CallFunc { func, args }
    }

    /// Create a field access expression (e.g., a.b where a is a struct)
    pub fn field_access(expr: ScalarExpr, field_name: impl Into<String>) -> Self {
        ScalarExpr::FieldAccess {
            expr: Box::new(expr),
            field_name: field_name.into(),
        }
    }

    /// Create a list indexing expression (e.g., a[0] where a is a list)
    pub fn list_index(expr: ScalarExpr, index_expr: ScalarExpr) -> Self {
        ScalarExpr::ListIndex {
            expr: Box::new(expr),
            index_expr: Box::new(index_expr),
        }
    }

    /// Check if this expression is a column reference
    pub fn is_column(&self) -> bool {
        matches!(self, ScalarExpr::Column { .. })
    }

    /// Get the source and column names if this is a column reference
    pub fn as_column(&self) -> Option<(&str, &str)> {
        if let ScalarExpr::Column {
            source_name,
            column_name,
        } = self
        {
            Some((source_name, column_name))
        } else {
            None
        }
    }

    /// Check if this expression is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(..))
    }

    /// Get the literal value if this is a literal expression
    pub fn as_literal(&self) -> Option<&Value> {
        if let ScalarExpr::Literal(val, _) = self {
            Some(val)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarExpr::Column {
                source_name,
                column_name,
            } => write!(f, "Column({}.{})", source_name, column_name),
            ScalarExpr::Wildcard { source_name } => {
                write!(f, "Wildcard({:?})", source_name)
            }
            ScalarExpr::Literal(val, typ) => write!(f, "Literal({:?}, {:?})", val, typ),
            ScalarExpr::CallUnary { func, expr } => write!(f, "CallUnary({:?}, {:?})", func, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                write!(f, "CallBinary({:?}, {:?}, {:?})", func, expr1, expr2)
            }
            ScalarExpr::FieldAccess { expr, field_name } => {
                write!(f, "FieldAccess({:?}, {})", expr, field_name)
            }
            ScalarExpr::ListIndex { expr, index_expr } => {
                write!(f, "ListIndex({:?}, {:?})", expr, index_expr)
            }
            ScalarExpr::CallFunc { func, args } => {
                write!(f, "CallFunc({}, {:?})", func.name(), args)
            }
        }
    }
}

impl PartialEq for ScalarExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                ScalarExpr::Column {
                    source_name: sa,
                    column_name: ca,
                },
                ScalarExpr::Column {
                    source_name: sb,
                    column_name: cb,
                },
            ) => sa == sb && ca == cb,
            (
                ScalarExpr::Wildcard { source_name: sa },
                ScalarExpr::Wildcard { source_name: sb },
            ) => sa == sb,
            (ScalarExpr::Literal(va, ta), ScalarExpr::Literal(vb, tb)) => va == vb && ta == tb,
            (
                ScalarExpr::CallUnary { func: fa, expr: ea },
                ScalarExpr::CallUnary { func: fb, expr: eb },
            ) => fa == fb && ea == eb,
            (
                ScalarExpr::CallBinary {
                    func: fa,
                    expr1: e1a,
                    expr2: e2a,
                },
                ScalarExpr::CallBinary {
                    func: fb,
                    expr1: e1b,
                    expr2: e2b,
                },
            ) => fa == fb && e1a == e1b && e2a == e2b,
            (
                ScalarExpr::FieldAccess {
                    expr: ea,
                    field_name: na,
                },
                ScalarExpr::FieldAccess {
                    expr: eb,
                    field_name: nb,
                },
            ) => ea == eb && na == nb,
            (
                ScalarExpr::ListIndex {
                    expr: ea,
                    index_expr: ia,
                },
                ScalarExpr::ListIndex {
                    expr: eb,
                    index_expr: ib,
                },
            ) => ea == eb && ia == ib,
            (
                ScalarExpr::CallFunc { func: fa, args: aa },
                ScalarExpr::CallFunc { func: fb, args: ab },
            ) => fa.name() == fb.name() && aa == ab,
            _ => false,
        }
    }
}
