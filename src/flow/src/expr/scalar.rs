use datatypes::{ConcreteDatatype, Value};

use crate::expr::custom_func::CustomFunc;
use crate::expr::datafusion_func::DataFusionEvaluator;
use crate::expr::func::{BinaryFunc, EvalError, UnaryFunc};
use crate::model::Collection;
use std::sync::Arc;

/// A scalar expression, which can be evaluated to a value.
#[derive(Clone)]
pub enum ScalarExpr {
    /// A column reference by source name and column name
    Column {
        source_name: String,
        column_name: String,
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
    /// A call to a DataFusion scalar function
    CallDf {
        /// The name of the DataFusion function (e.g., "concat", "upper", "lower")
        function_name: String,
        /// The arguments to the function
        args: Vec<ScalarExpr>,
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
    /// Evaluate this expression against a collection using vectorized evaluation.
    /// This method performs vectorized evaluation where one call can process multiple rows.
    ///
    /// # Arguments
    ///
    /// * `evaluator` - The DataFusion evaluator for handling CallDf expressions
    /// * `collection` - The collection containing data (can be row-based or column-based)
    ///
    /// # Returns
    ///
    /// Returns a vector of evaluated values, one for each row in the collection.
    pub fn eval_with_collection(
        &self,
        evaluator: &DataFusionEvaluator,
        collection: &dyn Collection,
    ) -> Result<Vec<Value>, EvalError> {
        self.eval_vectorized(evaluator, collection)
    }

    /// Core vectorized evaluation implementation
    /// This is about vectorized computation, not necessarily columnar storage
    pub fn eval_vectorized(
        &self,
        _evaluator: &DataFusionEvaluator,
        collection: &dyn Collection,
    ) -> Result<Vec<Value>, EvalError> {
        match self {
            ScalarExpr::Column {
                source_name,
                column_name,
            } => collection
                .column_by_name(source_name, column_name)
                .or_else(|| collection.column_by_name("", column_name))
                .map(|col| col.values().to_vec())
                .ok_or_else(|| EvalError::ColumnNotFound {
                    source: source_name.clone(),
                    column: column_name.clone(),
                }),
            ScalarExpr::Literal(val, _) => {
                // Literal value - create a vector of the same value
                Ok(vec![val.clone(); collection.num_rows()])
            }
            ScalarExpr::CallUnary { func, expr } => {
                // Vectorized unary operation
                let args = expr.eval_vectorized(_evaluator, collection)?;
                args.into_iter().map(|arg| func.eval_unary(arg)).collect()
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                // Vectorized binary operation
                let left_vals = expr1.eval_vectorized(_evaluator, collection)?;
                let right_vals = expr2.eval_vectorized(_evaluator, collection)?;

                if left_vals.len() != right_vals.len() || left_vals.len() != collection.num_rows() {
                    return Err(EvalError::NotImplemented {
                        feature: "Vector length mismatch in binary operation".to_string(),
                    });
                }

                left_vals
                    .into_iter()
                    .zip(right_vals)
                    .map(|(left, right)| func.eval_binary(left, right))
                    .collect()
            }
            ScalarExpr::FieldAccess { expr, field_name } => {
                // Vectorized field access
                let struct_vals = expr.eval_vectorized(_evaluator, collection)?;
                struct_vals
                    .into_iter()
                    .map(|struct_val| {
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
                    })
                    .collect()
            }
            ScalarExpr::ListIndex { expr, index_expr } => {
                // Vectorized list indexing
                let list_vals = expr.eval_vectorized(_evaluator, collection)?;
                let index_vals = index_expr.eval_vectorized(_evaluator, collection)?;

                if list_vals.len() != index_vals.len() || list_vals.len() != collection.num_rows() {
                    return Err(EvalError::NotImplemented {
                        feature: "Vector length mismatch in list indexing".to_string(),
                    });
                }

                list_vals
                    .into_iter()
                    .zip(index_vals)
                    .map(|(list_val, index_val)| {
                        if let Value::List(list_val) = list_val {
                            if let Value::Int64(index) = index_val {
                                if index >= 0 && (index as usize) < list_val.len() {
                                    Ok(list_val.get(index as usize).unwrap().clone())
                                } else {
                                    Err(EvalError::ListIndexOutOfBounds {
                                        index: index as usize,
                                        list_length: list_val.len(),
                                    })
                                }
                            } else {
                                Err(EvalError::InvalidIndexType {
                                    expected: "Int64".to_string(),
                                    actual: format!("{:?}", index_val),
                                })
                            }
                        } else {
                            Err(EvalError::TypeMismatch {
                                expected: "List".to_string(),
                                actual: format!("{:?}", list_val),
                            })
                        }
                    })
                    .collect()
            }
            #[cfg(feature = "datafusion")]
            ScalarExpr::CallDf {
                function_name,
                args,
            } => {
                // Vectorized DataFusion function evaluation
                // Prepare arguments as vectors (one vector per argument, containing all rows)
                let mut arg_vectors = Vec::new();
                for arg_expr in args {
                    arg_vectors.push(arg_expr.eval_vectorized(_evaluator, collection)?);
                }

                // Validate vector dimensions
                let num_rows = collection.num_rows();
                for (i, arg_vec) in arg_vectors.iter().enumerate() {
                    if arg_vec.len() != num_rows {
                        return Err(EvalError::NotImplemented {
                            feature: format!(
                                "Argument {} has {} values, expected {}",
                                i,
                                arg_vec.len(),
                                num_rows
                            ),
                        });
                    }
                }

                // Use DataFusion's native vectorized evaluation
                evaluator
                    .evaluate_df_function_vectorized(function_name, &arg_vectors, collection)
                    .map_err(|df_error| EvalError::DataFusionError {
                        message: df_error.to_string(),
                    })
            }
            #[cfg(not(feature = "datafusion"))]
            ScalarExpr::CallDf { function_name, .. } => Err(EvalError::DataFusionError {
                message: format!(
                    "Function '{}' requires enabling the 'datafusion' feature",
                    function_name
                ),
            }),
            ScalarExpr::CallFunc { func, args } => {
                // Vectorized custom function evaluation
                // Prepare arguments as vectors (one vector per argument, containing all rows)
                let mut arg_vectors = Vec::new();
                for arg_expr in args {
                    arg_vectors.push(arg_expr.eval_vectorized(_evaluator, collection)?);
                }

                // Validate vector dimensions
                let num_rows = collection.num_rows();
                for (i, arg_vec) in arg_vectors.iter().enumerate() {
                    if arg_vec.len() != num_rows {
                        return Err(EvalError::NotImplemented {
                            feature: format!(
                                "Argument {} has {} values, expected {}",
                                i,
                                arg_vec.len(),
                                num_rows
                            ),
                        });
                    }
                }

                // Use vectorized validation and evaluation
                func.validate_vectorized(&arg_vectors)?;
                func.eval_vectorized(&arg_vectors)
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

    /// Create a DataFusion function call expression
    pub fn call_df(function_name: impl Into<String>, args: Vec<ScalarExpr>) -> Self {
        ScalarExpr::CallDf {
            function_name: function_name.into(),
            args,
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
            ScalarExpr::CallDf {
                function_name,
                args,
            } => {
                write!(f, "CallDf({}, {:?})", function_name, args)
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
                ScalarExpr::CallDf {
                    function_name: na,
                    args: aa,
                },
                ScalarExpr::CallDf {
                    function_name: nb,
                    args: ab,
                },
            ) => na == nb && aa == ab,
            (
                ScalarExpr::CallFunc { func: fa, args: aa },
                ScalarExpr::CallFunc { func: fb, args: ab },
            ) => {
                // Compare custom functions by name and arguments
                fa.name() == fb.name() && aa == ab
            }
            _ => false,
        }
    }
}
