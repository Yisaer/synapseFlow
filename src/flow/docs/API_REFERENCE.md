# Flow Crate API 参考

## SQL 转换模块 (sql_conversion)

### 主要函数

#### `extract_select_expressions`
从 SELECT 语句中提取表达式并转换为 ScalarExpr。

```rust
pub fn extract_select_expressions(sql: &str) -> Result<Vec<ScalarExpr>, ConversionError>
```

**参数**: SQL 字符串
**返回**: ScalarExpr 向量或转换错误

**示例**:
```rust
let sql = "SELECT a + b, 42";
let expressions = extract_select_expressions(sql)?;
```

#### `convert_expr_to_scalar`
将 sqlparser 的 Expr 转换为 ScalarExpr。

```rust
pub fn convert_expr_to_scalar(expr: &sqlparser::ast::Expr) -> Result<ScalarExpr, ConversionError>
```

**参数**: sqlparser 表达式
**返回**: ScalarExpr 或转换错误

### 错误类型

#### `ConversionError`
```rust
pub enum ConversionError {
    UnsupportedExpression(String),
    UnsupportedOperator(String), 
    TypeConversionError(String),
}
```

## 表达式模块 (expr)

### ScalarExpr

#### 构造函数

```rust
// 列引用
ScalarExpr::column(index: usize)

// 字面量
ScalarExpr::literal(value: Value, typ: ConcreteDatatype)

// 二元操作
ScalarExpr::call_binary(self, other: Self, func: BinaryFunc)

// 一元操作
ScalarExpr::call_unary(self, func: UnaryFunc)

// DataFusion 函数调用
ScalarExpr::call_df(function_name: String, args: Vec<ScalarExpr>)
```

#### 计算方法

```rust
pub fn eval(&self, evaluator: &DataFusionEvaluator, row: &dyn Row) -> Result<Value, EvalError>
```

### BinaryFunc

支持的二元操作：

```rust
pub enum BinaryFunc {
    Add,    // +
    Sub,    // -
    Mul,    // *
    Div,    // /
    Mod,    // %
    Eq,     // ==
    NotEq,  // !=
    Lt,     // <
    Lte,    // <=
    Gt,     // >
    Gte,    // >=
}
```

### UnaryFunc

支持的一元操作：

```rust
pub enum UnaryFunc {
    Not,        // 逻辑非
    IsNull,     // 是否为 NULL
    IsTrue,     // 是否为真
    IsFalse,    // 是否为假
    Cast(ConcreteDatatype),  // 类型转换
}
```

### DataFusionEvaluator

```rust
impl DataFusionEvaluator {
    pub fn new() -> Self
    pub fn evaluate_expr(&self, expr: &ScalarExpr, row: &dyn Row) -> Result<Value, DataFusionError>
}
```

## 完整示例

```rust
use flow::sql_conversion::{extract_select_expressions, ConversionError};
use flow::expr::{ScalarExpr, BinaryFunc, DataFusionEvaluator, EvalError};
use flow::tuple::Tuple;
use datatypes::{Value, ConcreteDatatype, Int64Type};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 解析 SQL
    let sql = "SELECT a + b, 42";
    let expressions = extract_select_expressions(sql)?;
    
    // 2. 创建评估器
    let evaluator = DataFusionEvaluator::new();
    let schema = datatypes::Schema::new(vec![]);
    let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]);
    let tuple = Tuple::new(schema, row);
    
    // 3. 计算每个表达式
    for (i, expr) in expressions.iter().enumerate() {
        match expr.eval(&evaluator, &tuple) {
            Ok(value) => println!("Expression {}: {:?}", i, value),
            Err(e) => eprintln!("Error evaluating expression {}: {:?}", i, e),
        }
    }
    
    Ok(())
}
```

## 性能考虑

1. **表达式缓存**: 对于重复计算的表达式，考虑缓存结果
2. **批量处理**: 一次处理多个表达式比单独处理更高效
3. **类型检查**: 在转换阶段进行类型检查，避免运行时错误

## 调试技巧

```rust
// 打印表达式结构
println!("Expression: {:?}", expr);

// 检查表达式类型
match expr {
    ScalarExpr::CallBinary { func, expr1, expr2 } => {
        println!("Binary operation: {:?}", func);
    }
    ScalarExpr::Literal(val, typ) => {
        println!("Literal: {:?} of type {:?}", val, typ);
    }
    _ => {}
}
```