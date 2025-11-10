# Flow Crate - SQL 表达式转换与计算

## 概述

Flow crate 提供了从 SQL 表达式到可计算 ScalarExpr 的完整转换功能，打通了 SQL 解析和表达式计算的壁垒。

## 核心功能

### 1. SQL 到 ScalarExpr 转换

```rust
use flow::sql_conversion::extract_select_expressions;

let sql = "SELECT a + b";
let expressions = extract_select_expressions(sql)?;
// expressions[0] 是可以计算的 ScalarExpr
```

### 2. 支持的表达式类型

| SQL 表达式 | ScalarExpr 表示 | 说明 |
|-----------|----------------|------|
| `a + b` | `CallBinary(Add, ...)` | 加法运算 |
| `a - b` | `CallBinary(Sub, ...)` | 减法运算 |
| `a * b` | `CallBinary(Mul, ...)` | 乘法运算 |
| `a / b` | `CallBinary(Div, ...)` | 除法运算 |
| `42` | `Literal(Int64(42), Int64Type)` | 整数字面量 |
| `'hello'` | `Literal(String("hello"), StringType)` | 字符串字面量 |
| `CONCAT(a,b)` | `CallDf("concat", [args])` | 函数调用 |
| `(a + b) * c` | 嵌套的 `CallBinary` | 复合表达式 |

### 3. 表达式计算

```rust
use flow::expr::{DataFusionEvaluator, ScalarExpr, BinaryFunc};
use datatypes::{Value, ConcreteDatatype, Int64Type};

// 创建表达式: 5 + 3
let expr = ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type))
    .call_binary(
        ScalarExpr::Literal(Value::Int64(3), ConcreteDatatype::Int64(Int64Type)),
        BinaryFunc::Add
    );

// 计算结果
let evaluator = DataFusionEvaluator::new();
let result = expr.eval(&evaluator, &tuple)?;
// result = Value::Int64(8)
```

## 模块结构

```
src/
├── lib.rs                    # 库入口，导出公共 API
├── sql_conversion.rs         # SQL 到 ScalarExpr 转换器
├── expr/                     # 表达式计算模块
│   ├── mod.rs
│   ├── context.rs           # 计算上下文
│   ├── evaluator.rs         # DataFusion 评估器
│   ├── func.rs              # 函数定义（二元、一元）
│   ├── scalar.rs            # ScalarExpr 定义
│   └── datafusion_adapter.rs # DataFusion 适配器
├── row.rs                    # 行数据处理
└── tuple.rs                  # 元组处理
```

## 主要 API

### SQL 转换 API

```rust
// 在 sql_conversion 模块中
pub fn extract_select_expressions(sql: &str) -> Result<Vec<ScalarExpr>, ConversionError>
pub fn convert_expr_to_scalar(expr: &sqlparser::ast::Expr) -> Result<ScalarExpr, ConversionError>
```

### 表达式计算 API

```rust
// 在 expr 模块中
impl ScalarExpr {
    pub fn eval(&self, evaluator: &DataFusionEvaluator, row: &dyn Row) -> Result<Value, EvalError>
    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self
    pub fn call_unary(self, func: UnaryFunc) -> Self
}
```

## 使用示例

### 完整工作流程

```rust
use flow::sql_conversion::extract_select_expressions;
use flow::expr::{DataFusionEvaluator, ScalarExpr};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Value, Schema};

// 1. 解析 SQL
let sql = "SELECT a + b, 42";
let expressions = extract_select_expressions(sql)?;

// 2. 准备数据（实际使用时需要正确的 schema 和数据）
let schema = Schema::new(vec![]);
let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]);
let tuple = Tuple::new(schema, row);

// 3. 计算表达式
let evaluator = DataFusionEvaluator::new();
for expr in &expressions {
    let result = expr.eval(&evaluator, &tuple)?;
    println!("Result: {:?}", result);
}
```

### 创建自定义表达式

```rust
use flow::expr::{ScalarExpr, BinaryFunc};
use datatypes::{Value, ConcreteDatatype, Int64Type};

// 创建字面量
let literal = ScalarExpr::Literal(Value::Int64(10), ConcreteDatatype::Int64(Int64Type));

// 创建二元操作
let addition = literal.call_binary(
    ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type)),
    BinaryFunc::Add
);
```

## 错误处理

转换和计算过程中可能遇到的错误：

- `ConversionError::UnsupportedExpression` - 不支持的表达式类型
- `ConversionError::UnsupportedOperator` - 不支持的操作符
- `ConversionError::TypeConversionError` - 类型转换错误
- `EvalError` - 表达式计算错误

## 测试

运行 flow crate 的测试：

```bash
cargo test --package flow
```

## 扩展性

可以通过以下方式扩展功能：

1. **添加新操作符**: 在 `convert_binary_op` 和 `convert_unary_op` 中添加
2. **支持新表达式**: 在 `convert_expr_to_scalar` 中添加新的 match 分支
3. **自定义函数**: 扩展示有的函数调用处理逻辑