# Parser Crate - SQL 解析与表达式提取

## 概述

Parser crate 专注于 SQL 解析和表达式提取，提供了从 SQL 语句中提取和分析表达式的工具。

## 核心功能

### 1. SQL 表达式提取

```rust
use parser::{extract_expressions_from_sql, extract_select_expressions_simple};

// 提取所有表达式
let sql = "SELECT a + b, CONCAT(name, 'test') FROM users WHERE age > 18";
let expressions = extract_expressions_from_sql(sql)?;

// 提取 SELECT 表达式
let select_exprs = extract_select_expressions_simple(sql)?;
```

### 2. SQL 表达式分析

```rust
use parser::analyze_sql_expressions;

let sql = "SELECT a + b, CONCAT(name, 'test'), 42 WHERE x > 10";
let analysis = analyze_sql_expressions(sql)?;

println!("Total expressions: {}", analysis.expression_count);
println!("Binary operations: {:?}", analysis.binary_operations);
println!("Functions: {:?}", analysis.functions);
println!("Literals: {:?}", analysis.literals);
```

### 3. 自定义 SQL Dialect

```rust
use parser::StreamDialect;
use sqlparser::parser::Parser;

let dialect = StreamDialect::new();
let statements = Parser::parse_sql(&dialect, sql)?;
```

## 模块结构

```
src/
├── lib.rs                    # 库入口，导出公共 API
├── expression_extractor.rs   # 表达式提取和分析工具
├── dialect.rs               # 自定义 SQL dialect
└── window.rs                # 窗口函数支持
```

## 主要 API

### 表达式提取

```rust
// 提取 SQL 中所有表达式
pub fn extract_expressions_from_sql(sql: &str) -> Result<Vec<String>, String>

// 提取 SELECT 语句中的表达式
pub fn extract_select_expressions_simple(sql: &str) -> Result<Vec<String>, String>

// 分析 SQL 表达式结构
pub fn analyze_sql_expressions(sql: &str) -> Result<ExpressionAnalysis, String>
```

### 分析结果结构

```rust
pub struct ExpressionAnalysis {
    pub sql: String,                    // 原始 SQL
    pub expression_count: usize,         // 表达式总数
    pub binary_operations: Vec<String>,  // 二元操作列表
    pub literals: Vec<String>,           // 字面量列表
    pub functions: Vec<String>,          // 函数调用列表
}
```

## 使用示例

### 基本表达式提取

```rust
use parser::extract_expressions_from_sql;

let sql = "SELECT a + b FROM users";
match extract_expressions_from_sql(sql) {
    Ok(expressions) => {
        for expr in expressions {
            println!("Found expression: {}", expr);
        }
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

### 表达式分析

```rust
use parser::analyze_sql_expressions;

let sql = "SELECT a + b, CONCAT(name, suffix), 42 WHERE age > 18";
match analyze_sql_expressions(sql) {
    Ok(analysis) => {
        println!("Analysis of: {}", analysis.sql);
        println!("Total expressions: {}", analysis.expression_count);
        println!("Binary operations: {:?}", analysis.binary_operations);
        println!("Functions: {:?}", analysis.functions);
        println!("Literals: {:?}", analysis.literals);
    }
    Err(e) => eprintln!("Analysis error: {}", e),
}
```

### 使用自定义 Dialect

```rust
use parser::StreamDialect;
use sqlparser::parser::Parser;

let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)";
let dialect = StreamDialect::new();

match Parser::parse_sql(&dialect, sql) {
    Ok(statements) => {
        println!("Successfully parsed {} statements", statements.len());
    }
    Err(e) => eprintln!("Parse error: {}", e),
}
```

## 支持的 SQL 特性

### 表达式类型
- ✅ 二元操作：`a + b`, `a - b`, `a * b`, `a / b`
- ✅ 比较操作：`a = b`, `a > b`, `a < b`, `a >= b`, `a <= b`
- ✅ 逻辑操作：`AND`, `OR`, `NOT`
- ✅ 字面量：数字、字符串、布尔值
- ✅ 函数调用：`CONCAT()`, `UPPER()`, 等
- ✅ 嵌套表达式：`(a + b) * c`

### 高级特性
- ✅ 自定义方言支持
- ✅ 窗口函数（tumblingwindow）
- ✅ 表达式访问者模式

## 错误处理

所有函数都返回 `Result<T, String>`，错误信息清晰描述问题：

```rust
match extract_expressions_from_sql("INVALID SQL") {
    Ok(_) => println!("Success"),
    Err(e) => println!("Error: {}", e), // 详细的错误信息
}
```

## 性能考虑

1. **Visitor 模式**: 使用高效的访问者模式遍历 AST
2. **字符串格式化**: 延迟格式化，只在需要时进行
3. **错误收集**: 一次性收集所有错误信息

## 测试

运行 parser crate 的测试：

```bash
cargo test --package parser
```

## 扩展指南

要添加新的表达式提取功能：

1. 在 `expression_extractor.rs` 中创建新的访问者
2. 实现 `Visitor` trait 的相应方法
3. 在 `lib.rs` 中导出新的函数

例如，添加 WHERE 子句表达式提取：

```rust
struct WhereClauseExtractor {
    where_exprs: Vec<String>,
}

impl Visitor for WhereClauseExtractor {
    // 实现相应方法
}
```