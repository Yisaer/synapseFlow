# Parser Crate - 表达式分析指南

## 概述

本文档详细说明如何使用 parser crate 来分析和理解 SQL 表达式的结构。

## 表达式提取流程

### 1. SQL 解析阶段

```rust
use sqlparser::parser::Parser;
use parser::StreamDialect;

let dialect = StreamDialect::new();
let statements = Parser::parse_sql(&dialect, sql)?;
```

### 2. 表达式访问阶段

```rust
use sqlparser::ast::{Visit, Visitor};

struct MyExpressionVisitor {
    expressions: Vec<String>,
}

impl Visitor for MyExpressionVisitor {
    type Break = ();
    
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        self.expressions.push(format!("{:?}", expr));
        ControlFlow::Continue(())
    }
}
```

### 3. 结果分析阶段

```rust
let mut visitor = MyExpressionVisitor::new();
for statement in &statements {
    statement.visit(&mut visitor);
}
```

## 表达式类型分析

### 二元操作表达式

```sql
SELECT a + b, price * quantity, total / count
```

对应的 AST 结构：
```rust
Expr::BinaryOp {
    left: Box<Expr>,
    op: BinaryOperator::Plus,  // Plus, Minus, Multiply, Divide, etc.
    right: Box<Expr>,
}
```

使用分析器：
```rust
let analysis = analyze_sql_expressions("SELECT a + b, c * d")?;
assert_eq!(analysis.binary_operations, vec!["Plus", "Multiply"]);
```

### 函数调用表达式

```sql
SELECT CONCAT(first_name, ' ', last_name), UPPER(email)
```

对应的 AST 结构：
```rust
Expr::Function {
    name: ObjectName(vec![Ident::new("CONCAT")]),
    args: vec![FunctionArg::Unnamed(...), FunctionArg::Unnamed(...)],
    // ...
}
```

使用分析器：
```rust
let analysis = analyze_sql_expressions("SELECT CONCAT(a, b), UPPER(c)")?;
assert_eq!(analysis.functions, vec!["concat", "upper"]);
```

### 字面量表达式

```sql
SELECT 42, 'hello', 3.14, true, NULL
```

对应的 AST 结构：
```rust
Expr::Value(Value::Number("42", false))
Expr::Value(Value::SingleQuotedString("hello"))
Expr::Value(Value::Boolean(true))
Expr::Value(Value::Null)
```

使用分析器：
```rust
let analysis = analyze_sql_expressions("SELECT 42, 'hello', true")?;
assert_eq!(analysis.literals.len(), 3);
```

### 嵌套表达式

```sql
SELECT (a + b) * c, total / (count - 1)
```

分析器会递归处理嵌套结构：
```rust
let analysis = analyze_sql_expressions("SELECT (a + b) * c")?;
assert_eq!(analysis.binary_operations, vec!["Multiply", "Plus"]);
```

## 高级分析技术

### 表达式复杂度分析

```rust
struct ComplexityAnalyzer {
    max_depth: usize,
    current_depth: usize,
    operation_count: usize,
}

impl Visitor for ComplexityAnalyzer {
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        self.current_depth += 1;
        self.max_depth = self.max_depth.max(self.current_depth);
        
        if matches!(expr, Expr::BinaryOp { .. }) {
            self.operation_count += 1;
        }
        
        ControlFlow::Continue(())
    }
    
    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.current_depth -= 1;
        ControlFlow::Continue(())
    }
}
```

### 列引用分析

```rust
struct ColumnAnalyzer {
    column_references: Vec<String>,
}

impl Visitor for ColumnAnalyzer {
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => {
                self.column_references.push(ident.value.clone());
            }
            Expr::CompoundIdentifier(idents) => {
                let full_name = idents.iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                self.column_references.push(full_name);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
```

### 函数依赖分析

```rust
struct FunctionDependencyAnalyzer {
    function_calls: HashMap<String, Vec<String>>,
}

impl Visitor for FunctionDependencyAnalyzer {
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name = func.name.to_string();
            let mut args = Vec::new();
            
            for arg in &func.args {
                // 提取参数信息
                args.push(format!("{:?}", arg));
            }
            
            self.function_calls.insert(func_name, args);
        }
        ControlFlow::Continue(())
    }
}
```

## 实际应用案例

### 案例 1：查询复杂度评估

```rust
pub fn assess_query_complexity(sql: &str) -> Result<QueryComplexity, String> {
    let analysis = analyze_sql_expressions(sql)?;
    
    Ok(QueryComplexity {
        expression_count: analysis.expression_count,
        binary_op_count: analysis.binary_operations.len(),
        function_count: analysis.functions.len(),
        literal_count: analysis.literals.len(),
        complexity_score: calculate_complexity_score(&analysis),
    })
}
```

### 案例 2：列依赖分析

```rust
pub fn analyze_column_dependencies(sql: &str) -> Result<Vec<String>, String> {
    let mut analyzer = ColumnDependencyAnalyzer::new();
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    
    for statement in &statements {
        statement.visit(&mut analyzer)?;
    }
    
    Ok(analyzer.get_dependencies())
}
```

### 案例 3：表达式验证

```rust
pub fn validate_expressions(sql: &str) -> Result<Vec<ExpressionIssue>, String> {
    let mut validator = ExpressionValidator::new();
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    
    for statement in &statements {
        statement.visit(&mut validator)?;
    }
    
    Ok(validator.get_issues())
}
```

## 最佳实践

### 1. 组合多个分析器

```rust
pub fn comprehensive_analysis(sql: &str) -> Result<ComprehensiveAnalysis, String> {
    let complexity = assess_query_complexity(sql)?;
    let dependencies = analyze_column_dependencies(sql)?;
    let issues = validate_expressions(sql)?;
    
    Ok(ComprehensiveAnalysis {
        complexity,
        dependencies,
        issues,
    })
}
```

### 2. 错误处理

```rust
impl Visitor for RobustAnalyzer {
    type Break = AnalysisError;
    
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        // 详细的错误检查
        if let Err(e) = self.validate_expression(expr) {
            return ControlFlow::Break(AnalysisError::ValidationError(e));
        }
        
        ControlFlow::Continue(())
    }
}
```

### 3. 性能优化

```rust
// 缓存解析结果
lazy_static! {
    static ref SQL_CACHE: Mutex<HashMap<String, ParsedSql>> = Mutex::new(HashMap::new());
}

pub fn cached_analysis(sql: &str) -> Result<ExpressionAnalysis, String> {
    let mut cache = SQL_CACHE.lock().unwrap();
    
    if let Some(parsed) = cache.get(sql) {
        return analyze_parsed_sql(parsed);
    }
    
    let parsed = parse_sql(sql)?;
    let analysis = analyze_parsed_sql(&parsed)?;
    
    cache.insert(sql.to_string(), parsed);
    Ok(analysis)
}
```

## 总结

Parser crate 提供了强大的 SQL 表达式分析能力，通过访问者模式可以：

1. **提取**各种类型的表达式
2. **分析**表达式的结构和复杂度
3. **验证**表达式的正确性
4. **优化**查询性能

这些功能为构建更高级的 SQL 处理工具奠定了基础。