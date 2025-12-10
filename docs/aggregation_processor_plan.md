## 聚合处理设计与落地步骤（草案）

### 背景与目标
- 当前 parser 与 PhysicalAggregation 分别维护聚合函数名单，存在重复与漂移；processor 侧缺少执行实现。
- 目标：在 flow 内集中管理聚合函数注册表，parser/planner/processor 共用；定义统一聚合函数 trait；实现 AggregationProcessor 支持 PhysicalAggregation 的执行，现阶段先支持 `sum`。

### 核心思路
- 在 flow 实例侧新增 `AggregateFunctionRegistry`，内建函数集中注册，默认注册 `sum`；对 parser 暴露只读接口（`AggregateRegistry`）。
- 引入 `RegistryManager`（或扩展现有 PipelineRegistries）统一提供各类 registry，减少 parser/planner/processor 间的参数传递。
- 定义聚合接口：`AggregateFunction`（含 `name`、`return_type`、`create_accumulator`），`AggregateAccumulator`（`update`/`merge`/`finalize`）。
- PhysicalAggregation 仅保存函数名字符串与参数；planner 通过 registry 校验函数存在与类型可行性；parser 用 registry 判断 SQL 中的标识符是否为聚合函数。
- Processor 侧新增 `AggregationProcessor`：根据 PhysicalAggregation 的 `aggregate_calls` 从 registry 拉取实现并执行累计，结束时写入 affiliate 的 `output_column`。

### 模块与接口设计
- `AggregateFunctionRegistry`：集中存放实现，小写函数名为 key；提供注册/查找，默认注册 `sum`。实现 parser 侧需要的 `AggregateRegistry`（只判断是否聚合函数）。
- `RegistryManager`：封装 aggregate_registry（后续可扩展 codec 等），FlowInstance 构造时创建默认实例，parser/planner/processor 通过 manager 获取。
- `AggregateFunction`/`AggregateAccumulator`：
  - `AggregateFunction`：`name()`、`return_type(input_schema, args)`、`create_accumulator() -> AggregateAccumulator`。
  - `AggregateAccumulator`：`update(row)`、`merge(other)`、`finalize() -> ScalarValue`。
- `AggregationProcessor`：初始化为每个 `AggregateCall` 从 registry 获取实现与 accumulator；对流数据逐行 `update`，在 `StreamGracefulEnd`（或流结束）时 `finalize`，将结果按 `output_column` 写入 affiliate 并输出 RecordBatch；收到其他控制信号透传或重置；distinct 暂不支持直接报错。

### 数据流与校验
- Parser：解析 SQL 时借助 registry 判断 `SUM/COUNT...` 是否为聚合函数，不再硬编码。
- Planner：PhysicalAggregation 与 `AggregateCall` 使用函数名字符串；构建时用 registry 校验存在性与类型。
- Processor：执行时缺失注册表或未注册函数直接报错；使用 accumulator 完成累计与产出。

### 实现步骤（建议顺序）
1. 在 flow 新增聚合模块：定义 `AggregateFunction`/`AggregateAccumulator` trait，`AggregateFunctionRegistry`，并注册内置 `sum`。
2. 添加 `RegistryManager`（或扩展 PipelineRegistries）：集中持有 aggregate_registry，FlowInstance 构造时创建默认实例。
3. Parser 接入：新增 `AggregateRegistry` trait；`parse_sql`/visitor 接口接受 registry；解析时用 registry 判断聚合函数。
4. Planner 接入：PhysicalAggregation/`AggregateCall` 改用函数名字符串并用 registry 校验；plan 构建接口通过 RegistryManager 取得 aggregate_registry。
5. Processor 实现：新增 `AggregationProcessor` 按 PhysicalAggregation 执行聚合，写入 affiliate，distinct/未注册函数报错。
6. 测试与验证：更新现有解析/计划单测以传入默认 registry；新增端到端管线用例（如 `SELECT sum(a) AS total FROM stream`）；运行 `make test` 或针对性 crate 测试。

### 后续扩展
- 在 registry 中扩展更多聚合（count/avg/min/max 等），补齐 distinct 支持与类型推断。
- 视需要增加窗口/group key 支持、多聚合并行优化，以及错误信息细化。
