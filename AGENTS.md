# 仓库规范

## 文档与设计背景
- 本项目的功能设计背景与方案沉淀在 `docs/` 目录的 Markdown 文档中；在了解某个功能的背景/约束/方案时，优先查阅对应文档。
- 在 `docs/` 中新增文档时，语法（syntax）类内容需要单独分类/区分，便于后续查找与维护。

## 构建、测试与开发命令
- `make build` — 以 debug 模式编译整个 workspace。
- `make release` — 构建优化后的二进制产物到 `target/release/`。
- `make test` — 运行 workspace 测试以及各 crate 的本地套件（parser、flow、datatypes）。
- `make fmt`, `make clippy` — 执行格式化与 lint（`-D warnings`）。

## 代码风格与命名约定
- 运行 `make fmt`，遵循 Rustfmt 默认风格（4 空格缩进、尾随逗号）。
- 代码、注释与标识符统一使用英文;使用 `snake_case` 命名条目、`CamelCase` 命名类型、SCREAMING_SNAKE_CASE 命名常量。
- 遵守模块边界：parser 不依赖 flow；flow 只消费 `SelectStmt` 的输出（不直接消费原始 `sqlparser::Expr`）。

## 提交前自检守则

每次代码修改完成后、提交前，必须完成以下自检步骤。这些步骤来自历史 PR review 中反复出现的返工模式（遗漏调用点、文档漂移、修复不治本等），目的是在 reviewer 看到代码之前先由自己消灭低级问题。

### 变更影响面分析
- 当删除或重命名一个 public API（函数、类型、trait、enum variant、struct 字段、config 字段）时，必须用 `rg`（ripgrep）全局搜索该名称，确保没有残留的调用点、配置引用或文档引用。
- 对于「删除型」变更（如移除某个模块/功能），必须额外检查：
  - 所有 `tests/` 目录下的集成测试、e2e 测试、regression 测试、fuzzing 测试
  - 所有 `scripts/` 目录下的脚本（包括 shell 脚本中的变量引用和使用说明）
  - `config.yaml`、`etc/config.example.yaml`、`distros/` 下的所有配置文件
  - `docs/` 下所有 Markdown 文档
- 搜索时不要只搜精确匹配——考虑变体：snake_case / kebab-case / CamelCase、路径分隔符差异、YAML 缩进层级差异。

### 编译与测试验证
- 修改 public API 后，必须运行受影响的测试目标的编译检查：`cargo check --test <target>`（如 `cargo check --test regression_tests`、`cargo check --test fuzzing_tests`），确认没有编译错误后再提交。
- 如果使用了 sed 或脚本批量修改，必须在修改后运行编译检查——sed 不生效是真实发生过的教训。
- 修改 shell 脚本后，必须运行 `bash -n <script>` 做语法检查。
- 修改 config YAML 示例后，确认字段路径与相关脚本/文档中的引用一致（例如 cgroup 路径在 setup 脚本、config.yaml、README 中三处一致）。

### 文档同步
- 当修改了某个功能的运行时行为（API 返回值、错误处理策略、生命周期语义等），必须同步检查并更新 `docs/` 下相关文档中对该行为的描述。代码实现是最终的真相来源，文档必须跟随代码，不得出现「文档说 X，代码做 Y」的情况。
- 新增功能时，文档中描述的行为必须是当前 PR 实际实现的行为。不得在文档中描述尚未实现的设计规划（如"未来支持 tar.gz 导出"），除非明确标注为 deferred/future work。
- 删除功能时，必须删除或重写文档中对该功能的所有描述，不得留下孤立的章节或残缺的句子。

### 修复方式
- 面对 reviewer 指出的问题，要从根因层面修复，而不是在具体指出的路径上打补丁。
  - 示例（反面）：reviewer 指出内存上限未设置，只给 `host_alloc` 加了 growth guard，不思考初始内存和 guest `memory.grow` 是否也能绕过。
  - 示例（正面）：直接配置 wasmtime 的 `StoreLimits` / `ResourceLimiter`，一次性覆盖所有内存分配路径。
- 对于安全/资源限制类问题（sandbox、caps、bounds），不得使用正则匹配、substring 扫描等脆弱手段。优先使用结构化/语义化方法（如解析 AST、使用引擎原生限制 API）。
- 如果正确的修复方案工作量较大，需要在 commit message 中说明当前方案的局限性，并在 `docs/` 中明确标注为 deferred/future work，不得假装问题已解决。

## 沟通原则
- 与维护者沟通时统一使用中文（需求澄清、方案确认、评审反馈、变更说明等）。

## 编码守则
- 每次写代码前，必须先将代码修改的设计方案与我确认；未经确认不得直接改代码。
- 每次修改完 Rust 代码后，必须立即运行 `make fmt` 与 `make clippy` 检查，确认通过后才能继续下一步。
- 修改代码时只能在当前分支上进行，严禁切换分支；所有改动都在用户当前所处的分支上完成。
- 不用关心 `git status`，交给用户自己操作。
- 开发完成的代码必须通过 `make fmt` 与 `make clippy`；提交/合并前优先用这两个命令自检（clippy 以 `-D warnings` 运行）。
- 任何代码注释以及 `docs/` 下的文档编写均应使用英文。
- 对同一 struct 的同一职责，尽量维护单一方法入口；允许但克制使用 `with_*` 变体，避免因少量参数差异扩散出多个近似方法。需要可选项/扩展点时，优先使用 Options/Config struct、枚举参数，或独立的 builder struct。
- 当需要我们生成 PR title 和 PR description 时，PR title 必须通过 CI 的 `PR Title Lint`（Conventional Commits 规范）。
  - 格式：`<type>(<scope>): <subject>`（`(<scope>)` 可选；breaking change 可用 `!`，例如：`<type>(<scope>)!: <subject>` 或 `<type>!: <subject>`）。
  - `type`/`scope` 使用小写；常用 `type`：`feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert`。
  - 示例：`feat(flow): add ...`、`fix(parser): handle ...`、`docs: update ...`。
  - PR title 和 PR description 必须使用英文撰写。
- 当使用 `gh` 处理 PR review comment 后，如产生本地改动，需要主动提交 commit；提交必须使用 `git commit -s` 保持签名一致，并在 commit message body 中说明处理方式与取舍思路。
- 当 CI 中不熟悉的工具（如 Kani、cargo-deny 等）报错时，必须优先查阅该工具的官方文档理解其工作原理和已知限制，再制定修复方案；禁止仅凭错误信息字面意思猜测 flag 或参数。

## 测试守则
- 不要求每次修改代码后都运行 `make test` 做全量验证；优先运行与改动ni相关的最小测试集合（例如仅运行新增/受影响的单元测试）。
- 在修复 `bench` 相关代码时，永远不要本地运行 `cargo bench`。
- 如需新增单元测试：先向我确认该单元测试的验证用例（case）与预期结果，再进行实现与运行。
- 若修改涉及 `flow` 模块，尤其是 `planner` 相关逻辑，需额外验证 `src/flow/tests/planner/` 下的测试用例。
- `src/flow/tests/planner/` 下的测试用例采用 table-driven 风格：以 SQL 作为主要输入参数，以 `EXPLAIN` 的 JSON 输出作为主要断言依据；同时需要打印 SQL 与 explain 的 pretty string，方便人工阅读与排查。
- `src/flow/tests/pipeline/` 下维护基于 SQL 的输入/输出验证测试，采用 table-driven 风格；新增类似“验证输入/输出”的测试时，优先考虑以新增 case 的方式补充到这里。

## 各模块开发守则
- `parser` 模块：负责将 SQL 解析为 AST；解析结果存放在 `SelectStmt` 中，作为后续模块交互的契约。
- 修改 `parser` 模块相关代码时，可优先阅读 `docs/` 下与语法（syntax）相关的文档。
- `flow` 模块（planner/optimizer）：对于 logical optimize 与 physical optimize 的每一条 rule，优先在 `docs/planner_optimize/logical/` 与 `docs/planner_optimize/physical/` 查找对应设计文档，理解约束与适用边界；再结合 `src/flow/tests/planner/` 的 `EXPLAIN` JSON 断言验证规则效果（如需新增 case，先与维护者确认），确认无误后将结论/注意事项补充回 `AGENTS.md`。
- 固定输出布局审计结论：sink 最终列统一由 planner 的 `OutputLayout` 描述；direct column/alias 继承 message 固定槽位，计算列使用有序 affiliate 槽位。encoder、row-diff、template 与 memory materialize 不得重新按 source/name 猜测布局，也不得恢复 `ByIndexProjection`、`late_projection` 或 `passthrough_messages` 兼容路径；fan-in 布局不兼容时应在规划阶段失败。
- `processor` 模块：processor 指实际处理流数据的算子；每个 processor 处理完数据后会将数据转交给下一个算子。
- 通道约束：每个 processor 有两类订阅通道 `control channel` 与 `data channel`；从哪个 channel 收到的数据，就只能转发到同类型的 channel（不得混用/串线）。
- `decoder` / `encoder`：`decoder processor` 负责将 `[]byte` 转为 `Collection`/`Tuple`；`encoder` 负责将 `Collection`/`Tuple` 转为 `[]byte`。
- 性能约束：processor 处理 `Collection`/`Tuple` 时尽可能避免值拷贝（优先借用/引用或零拷贝结构）。
- Hot path 约束：对于 processor 中的高频路径（尤其是 `partition key` 相关逻辑），避免使用 `format!("{:?}", key_values)` 这类写法；这类实现通常会同时引入 `Vec<Value>` 分配、`String` 分配和 `Debug` 格式化开销，容易成为明显的 CPU 热点。优先保持结构化 key，并直接使用可 `Hash` / `Eq` 的数据结构作为 key。
