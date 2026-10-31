# TimeSeek 报告提纲

## 0. 报告定位与核心结论

### 0.1 报告标题建议

- TimeSeek: 面向 Apache IoTDB 的数据智能体系统设计与实践
- 副标题建议: 基于 MCP、Skills 与 workflow 编排的 coding + data agent 框架

### 0.2 报告目标

- 说明 TimeSeek 为什么不是“单纯把大模型接到数据库上”，而是一个围绕 IoTDB 场景构建的可治理数据智能体系统。
- 说明 TimeSeek 如何在 Apache IoTDB 上抽象 Agent 接口，包括 MCP 工具接口与 Skills 工作流接口。
- 说明 TimeSeek 如何结合 `opencode` 构建 coding + data agent 的统一运行底座。
- 说明 TimeSeek 如何通过业务 MCP、workspace、workflow、trace 将“能力”转化为“可复用产品流程”。
- 说明当前阶段的独特工作、与已有相关工作的差异、后续优化方向与产品可扩展空间。

### 0.3 核心观点

- TimeSeek 的核心创新不在单点模型能力，而在于围绕 IoTDB 形成了“接口层 + 编排层 + 业务层 + 可观测层”的完整闭环。
- MCP 将 IoTDB 原子能力工具化，Skills 与 workflow 将业务流程模板化，workspace 将任务上下文产品化，trace 将 agent 执行过程可审计化。
- 与通用数据库 Agent 相比，TimeSeek 更强调面向时序数据场景的结构化流程、SQL 治理、数据导入治理与报告输出治理。
- TimeSeek 具备从 demo/workflow 原型向行业化数据智能产品演进的条件。

### 0.4 建议的报告总结构

- 第一章: Apache IoTDB 的 Agent 接口
- 第二章: 基于 opencode 的 coding + data agent
- 第三章: TimeSeek 的业务 MCP 模块设计
- 第四章: TimeSeek 的 workspace 和 workflow 设计
- 第五章: Agent trace 与后续优化方向
- 第六章: 总结与产品扩展展望

## 1. Apache IoTDB 的 Agent 接口: Apache IoTDB MCP 和 IoTDB Skills

### 1.1 为什么 Apache IoTDB 需要 Agent 接口层

- IoTDB 面向时序数据、设备模型、树模型/表模型、数据导入、规则分析、运维诊断等多样任务，天然适合 Agent 化调用。
- 传统数据库驱动接口偏“命令式调用”，不适合大模型进行能力发现、约束执行和工具组合。
- 因此需要在 IoTDB 之上构建面向 Agent 的标准接口层，将数据库能力从“API/SQL”提升为“工具/技能”。

### 1.2 Agent 接口分层: MCP 与 Skills

- MCP: 面向模型可调用的细粒度工具接口，承载具体操作能力。
- Skills: 面向复杂任务复用的工作流接口，承载任务模板、提示约束、输入输出规范和最佳实践。
- 二者关系:
  - MCP 解决“能做什么”。
  - Skills 解决“按什么流程做”。
  - workflow 解决“如何串起来稳定做”。

### 1.3 Apache IoTDB MCP 的接口价值

- 将 IoTDB 的连接探测、元数据查询、SQL 执行、导入校验等能力统一成工具集合。
- 使 Agent 可以在权限和策略约束下调用 IoTDB，而不是直接生成不可控脚本。
- 为上层业务场景提供稳定的原子能力基础。

### 1.4 TimeSeek 中 IoTDB MCP 的体现方式

- `timeseek/opencode/opencode.json` 中将 IoTDB MCP 作为本地 MCP 服务接入。
- 环境变量约束了数据库地址、用户、密码、数据库名和 SQL dialect。
- 在运行时，通过 MCP 工具而非自由 shell/code 访问 IoTDB，形成更强的治理边界。

### 1.5 IoTDB Skills 的接口价值

- Skills 不是单个工具，而是面向典型问题域的“可复用工作说明书”。
- Skills 可以沉淀提示模板、规则参考、报告模板、执行约束和 artifact 规范。
- 这使得 agent 的执行从“每次临时推理”转向“沿着已有专业套路推理”。

### 1.6 TimeSeek 中 Skills 的体现方式

- `timeseek/skills/timeseek-workflow` 已开始承载 workflow 级知识。
- `references/rules_reference.md` 可作为规则知识入口。
- `templates/report_template.md` 可作为报告生成模板入口。
- 这意味着 TimeSeek 已具备将领域经验产品化为 skill 的基本结构。

### 1.7 本章需要突出展示的现有独特工作

- 面向 IoTDB Agent 的接口不是单一 SQL tool，而是 MCP + Skills 的双层设计。
- 将“数据库能力调用”与“业务流程模板”明确拆分，降低了单次 prompt 工程的脆弱性。
- 在 `opencode` 配置、workspace guide、workflow yaml、MCP README 中形成了相互一致的接口边界。

### 1.8 本章相关工作对比角度

- 通用 LLM + SQL Agent: 通常只提供 SQL 生成与执行，缺乏导入、规则治理、报告治理和 trace。
- 通用 MCP 数据库连接器: 解决连接问题，但未解决复杂业务流程编排问题。
- 通用技能库/Prompt 库: 能复用提示，但不一定具备 IoTDB 场景下的执行约束与 artifact 规范。

### 1.9 本章未来优化方向

- 补全 IoTDB table/tree 双模型统一 Agent 接口。
- 将更多 IoTDB 原生运维与分析能力 MCP 化。
- 将 skill 从静态模板升级为可参数化、可版本化、可评测的专业能力包。

## 2. 基于 opencode 的 coding + data agent

### 2.1 为什么需要 coding + data agent 一体化

- 纯 data agent 往往只能查数据、写 SQL，难以覆盖 schema 规划、文件处理、结果导出和流程修复。
- 纯 coding agent 又容易绕开数据库工具治理，直接写脚本，带来不可控执行路径。
- TimeSeek 需要的是“默认走 data/MCP 通道，必要时才进入 coding fallback”的一体化机制。

### 2.2 opencode 在 TimeSeek 中的角色

- 作为运行入口和编排底座，负责 agent 配置、MCP 注册、权限约束和子 agent 路由。
- 通过 `timeseek-orchestrator` 定义面向 TimeSeek 的专属编排代理。
- 通过工具权限限制 `write/edit/bash`，体现“MCP-first”的执行原则。

### 2.3 TimeSeek 的 agent 分工机制

- `timeseek/workspaces/AGENTS.md` 定义 master agent 与 `timeseek-auto-rule` 子代理职责。
- query 类请求留在主流程。
- auto-rule 类请求路由到专门子流程。
- coding fallback 只有在用户明确要求或 MCP 路径被证明阻塞时才启用。

### 2.4 coding + data agent 的核心设计原则

- MCP-first: 优先调用数据库与业务工具，不直接自由编码。
- least privilege: 代理和工具权限最小化。
- route by intent: 按任务类型路由到合适子 agent/workspace。
- artifacts first: 每一步都围绕 artifact 输入输出，不依赖隐式上下文。
- governed fallback: 必要时允许 coding，但必须是受限、可解释、可回退的。

### 2.5 从代码仓库看已有落地

- `opencode.json` 已注册 IoTDB、CSV、Rule、SQL、Report 五类 MCP。
- `timeseek-orchestrator.md` 明确要求使用 MCP tools 和 workflow skills。
- workspace guide 明确“优先 MCP，避免 ad-hoc code generation”。
- auto_rule workspace 进一步限制 CSV 导入必须走 IoTDB `import-data` CLI 通道。

### 2.6 这一设计的独特性

- 不是简单把代码代理接到数据库，而是构建“可治理的数据工作代理”。
- coding 能力被降级为兜底机制，数据操作主路径由 MCP 和 workflow 承载。
- 对 IoTDB 数据导入、规则编译、报告生成等高频路径做了专门治理，而不是把所有事情都交给一个通用大模型自由发挥。

### 2.7 本章可展开的相关工作

- 通用代码代理平台: 强于代码修改，弱于数据库业务流程治理。
- 通用数据分析 Agent: 强于问答和报表，弱于工程化 artifact 和 workspace 隔离。
- AutoGPT/多代理框架类方案: 强于自主规划，但常缺乏面向数据库生产路径的精细权限与步骤治理。

### 2.8 本章未来优化方向

- 在 opencode 中加入面向 workflow 的可视化 stepper 与 trace UI。
- 补全 coding fallback 的触发条件、审计记录与回退机制。
- 支持更细粒度的 agent/tool 授权策略，例如按 workspace、按 workflow、按风险等级授权。

## 3. TimeSeek 的业务 MCP 模块设计

### 3.1 业务 MCP 设计目标

- 将业务流程中可稳定沉淀的能力拆解为独立 MCP 服务。
- 每个 MCP 专注单一职责，降低工具耦合和提示复杂度。
- 通过 MCP 分层支持组合编排，而不是构建一个大而全的超级工具。

### 3.2 当前 MCP 模块总览

- IoTDB MCP: 数据库连接、元数据、SQL 驱动等底层数据库能力。
- `csv_tools`: CSV 探测、采样、标准化、schema 规划、导入校验。
- `rule_tools`: 规则 IR 的保存、读取、更新、装载。
- `sql_tools`: SQL 上下文构建、候选验证、dry-run、解释排序、最终定稿。
- `report_tools`: Markdown 校验、CSV 导出、Markdown 落盘。
- `knowledge_tools`: 面向知识检索和知识库管理的扩展能力。

### 3.3 模块边界设计

#### 3.3.1 csv_tools

- 解决数据入口问题，而不是直接参与 SQL 推理。
- 负责从 CSV 到 IoTDB tree import format 的转换。
- 负责从样本数据推导 schema 规划信息。
- 代表了 TimeSeek 对“数据进入数据库之前”这一步骤的工程化抽象。

#### 3.3.2 rule_tools

- 负责规则 IR 持久化与装载。
- 将自然语言规则的执行对象从“一段 prompt”转化为“可复用、可存储、可追踪的规则对象”。
- 为后续 SQL 编译提供稳定输入。

#### 3.3.3 sql_tools

- 负责 SQL 上下文治理与候选 SQL 治理。
- 包括 `build_sql_context`、`validate_sql_candidates`、`dry_run_sql_candidates`、`explain_and_rank_sql`、`approve_and_finalize_sql`。
- 这是 TimeSeek 区别于“直接让模型生成 SQL”方案的关键模块。

#### 3.3.4 report_tools

- 负责将分析结果转化为结构化报告产物。
- 将“报告生成”拆成 markdown 生成、markdown 校验、表格 CSV 导出三类能力。
- 保证最终输出不仅是模型回复，而是可持久化的业务交付物。

#### 3.3.5 knowledge_tools

- 提供知识检索、知识增删改查能力。
- 为未来把 TimeSeek 从“任务型 agent”升级为“带长期记忆的数据智能体”预留接口。

### 3.4 业务 MCP 组合方式

- 数据入口: `csv_tools`
- 规则持久化: `rule_tools`
- SQL 编译与治理: `sql_tools`
- 结果输出: `report_tools`
- 知识增强: `knowledge_tools`
- 底层数据库与执行环境: IoTDB MCP + opencode runtime

### 3.5 当前模块设计的独特工作

- 模块职责边界清晰，避免“一个 SQL 工具包干完所有事情”。
- `rule_tools` 与 `sql_tools` 已显式解耦，说明系统已经从早期原型走向可维护架构。
- `report_tools` 将输出治理纳入 MCP 范围，而不是把报告视为随意文本。
- `csv_tools` 覆盖了 IoTDB 数据导入前的关键预处理环节，贴合时序数据落库需求。

### 3.6 本章可用的相关工作对比角度

- 与传统 ETL 工具相比: TimeSeek 更强调“由 Agent 驱动的动态分析与规则生成”。
- 与通用 BI/报表助手相比: TimeSeek 覆盖了从数据进入 IoTDB 到规则编译再到报告输出的全链路。
- 与普通 SQL Agent 相比: TimeSeek 将规则 IR、SQL 治理、报告治理分别模块化。

### 3.7 本章未来优化方向

- 增加 business MCP，例如异常检测、设备画像、预测维护、指标计算、告警解释。
- 在 knowledge_tools 之上叠加“规则推荐”“SQL 模板召回”“历史案例复用”。
- 为 MCP 引入统一版本管理、输入输出 schema 注册和服务健康检查。
- 支持跨库、跨表、跨设备的多源分析工具。

### 3.8 产品可扩展性

- 可以按行业打包新的 MCP 组合，例如制造、能源、车联网、楼宇运维。
- 可以按部署形态扩展为本地版、企业私有化版、云上托管版。
- 可以按角色扩展为分析师 Agent、运维 Agent、数据治理 Agent、行业专家 Agent。

## 4. TimeSeek 的 workspace 和 workflow 设计: 以 autorule 为例

### 4.1 为什么 workspace 很重要

- Agent 执行如果没有明确工作空间，就容易出现输入输出分散、上下文污染、结果难审计的问题。
- workspace 将数据、规则、合同、workflow、报告、代理说明聚合到统一目录语义下。
- 对 TimeSeek 而言，workspace 是业务流程产品化和可复现实验的最小单位。

### 4.2 TimeSeek 的 workspace 结构

- `workspaces/AGENTS.md`: 全局工作区路由规则。
- `workspaces/auto_rule/AGENTS.md`: auto_rule 场景专属执行规范。
- `workspaces/auto_rule/csv/`: 输入 CSV。
- `workspaces/auto_rule/rules/`: 规则文件。
- `workspaces/auto_rule/workflows/`: workflow 描述。
- `workspaces/auto_rule/workflows/contracts/`: 输入输出合同。
- `workspaces/auto_rule/reports/`: 最终报告与导出物。

### 4.3 autorule 的业务目标

- 从 CSV 数据出发，完成样本分析、schema 建立、数据导入、规则解析、SQL 生成与校验、报告输出。
- 将自然语言规则转化为可审计 SQL。
- 将执行结果落成 markdown 和 CSV 形式的业务报告。

### 4.4 autorule workflow 的三阶段主流程

#### 4.4.1 第一阶段: 表创建与数据导入

- 对应 `01_table_creaing.yaml`
- 包含 IoTDB 探测、CSV 元数据分析、采样、CSV 标准化、tree schema 规划、schema 存在性检查、schema 应用、导入前数据存在性检查、CSV 导入、导入后校验。
- 这一阶段体现了 TimeSeek 对“导入前检查”和“warm start 复用”的重视。

#### 4.4.2 第二阶段: 规则到 SQL 的受控编译

- 对应 `02_rule_sql_generation.yaml`
- 包含规则装载、SQL context 构建、主 agent 语义对齐与 SQL 候选生成、候选验证、dry-run、EXPLAIN 排序、审批定稿。
- 这是 TimeSeek 的核心智能环节，也是治理最严格的环节。

##### 4.4.2.1 上下文构建: build_sql_context 不是简单拼 prompt，而是构造 SQL context pack

- `build_sql_context` 会把自然语言规则、表结构、样本数据、策略约束、运行时反馈和 skill 内容统一打包成 `sql_context_ir`。
- 这一 context pack 当前至少包含以下几类信息:
  - `rule_raw_json`: 规则原文、规则名、规则描述、状态、创建时间等。
  - `database_summary`: `database`、`table`、`database_meta`、`table_summary`、`column_summary`、`sample_rows`。
  - `nlp_prompt_booster`: 为语义对齐提供默认推理链，而不是让模型无约束自由联想。
  - `runtime_outputs`: 包括 `explain_output`、`react_last_output`、`user_feedback`，用于多轮迭代时吸收上轮失败反馈。
  - `knowledge_context`: 包括 `iotdb_skills`、`timeseek_skills`、`raw_skill_context`。
  - `constraints_context`: 包括 `dialect`、`mode`、`allowed_statement_types`、`disabled_sql_patterns`、`timeout_ms`、`row_limit`。
  - `dialect_profile`: 针对 tree/table 方言给出 path model、推荐聚合函数、高风险模式、使用说明。
  - `skill_injection_meta`: 记录 skill 是用户显式提供还是系统自动注入。
- 这一步的重点不是“信息越多越好”，而是把后续 SQL 生成所需的关键信号结构化，避免模型只看自然语言规则就直接产 SQL。

##### 4.4.2.2 语义对齐: 通过 schema/sample/skills/constraints 四类上下文压缩歧义

- 当前 `sql_tools` 内置的 `nlp_prompt_booster` 默认推理链是:
  - 识别规则中的核心概念和变量。
  - 结合 schema 和 sample 检查歧义。
  - 在生成 SQL 之前先形成简明解释。
- 这意味着语义对齐不是单纯“把中文翻成 SQL”，而是显式执行一个从业务语言到数据库语义的对齐过程。
- 在 autorule 场景下，这个过程尤其重要，因为规则文本往往包含如下歧义:
  - 指标名与物理测点名不一致，例如“主轴转速”对应 `spindle_rpm`。
  - 自然语言中的持续时间、连续区间、事件次数，并不等于普通 `COUNT(*)`。
  - 规则描述默认假设了采样周期、时间粒度或业务背景，但这些信息必须通过 sample/schema 才能被识别。
- `build_sql_context` 通过 `table_schema_summary + table_sample_rows + rule_ir + skill_context` 的组合，帮助主 agent 先完成业务语义到字段语义的映射，再进入 SQL 候选生成。
- `skill_context` 不是装饰项。当前 `timeseek_sql_tools` 会自动注入 `iotdb-grammar-dialect-guardrails`、`iotdb-dialect-differences`、`iotdb-error-rewrite-playbook` 三类 IoTDB dialect skill，用来提醒模型区分 tree/table 语法差异、避免错误聚合函数和危险 clause。
- 这一步实际上实现了三层语义约束:
  - 业务语义约束: 由 `rule_ir` 和 `user_feedback` 提供。
  - 数据语义约束: 由 `column_summary`、`sample_rows`、`table_summary` 提供。
  - 方言语义约束: 由 `dialect_profile` 和自动注入的 IoTDB grammar skills 提供。

##### 4.4.2.3 当前上下文构建的风险提示机制

- `build_sql_context` 不只输出上下文，也会输出 warnings。
- 当前已实现的 warnings 至少包括:
  - `skill_context` 为空时，提示 SQL 生成将主要依赖 schema/sample。
  - `table_sample_rows` 为空时，提示歧义风险升高。
  - `table_summary` 为空时，提示缺少关键属性/主键信息。
- 这说明 TimeSeek 已经开始把“上下文不充分”本身视为可观测风险，而不是把一切问题都推给大模型。

##### 4.4.2.4 候选验证: 先做静态和方言校验，再讨论 ranking

- `validate_sql_candidates` 的目标不是挑最优 SQL，而是先淘汰不该进入下一步的 SQL。
- 当前校验维度包括:
  - 单语句约束: 禁止多条 SQL 混发。
  - 语句类型白名单: 默认只允许 `SELECT/SHOW/DESC/DESCRIBE/EXPLAIN`。
  - 破坏性约束: 默认禁止 destructive SQL。
  - `UPDATE/DELETE` 的 `WHERE` 约束。
  - dialect-aware rule: 针对 tree/table 分别执行方言规则。
- tree dialect 下，系统当前会显式拦截或警告以下高风险模式:
  - `FROM (SELECT ...)` 子查询形态。
  - 使用 `MAX()/MIN()` 而不是 `MAX_VALUE()/MIN_VALUE()`。
  - 在 `SELECT` 投影中直接使用 `root...` 全路径。
  - 聚合表达式和原始投影字段混用。
  - `GROUP BY SESSION/CONDITION/VARIATION/COUNT` 但没有聚合意图。
  - `TIME` 与字符串字面量比较，提示优先使用 epoch-millis。
- 校验输出不仅包含 `errors/warnings`，还会生成 `rewrite_hints`，这为 react loop 提供了明确的修正方向。

##### 4.4.2.5 ranking 实现: 当前以静态特征和风险分数为核心，而不是完整运行时代价模型

- 当前 ranking 相关逻辑由 `dry_run_sql_candidates` 和 `explain_and_rank_sql` 两步共同完成。
- `dry_run_sql_candidates` 会先对已通过验证的候选 SQL 做静态预检查，并输出:
  - `ready_candidates`
  - `dry_run_sql`
  - `risk_score`
  - `risk_reasons`
  - `score`
  - `explain_queue`
- 其中 `explain_queue` 的作用是为后续 IoTDB `EXPLAIN` 调用准备待执行队列。也就是说，当前实现里“dry-run”更接近“静态预演 + EXPLAIN 队列构造”，并不直接执行真实数据库 explain。
- `explain_and_rank_sql` 当前实现也仍然是静态 ranking，不是基于数据库真实执行计划的 fully runtime ranker。
- 它会从候选 SQL 中提取静态特征，例如:
  - `statement_type`
  - `has_where`
  - `has_group_by`
  - `has_order_by`
  - `has_limit`
  - `has_join`
  - `has_subquery`
  - `uses_wildcard`
- 然后通过 `_risk_score` 计算风险分数，再用 `score = 1 - risk` 作为排序分数。

##### 4.4.2.6 ranking 的打分逻辑细节

- 当前风险分数是显式规则计算，不是黑盒模型分数。
- 典型加分项包括:
  - 破坏性或修改类语句: `+0.6`
  - `SELECT *`: `+0.1`
  - `SELECT` 没有 `LIMIT`: `+0.2`
  - 包含 `JOIN`: `+0.1`
  - 包含子查询: `+0.05`
  - 在 `readonly` 模式下出现非只读语句: `+0.5`
- 系统同时会把这些加分原因保存在 `risk_reasons` 中，例如:
  - `destructive_or_mutating_statement`
  - `select_wildcard`
  - `no_limit`
  - `has_join`
  - `has_subquery`
  - `readonly_mode_violation`
- 这使 ranking 结果具备可解释性。最终推荐的 `recommended_candidate_id` 不是“模型偏好”，而是一个可回溯的低风险选择。
- 需要特别说明的是，workflow YAML 中已经声明了 `prefer_low_scan`、`prefer_simple_sql`、`prefer_strict_rule_match` 这类 ranking policy，但当前 `sql_tools` 实现里的 `_risk_score` 主要仍依据静态 SQL 结构特征和 `mode` 打分。
- 换句话说，ranking policy 的产品接口已经设计出来，但更精细的“低扫描代价”“严格规则拟合度”“真实 explain 计划质量”评分仍属于下一阶段优化点。

##### 4.4.2.7 当前实现与 workflow 描述之间的一个关键事实

- workflow 文案里写的是“Run EXPLAIN and rank valid candidates by execution risk and rule fitness”。
- 但从当前 `timeseek/mcp_extensions/sql_tools` 的实现看:
  - `dry_run_sql_candidates` 负责准备 `EXPLAIN` 队列；
  - `explain_and_rank_sql` 目前主要基于静态特征和风险规则进行排序；
  - 返回的 `explain_summary` 本质上是特征摘要，而不是数据库真实执行计划解析结果。
- 这一点很值得在报告中单独说明，因为它恰好体现了 TimeSeek 当前所处阶段:
  - 已经完成了 SQL 治理链路的结构化设计；
  - 但 ranking 仍偏静态规则驱动，未来可以升级为“静态规则 + 实际 EXPLAIN 计划 + 查询结果质量”的联合排序。

#### 4.4.3 第三阶段: 报告生成与输出固化

- 对应 `03_report_generation.yaml`
- 包含报告上下文构建、报告生成、Markdown 校验、CSV 导出、审批定稿。
- 这使得 workflow 输出不是一次对话，而是一组业务可交付 artifact。

### 4.5 autorule workflow 的关键机制

#### 4.5.1 artifact 驱动

- 每个阶段都有明确 artifact 列表，如 `rule_ir`、`sql_context_ir`、`final_sql_ir`、`report_markdown_ir`。
- artifact 是 agent 与工具之间的标准中间表示，也是 trace 与复盘的基础。

#### 4.5.2 react loop 驱动

- SQL 与报告两个阶段都启用了 `react_loop`。
- 这意味着系统允许“生成 - 校验 - 反馈 - 再生成”的受控迭代，而不是一次性生成后直接执行。

#### 4.5.2.1 SQL 阶段的 react loop 如何工作

- SQL 阶段的 `react_loop` 最多执行 `3` 轮。
- 重试触发条件包括:
  - `retry_on_invalid_sql`
  - `retry_on_high_risk_plan`
  - 必须吸收来自 `validate` 和 `explain` 的反馈
- 停止条件包括:
  - `sql_is_valid`
  - `plan_risk_is_acceptable`
  - `human_approved`
  - `max_iterations_reached`
  - `user_abort`
- 这意味着 react loop 的本质不是“多生成几次试试看”，而是“基于结构化反馈不断收缩候选空间”。

#### 4.5.3 guard/skip condition 驱动

- 在 schema 创建与数据导入阶段设置 warm start / cold start 策略。
- 在 SQL 阶段设置 validate、dry-run、rank、finalize 的强约束链路。
- 体现 TimeSeek 的思路不是“提高模型正确率”，而是“降低错误进入下一阶段的概率”。

#### 4.5.3.1 SQL guard 链的意义

- SQL 阶段当前 guard 明确要求:
  - 必须先 `validate`，再 `dry_run`；
  - 必须先 `dry_run`，再 `rank/finalize`；
  - `ready_candidates` 为空时禁止 finalize；
  - 不允许绕过链路直接试探执行 query。
- 这套 guard 链让 TimeSeek 的 SQL 生成更像“受控编译流程”，而不是“对话式试错流程”。

#### 4.5.4 human approval 驱动

- SQL finalize 与 report finalize 都为人工审批留了接口。
- 这为 TimeSeek 后续进入真实业务流程提供了必要的人工兜底点。

#### 4.5.4.1 human approval 在当前实现中的位置

- `approve_and_finalize_sql` 会优先选择:
  - `selected_candidate_id`
  - 否则 `recommended_candidate_id`
  - 再否则使用第一个有效候选
- 如果提供 `human_approval` 且 `approved=false`，流程会显式返回拒绝结果而不是继续落库。
- 如果提供 `final_edits`，系统会在最终定稿前重新规范化 SQL，保证人工修订仍在单语句边界内。

### 4.6 autorule 例子中可重点展示的实际产物

- `sample_rules.json` 中的多条业务规则。
- `rule_1/2/3_*.md` 报告示例。
- 规则执行结果 CSV。
- `test_autorule_workspace.py` 冒烟测试，证明 rule/sql/report 三段链路已经可跑通。

### 4.7 workspace + workflow 设计的独特工作

- 不是简单 prompt chaining，而是显式 YAML workflow。
- 不是把所有上下文堆到对话里，而是以 workspace 为边界管理文件、规则、合同和报告。
- 不是只对最终 SQL 做检查，而是对 schema、import、SQL、report 都设置治理环节。
- 不是只做 demo 输出，而是已经具备测试、合同、报告样例和日志 schema。

### 4.8 本章相关工作对比角度

- 与 DAG/ETL workflow 相比: TimeSeek 引入大模型参与规则理解与报告生成。
- 与纯 agent planner 相比: TimeSeek 的流程更强约束、更可审计。
- 与 notebook 式数据分析相比: TimeSeek 更适合沉淀为重复执行的产品流程。

### 4.9 本章未来优化方向

- workflow engine 从静态 YAML 升级为可执行 runtime。
- 增加失败重试、断点续跑、分支条件、并发规则执行和阶段缓存。
- 支持 multi-rule batch 执行与全局汇总报告，而非当前以单规则结果为主。
- 支持跨 workspace 模板复用和行业工作流模板市场。

## 5. Agent trace 和其他后续优化方向

### 5.1 为什么 TimeSeek 必须重视 trace

- 数据智能体一旦进入生产环境，用户关心的不只是结果，还关心结果从何而来。
- SQL 是如何生成的、用过哪些工具、是否经过校验、最终报告是否被修改，这些都需要可追踪。
- trace 是 TimeSeek 从“实验性 Agent”走向“企业级产品”的关键能力。

### 5.2 当前 trace 设计

- `observability/run_trace_schema.json` 定义了 `RunTraceV1`。
- trace 记录 `run_id`、`session_id`、`message_id`、时间戳、状态、事件流、工具调用、MCP 信息、输入输出、错误等字段。
- `logging.yaml` 规定了 trace schema、trace_root、jsonl 事件流格式以及关键日志字段。

### 5.3 当前 trace 的价值

- 支持按 run 追踪完整执行过程。
- 支持从失败步骤回放与恢复。
- 支持对 agent、tool、mcp、模型、调用链进行审计。
- 支持后续做性能分析、错误分析与提示优化。

### 5.4 trace 仍然处于早期的地方

- 当前更多是 schema 与日志约定，尚未形成完整的 trace 可视化产品体验。
- 对 artifact 版本、输入输出 diff、审批痕迹、重试原因等深层信息还可继续补齐。
- 与 workflow runtime 的联动还可以更紧密。

### 5.5 未来优化方向一: 执行可观测性增强

- 增加 workflow 级 step timeline。
- 增加 per-step 输入输出快照与 diff。
- 增加 token、延迟、错误码、重试次数、人工介入次数等指标。
- 增加 trace 到报告、规则、SQL artifact 的双向链接。

### 5.6 未来优化方向二: Agent 治理增强

- 风险分级 SQL 审批。
- 更细粒度的权限控制与策略检查。
- 引入 policy engine，对 SQL 类型、访问范围、输出行数、导出路径进行统一治理。
- 增加 prompt、tool、workflow 版本绑定，保证结果可复现。

### 5.7 未来优化方向三: 智能能力增强

- 规则理解从单轮 NL 解析升级到基于知识库的规则编译。
- 报告生成从结果摘要升级到可解释分析与异常归因。
- 增加跨表、跨设备、跨时间窗口的复杂分析能力。
- 增加主动推荐能力，例如“推荐可用规则”“推荐可疑异常”“推荐分析模板”。

### 5.8 未来优化方向四: 产品化增强

- 增加 Web UI，包括 workflow stepper、artifact 浏览、trace 时间线、审批界面。
- 增加多租户与项目级 workspace 管理。
- 增加任务中心、运行历史、模板市场、结果分享与权限管理。
- 增加行业化预置包，支持制造、能源、智慧楼宇、车联网等领域快速落地。

### 5.9 本章需要强调的产品可扩展性

- 纵向扩展: 从单一 autorule 流程扩展到更多 IoTDB 数据工作流。
- 横向扩展: 从制造数据扩展到其他时序数据行业。
- 技术扩展: 从本地 opencode + MCP 运行扩展到云原生服务形态。
- 组织扩展: 从研发 demo 扩展到数据分析、运维、行业顾问等多角色协同。

## 6. 总结与答辩式收束

### 6.1 一句话总结

- TimeSeek 已经形成了面向 Apache IoTDB 的数据智能体雏形，其价值在于把 IoTDB 能力、业务规则、工作流编排和执行审计统一到了一个可治理框架中。

### 6.2 建议在结论页强调的三点

- 已有独特工作: 构建了 MCP + Skills + workspace + workflow + trace 的完整链路，而不是单点 SQL agent。
- 相对相关工作的差异: 更贴近时序数据库真实业务流程，更重视治理、artifact 和可复现执行。
- 后续可扩展潜力: 可以从 autorule 继续扩展到更多业务 MCP、行业 workflow 和企业级产品能力。

### 6.3 可作为最终一页的展望

- 短期: 补全 workflow runtime、UI、trace 可视化、multi-rule 批处理。
- 中期: 补全知识增强、策略治理、多场景业务 MCP。
- 长期: 将 TimeSeek 发展为 Apache IoTDB 面向行业的数据智能体产品底座。

## 附录建议

### A. 仓库中可直接引用的材料

- `timeseek/opencode/opencode.json`
- `timeseek/workspaces/AGENTS.md`
- `timeseek/workspaces/auto_rule/AGENTS.md`
- `timeseek/workspaces/auto_rule/workflows/01_table_creaing.yaml`
- `timeseek/workspaces/auto_rule/workflows/02_rule_sql_generation.yaml`
- `timeseek/workspaces/auto_rule/workflows/03_report_generation.yaml`
- `timeseek/mcp_extensions/csv_tools/README.md`
- `timeseek/mcp_extensions/rule_tools/README.md`
- `timeseek/mcp_extensions/sql_tools/README.md`
- `timeseek/mcp_extensions/report_tools/README.md`
- `timeseek/observability/run_trace_schema.json`
- `timeseek/tests/e2e/test_autorule_workspace.py`

### B. IoTDB MCP Server 工具统计总览

说明: 下表按当前 `lib/iotdb_mcp_server/src/iotdb_mcp_server/server.py` 的注册逻辑统计，区分 tree/table 两种 SQL dialect。当前共有 `10` 个工具大类、`33` 个去重后的工具名；其中 tree dialect 注册 `25` 个工具，table dialect 注册 `18` 个工具。

| 大类 | tree 工具数 | table 工具数 | 去重后工具数 | 主要权限模型 |
| --- | ---: | ---: | ---: | --- |
| Query | 2 | 2 | 4 | 默认可用；只允许只读查询前缀；导出工具额外受 `export_path` 约束 |
| SQL Driver | 2 | 2 | 2 | `IOTDB_ENABLE_SQL_DRIVER` + allowed users + `IOTDB_SQL_DRIVER_MODE`；破坏性 SQL 需确认 |
| Metadata | 8 | 3 | 10 | `IOTDB_ENABLE_METADATA_QUERY` + `IOTDB_METADATA_ALLOWED_USERS` |
| Explain | 1 | 1 | 1 | 默认可用；只允许 query-like SQL |
| Database | 3 | 4 | 4 | `IOTDB_ENABLE_DATABASE_DDL`；`DROP DATABASE` 可要求确认 |
| Timeseries DDL | 4 | 0 | 4 | `IOTDB_ENABLE_TIMESERIES_DDL`；删除类操作可要求确认 |
| TTL | 2 | 0 | 2 | `IOTDB_ENABLE_TTL_SQL`；`UNSET TTL` 可要求确认 |
| Table DDL | 0 | 3 | 3 | `IOTDB_ENABLE_TABLE_DDL`；`DROP TABLE` 可要求确认 |
| Write DML | 1 | 1 | 1 | `IOTDB_ENABLE_WRITE_DML`；删除类操作可要求确认 |
| Model Management | 2 | 2 | 2 | `IOTDB_ENABLE_MODEL_MANAGEMENT`；破坏性模型操作可要求确认 |

### C. IoTDB MCP Server 全量工具矩阵

| 大类 | Dialect | 工具名称 | 权限/门槛 |
| --- | --- | --- | --- |
| Query | tree | `select_query` | 默认可用；仅允许 `SELECT` |
| Query | tree | `export_query` | 默认可用；仅允许 `SELECT`/`SHOW`；导出写入 `config.export_path` |
| Query | table | `read_query` | 默认可用；允许 `SELECT`/`SHOW`/`DESCRIBE`/`DESC` |
| Query | table | `export_table_query` | 默认可用；允许 `SELECT`/`SHOW`/`DESCRIBE`/`DESC`；导出写入 `config.export_path` |
| SQL Driver | both | `sql_driver_policy` | 默认可用；返回 whitelist、mode、destructive confirm 策略快照 |
| SQL Driver | both | `sql_execute` | 需 `IOTDB_ENABLE_SQL_DRIVER=true` + `IOTDB_SQL_DRIVER_ALLOWED_USERS`；同时受 `IOTDB_SQL_DRIVER_MODE=readonly/ddl/full` 约束；破坏性 SQL 在 `IOTDB_SQL_DRIVER_REQUIRE_DESTRUCTIVE_CONFIRM=true` 时需 `confirm_destructive=True` |
| Metadata | tree | `metadata_query` | 需 `IOTDB_ENABLE_METADATA_QUERY=true`（代码默认值为 true）+ `IOTDB_METADATA_ALLOWED_USERS`；只允许 `SHOW DATABASES/TIMESERIES/DEVICES/CHILD PATHS/CHILD NODES/FUNCTIONS`、`COUNT TIMESERIES/NODES/DEVICES` |
| Metadata | tree | `list_timeseries` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `list_devices` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `list_child_paths` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `list_child_nodes` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `count_timeseries` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `count_devices` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | tree | `count_nodes` | 复用 `metadata_query` 权限；路径需满足 tree path 校验 |
| Metadata | table | `metadata_query` | 需 `IOTDB_ENABLE_METADATA_QUERY=true`（代码默认值为 true）+ `IOTDB_METADATA_ALLOWED_USERS`；只允许 `SHOW`/`DESC`/`DESCRIBE` |
| Metadata | table | `list_tables` | 复用 `metadata_query` 权限 |
| Metadata | table | `describe_table` | 复用 `metadata_query` 权限；表名需满足 identifier 校验 |
| Explain | both | `explain_query` | 默认可用；tree 仅允许目标 SQL 以 `SELECT/SHOW/COUNT/WITH` 开头，table 仅允许 `SELECT/SHOW/DESC/DESCRIBE/WITH`；可选 `analyze=true` |
| Database | tree | `list_databases` | 默认可用；无额外开关 |
| Database | tree | `create_database` | 需 `IOTDB_ENABLE_DATABASE_DDL=true` + `IOTDB_DATABASE_DDL_ALLOWED_USERS` |
| Database | tree | `drop_database` | 同上；若 `IOTDB_REQUIRE_DROP_CONFIRM=true` 则需 `confirm=True` |
| Database | table | `list_databases` | 默认可用；无额外开关 |
| Database | table | `create_database` | 需 `IOTDB_ENABLE_DATABASE_DDL=true` + `IOTDB_DATABASE_DDL_ALLOWED_USERS` |
| Database | table | `drop_database` | 同上；若 `IOTDB_REQUIRE_DROP_CONFIRM=true` 则需 `confirm=True` |
| Database | table | `use_database` | 默认可用；无额外开关 |
| Timeseries DDL | tree | `create_timeseries_ddl` | 需 `IOTDB_ENABLE_TIMESERIES_DDL=true` + `IOTDB_TIMESERIES_DDL_ALLOWED_USERS`；仅允许 `CREATE TIMESERIES`/`CREATE ALIGNED TIMESERIES` |
| Timeseries DDL | tree | `create_timeseries_batch_ddl` | 同上；批量执行受 `ddl_list`、`continue_on_error`、`skip_if_exists`、`max_statements` 约束 |
| Timeseries DDL | tree | `alter_timeseries_ddl` | 需 `IOTDB_ENABLE_TIMESERIES_DDL=true` + `IOTDB_TIMESERIES_DDL_ALLOWED_USERS`；仅允许 `ALTER TIMESERIES` |
| Timeseries DDL | tree | `drop_timeseries_ddl` | 同上；仅允许 `DROP TIMESERIES`/`DELETE TIMESERIES`；若 `IOTDB_REQUIRE_TIMESERIES_DROP_CONFIRM=true` 则需 `confirm=True` |
| TTL | tree | `ttl_command` | 需 `IOTDB_ENABLE_TTL_SQL=true` + `IOTDB_TTL_ALLOWED_USERS`；仅允许 `SET TTL`/`UNSET TTL`；若 `IOTDB_REQUIRE_TTL_UNSET_CONFIRM=true`，执行 `UNSET TTL` 需 `confirm_unset=True` |
| TTL | tree | `ttl_query` | 需 `IOTDB_ENABLE_TTL_SQL=true` + `IOTDB_TTL_ALLOWED_USERS`；仅允许 `SHOW TTL ON`/`SHOW ALL TTL` |
| Table DDL | table | `create_table_ddl` | 需 `IOTDB_ENABLE_TABLE_DDL=true` + `IOTDB_TABLE_DDL_ALLOWED_USERS`；仅允许 `CREATE TABLE` |
| Table DDL | table | `alter_table_ddl` | 需 `IOTDB_ENABLE_TABLE_DDL=true` + `IOTDB_TABLE_DDL_ALLOWED_USERS`；仅允许 `ALTER TABLE` |
| Table DDL | table | `drop_table_ddl` | 需 `IOTDB_ENABLE_TABLE_DDL=true` + `IOTDB_TABLE_DDL_ALLOWED_USERS`；仅允许 `DROP TABLE`；若 `IOTDB_REQUIRE_TABLE_DROP_CONFIRM=true` 则需 `confirm=True` |
| Write DML | tree | `write_query` | 需 `IOTDB_ENABLE_WRITE_DML=true` + `IOTDB_WRITE_ALLOWED_USERS`；仅允许 `INSERT INTO`/`DELETE FROM`；若 `IOTDB_REQUIRE_DELETE_CONFIRM=true`，删除需 `confirm_delete=True` |
| Write DML | table | `write_query` | 需 `IOTDB_ENABLE_WRITE_DML=true` + `IOTDB_WRITE_ALLOWED_USERS`；仅允许 `INSERT INTO`/`UPDATE`/`DELETE FROM`/`DELETE DEVICES`；若 `IOTDB_REQUIRE_DELETE_CONFIRM=true`，删除需 `confirm_delete=True` |
| Model Management | both | `model_query` | 需 `IOTDB_ENABLE_MODEL_MANAGEMENT=true` + `IOTDB_MODEL_ALLOWED_USERS`；仅允许 `SHOW MODELS`/`SHOW LOADED MODELS`/`SHOW AI_DEVICES`/`SHOW AINODES` |
| Model Management | both | `model_command` | 需 `IOTDB_ENABLE_MODEL_MANAGEMENT=true` + `IOTDB_MODEL_ALLOWED_USERS`；仅允许 `CREATE MODEL`/`DROP MODEL`/`LOAD MODEL`/`UNLOAD MODEL`/`REMOVE AINODE`；若 `IOTDB_REQUIRE_MODEL_DESTRUCTIVE_CONFIRM=true`，执行 `DROP MODEL`/`UNLOAD MODEL`/`REMOVE AINODE` 需 `confirm_destructive=True` |

### D. IoTDB Skills 内容统计

说明: 这里统计的是仓库中 `lib/iotdb_skills` 下已经存在的 skill 资产，而不是概念性描述。当前 `timeseek_sql_tools` 会自动向 `skill_context` 注入 `3` 个 IoTDB dialect skill 条目，来源均为 `lib/iotdb_skills/iotdb/grammars/*`。

| 技能包/模块 | 层级 | SKILL 文件数 | references 数 | assets 数 | scripts 数 | 主要内容 | 与 TimeSeek 的关系 |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `lib/iotdb_skills/iotdb` | 主技能 | 1 | 11 | 2 | 2 | IoTDB 多语言连接、tree/table 数据模型、数据导入、JDBC、Spring Boot、MyBatis、REST、C++/Python/Java 示例 | 为 Apache IoTDB Skills 提供通用连接与开发知识底座 |
| `lib/iotdb_skills/iotdb/grammars` | 子技能 | 1 | 2 | 0 | 0 | TREE/TABLE dialect guardrails、语法差异、错误重写 playbook | 当前由 `timeseek/mcp_extensions/sql_tools` 自动注入，直接服务 SQL 生成/校验 |
| `lib/iotdb_skills/tsfile` | 辅助技能 | 1 | 1 | 4 | 2 | TsFile 读写、schema 设计、性能优化、Java/Python/C++/C 示例 | 当前未直接注入 TimeSeek SQL flow，但可扩展为文件侧数据能力 |

#### D.1 TimeSeek 当前自动注入的 IoTDB skill 条目

| skill_id | 来源文件 | 内容定位 |
| --- | --- | --- |
| `iotdb-grammar-dialect-guardrails` | `lib/iotdb_skills/iotdb/grammars/SKILL.md` | SQL 生成前的 TREE/TABLE 语法护栏 |
| `iotdb-dialect-differences` | `lib/iotdb_skills/iotdb/grammars/references/dialect-differences.md` | dialect 差异速查表 |
| `iotdb-error-rewrite-playbook` | `lib/iotdb_skills/iotdb/grammars/references/error-rewrite-playbook.md` | SQL 失败后的错误到重写策略 |

#### D.2 IoTDB Skills 关键内容清单

| 模块 | 内容类型 | 文件/目录 | 说明 |
| --- | --- | --- | --- |
| `iotdb` | 主技能 | `lib/iotdb_skills/iotdb/SKILL.md` | IoTDB 连接与使用总入口，覆盖 Java SessionPool/Session/JDBC、Python、C++、REST、tree/table 模型 |
| `iotdb` | references | `lib/iotdb_skills/iotdb/references/` | 共 11 个参考文件，覆盖 `data_models`、`data_import`、`java/python/cpp/rest`、`jdbc`、`spring_boot`、`mybatis` 等主题 |
| `iotdb` | assets | `lib/iotdb_skills/iotdb/assets/connection_templates/` | 连接模板 2 个: Java、Python |
| `iotdb` | scripts | `lib/iotdb_skills/iotdb/scripts/` | 实用脚本 2 个: 连接校验、性能测试 |
| `iotdb/grammars` | 主技能 | `lib/iotdb_skills/iotdb/grammars/SKILL.md` | grammar-first SQL guardrails |
| `iotdb/grammars` | references | `lib/iotdb_skills/iotdb/grammars/references/` | 2 个参考文件: dialect 差异、错误重写 playbook |
| `tsfile` | 主技能 | `lib/iotdb_skills/tsfile/SKILL.md` | TsFile 读写与优化主入口 |
| `tsfile` | references | `lib/iotdb_skills/tsfile/references/api_reference.md` | TsFile API 参考 |
| `tsfile` | assets | `lib/iotdb_skills/tsfile/assets/` | 4 个示例/配置文件: Java、Python、C++、`pom.xml` |
| `tsfile` | scripts | `lib/iotdb_skills/tsfile/scripts/` | 2 个脚本: build、example |

### E. TimeSeek MCP Extensions 内容与工具统计

说明: 这里统计的是 `timeseek/mcp_extensions` 下的业务扩展，而不是底层 `iotdb_mcp_server`。当前共有 `5` 个扩展包、`27` 个已声明工具。

| 扩展包 | 内容定位 | 关键文件 | 工具数 | 工具列表 |
| --- | --- | --- | ---: | --- |
| `csv_tools` | CSV 到 IoTDB tree 模型的数据入口、采样、标准化、schema 规划、导入校验 | `README.md`、`pyproject.toml` | 6 | `probe_iotdb`、`inspect_csv_metadata`、`sample_csv`、`normalize_csv_for_iotdb_tree`、`plan_tree_schema_from_csv`、`validate_iotdb_import_count` |
| `knowledge_tools` | 遗留 Java knowledge API 的 MCP 化，负责知识检索与知识管理 | `README.md`、`pyproject.toml` | 8 | `get_all_knowledge`、`retrieve_knowledge`、`add_knowledge`、`update_knowledge`、`delete_knowledge`、`get_knowledge_by_title`、`retrieve_knowledge_with_data`、`retrieve_knowledge_with_sql` |
| `report_tools` | 报告 markdown 校验、CSV 导出、markdown 持久化 | `README.md`、`pyproject.toml`、`src/timeseek_report_mcp/` | 3 | `validate_report_markdown`、`write_report_tables_csv`、`write_report_markdown` |
| `rule_tools` | 规则 IR 保存、更新、读取、加载 | `README.md`、`pyproject.toml` | 4 | `save_rule_json`、`upsert_rule_ir`、`read_rules_json`、`load_rule_ir` |
| `sql_tools` | SQL 上下文构建、候选验证、dry-run、排序、定稿、结果对账 | `README.md`、`pyproject.toml`、`src/timeseek_sql_mcp/` | 6 | `build_sql_context`、`validate_sql_candidates`、`dry_run_sql_candidates`、`explain_and_rank_sql`、`approve_and_finalize_sql`、`reconcile_final_and_executed_sql` |

#### E.1 TimeSeek MCP Extension 全量工具矩阵

| 扩展包 | 工具名称 | 核心内容/职责 |
| --- | --- | --- |
| `csv_tools` | `probe_iotdb` | 探测 IoTDB 可达实例，支持从安装目录解析默认连接并扫描 sibling package |
| `csv_tools` | `inspect_csv_metadata` | 读取 CSV 头、行数并推断基础列类型 |
| `csv_tools` | `sample_csv` | 生成 `head + random` 样本行 |
| `csv_tools` | `normalize_csv_for_iotdb_tree` | 将 CSV 重写为 IoTDB tree import 格式，确保首列为 `Time` |
| `csv_tools` | `plan_tree_schema_from_csv` | 从标准化 CSV 推导 `CREATE DATABASE`/`CREATE TIMESERIES` 计划 |
| `csv_tools` | `validate_iotdb_import_count` | 对比 CSV 行数与 IoTDB `COUNT(measurement)`，校验导入完整性 |
| `knowledge_tools` | `get_all_knowledge` | 获取知识库全部知识条目 |
| `knowledge_tools` | `retrieve_knowledge` | 按 query 检索知识 |
| `knowledge_tools` | `add_knowledge` | 新增知识条目 |
| `knowledge_tools` | `update_knowledge` | 更新知识条目 |
| `knowledge_tools` | `delete_knowledge` | 删除知识条目 |
| `knowledge_tools` | `get_knowledge_by_title` | 按标题获取单条知识 |
| `knowledge_tools` | `retrieve_knowledge_with_data` | 检索知识并结合数据返回 |
| `knowledge_tools` | `retrieve_knowledge_with_sql` | 检索知识并结合 SQL 返回 |
| `report_tools` | `validate_report_markdown` | 校验 markdown 表格结构、列对齐、空单元格等规则 |
| `report_tools` | `write_report_tables_csv` | 将结构化表格 IR 落成 CSV 文件 |
| `report_tools` | `write_report_markdown` | 将 markdown IR 写入目标文件 |
| `rule_tools` | `save_rule_json` | 保存单条结构化规则 |
| `rule_tools` | `upsert_rule_ir` | 按 `id` 或 `name` 更新/插入规则 IR |
| `rule_tools` | `read_rules_json` | 读取规则 JSON 文件中的全部规则 |
| `rule_tools` | `load_rule_ir` | 装载单条有效规则，供 SQL workflow 下游使用 |
| `sql_tools` | `build_sql_context` | 构建 SQL 生成上下文，打包 rule/schema/sample/policy/skill_context |
| `sql_tools` | `validate_sql_candidates` | 静态校验候选 SQL 的语法、方言和安全策略 |
| `sql_tools` | `dry_run_sql_candidates` | 进行 dialect-aware dry run/EXPLAIN 准备 |
| `sql_tools` | `explain_and_rank_sql` | 基于 EXPLAIN、风险和规则拟合度做排序 |
| `sql_tools` | `approve_and_finalize_sql` | 审批并定稿最终 SQL |
| `sql_tools` | `reconcile_final_and_executed_sql` | 对账最终 SQL 与实际执行 SQL，减少编译产物与执行产物不一致 |

#### E.2 报告中可强调的结论

- `timeseek/mcp_extensions` 已形成从数据入口、规则 IR、SQL 治理、报告输出到知识增强的完整扩展面。
- `sql_tools` 与 `lib/iotdb_skills/iotdb/grammars/*` 已经形成联动，说明 TimeSeek 不只是“有工具”，而是开始让 tool 与 skill 联合参与 SQL 生成治理。
- 从架构成熟度看，`rule_tools` 与 `sql_tools` 的拆分、`report_tools` 的独立化、`csv_tools` 的导入前处理能力，都是从原型走向产品化的重要信号。

### F. 单次 Run Trace 参考统计

说明: 以下统计来自单个真实运行文件 [run_20260316070832197_ses_30a88fe67ffeH4COSwovsz1bsm_msg_cf579960b001Yp8sbeYiZn6ZIq.json](/Users/alan671/Documents/IoTDB-TimeSeek/timeseek/observability/traces/runs/run_20260316070832197_ses_30a88fe67ffeH4COSwovsz1bsm_msg_cf579960b001Yp8sbeYiZn6ZIq.json)。该 run 开始于 `2026-03-16T07:08:32Z`，结束于 `2026-03-16T07:39:45Z`，总状态为 `completed`。这些数字适合作为当前 autorule workflow 的参考样本，但不应直接当作总体平均值。

#### F.1 工具调用次数统计

| 指标 | 数值 | 说明 |
| --- | ---: | --- |
| 总工具调用次数 | 54 | 以 `tool.call.started` 事件计数 |
| 成功结束次数 | 54 | `tool.call.finished` 与 started 数一致 |
| builtin 工具调用 | 6 | 全部为 `bash` |
| MCP 工具调用 | 48 | 来自 `timeseek_csv_tools`、`iotdb`、`timeseek_rule_tools`、`timeseek_sql_tools`、`timeseek_report_tools` |

| MCP Server / 工具域 | 调用次数 | 说明 |
| --- | ---: | --- |
| `timeseek_sql_tools` | 19 | SQL context、validate、dry-run、rank、finalize、reconcile 是本 run 最核心链路 |
| `iotdb` | 14 | schema 创建、查询、select、explain 等数据库操作 |
| `timeseek_report_tools` | 7 | markdown 写入与 CSV 导出 |
| `builtin` | 6 | shell 辅助步骤 |
| `timeseek_csv_tools` | 4 | CSV inspect/probe/normalize/plan |
| `timeseek_rule_tools` | 4 | 规则读取与逐条加载 |

| Top 工具 | 次数 | 说明 |
| --- | ---: | --- |
| `iotdb_select_query` | 7 | 最终规则执行与结果读取主通道 |
| `bash` | 6 | 辅助检查、CLI 调用与流程收尾 |
| `timeseek_sql_tools_validate_sql_candidates` | 4 | SQL 候选静态校验 |
| `timeseek_report_tools_write_report_markdown` | 4 | 报告 markdown 落盘 |
| `timeseek_rule_tools_load_rule_ir` | 3 | 三条 active rule 分别装载 |
| `timeseek_sql_tools_build_sql_context` | 3 | 每条规则构建一次 context pack |
| `timeseek_sql_tools_dry_run_sql_candidates` | 3 | 每条规则准备 dry-run / EXPLAIN 队列 |
| `timeseek_sql_tools_explain_and_rank_sql` | 3 | 每条规则执行一次排序 |
| `timeseek_sql_tools_approve_and_finalize_sql` | 3 | 每条规则定稿一次 |
| `timeseek_sql_tools_reconcile_final_and_executed_sql` | 3 | 每条规则定稿 SQL 与执行 SQL 对账 |

#### F.2 Token 消耗统计

说明: 这里按 `assistant.message.completed` 事件汇总 token 元数据。trace 中可直接获得 `input/output/reasoning/cache_read/cache_write`，但 `workspace_tokens`、`skills_tokens`、`tool_schema_tokens` 当前未暴露，因此无法进一步拆分。

| Token 指标 | 汇总值 |
| --- | ---: |
| 输入 tokens | 52,716 |
| 输出 tokens | 19,730 |
| reasoning tokens | 8,254 |
| cache read tokens | 2,282,688 |
| cache write tokens | 0 |

| 最终回答消息 token | 数值 |
| --- | ---: |
| final message input | 367 |
| final message output | 1,630 |
| final message reasoning | 602 |
| final message total | 66,957 |
| final message cache read | 64,960 |

#### F.3 SQL 生成链路统计

| 指标 | 数值 | 说明 |
| --- | ---: | --- |
| active rule 数 | 3 | 对应 rule 1/2/3 |
| `build_sql_context` 次数 | 3 | 每条规则一次 |
| `validate_sql_candidates` 次数 | 4 | rule3 有一次先失败后重试 |
| 总候选 SQL 数 | 7 | 四次 validate 的 `results` 总和 |
| 通过 validate 的候选 SQL 数 | 6 | validate pass rate = `6/7 = 85.7%` |
| `dry_run_sql_candidates` 次数 | 3 | 每条规则一次 |
| ready candidates 总数 | 6 | 进入 dry-run/EXPLAIN 队列的候选数 |
| `explain_and_rank_sql` 次数 | 3 | 每条规则一次 |
| ranked candidates 总数 | 6 | 被排序的候选数 |
| `approve_and_finalize_sql` 次数 | 3 | 每条规则定稿一次 |
| 最终 SQL 数 | 3 | 与 active rule 数一致 |
| `reconcile_final_and_executed_sql` 次数 | 3 | 每条规则一次 |
| final/executed SQL 一致数 | 3 | 全部 `consistent=true` |

#### F.4 SQL 生成准确率参考值

说明: trace 文件本身没有 gold SQL，也没有人工标注的语义正确答案。因此这里给出的“准确率”只能是流程代理指标，而不是严格的语义正确率评测。

| 指标 | 结果 | 含义 |
| --- | --- | --- |
| 候选级 validate 通过率 | `85.7%` (`6/7`) | 生成的候选 SQL 中，有 6 条通过静态与方言校验 |
| 规则级最终 SQL 产出率 | `100%` (`3/3`) | 3 条 active rule 均生成了最终 SQL |
| 规则级最终 SQL 执行一致率 | `100%` (`3/3`) | 3 条 final SQL 与 executed SQL 对账一致 |
| 流程级 SQL 生成成功率 | `100%` (`3/3`) | 从规则装载到 finalize 的完整链路全部跑通 |

建议在报告里把这组数字命名为:

- `SQL candidate validation pass rate`
- `rule-level finalized SQL success rate`
- `finalized/executed SQL consistency rate`

而不要直接写成“SQL 语义准确率 100%”，因为这会混淆流程正确与业务语义正确。

#### F.5 本次 run 的最终 SQL 清单

| Rule | Final SQL | 备注 |
| --- | --- | --- |
| Rule 1 | `SELECT COUNT(*) FROM root.timeseek.default_device WHERE spindle_rpm > 8000 GROUP BY CONDITION(spindle_rpm > 8000, 1)` | 事件计数类规则 |
| Rule 2 | `SELECT COUNT(*) * 10000 FROM root.timeseek.default_device WHERE run_mode = 2` | 依赖 10 秒采样间隔的持续时长近似 |
| Rule 3 | `SELECT MAX_VALUE(spindle_temp) - MIN_VALUE(coolant_temp) FROM root.timeseek.default_device` | 聚合差值规则 |

#### F.6 这一参考 run 能支持的报告结论

- 当前 autorule workflow 已经能在单次 run 中完成从 CSV、schema、SQL 生成到报告输出的全链路执行。
- SQL 治理链路具备可观测性: 可以统计候选数、validate pass rate、rank 结果、finalize 数量与最终一致性。
- token 与工具调用都已进入 trace，可以支撑后续做 cost/performance analysis。
- 但“语义准确率”仍需单独建设 benchmark 或人工标注集，不能仅依赖 trace 中的成功执行结果。

### G. 图示建议

- 图 1: TimeSeek 总体架构图
- 图 2: IoTDB MCP 与 Skills 双层接口图
- 图 3: opencode 下的 coding + data agent 路由图
- 图 4: 业务 MCP 模块划分图
- 图 5: autorule workflow 时序图
- 图 6: run trace 事件流示意图

### H. 汇报表达建议

- 先讲“为什么需要 Agent 接口层”，再讲“TimeSeek 怎么做”。
- 先讲“系统设计与治理”，再讲“模型能力”。
- 用 autorule 作为主例子，避免汇报内容过散。
- 每章都从“现有工作、相关工作、优化方向、可扩展性”四个维度收束。
