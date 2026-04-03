# TiCDC Dashboard TODO

这份文档只记录当前阶段性的后续工作。

稳定的维护说明、架构边界、日常编辑方式，请看：

- `metrics/grafana/README.md`

处理以下事项时，不能悄悄改变布局，也不能让已有 panel ID 发生漂移。

## P0：优先处理

- [ ] 修复 Event Store 中 pebble duration 相关面板的 legend 与 query 分组
      不一致的问题。
      当前 compaction / flush duration 面板按 `instance, id` 分组，但 legend
      仍然使用 `{{type}}`，会直接误导读图。
      代码位置：`metrics/rows/event_store.py`
- [ ] 审查所有仍然在 graph panel target 上设置 `format="heatmap"` 的地方，
      并在完成专门的 target-format 评审后清理错误用法。
      当前例子：`metrics/rows/event_store.py`、
      `metrics/rows/log_puller.py`、`metrics/rows/event_service.py`、
      `metrics/rows/ddl.py`

## P1：尽快处理

- [ ] 统一整个 dashboard 中 percentile 与 average 的 legend 命名规则。
      当前代码同时混用了 `p99`、`p999`、`p9999`、`P999`、`99.9`、`99.9%`、
      `99%-...` 等写法，可读性差，也不利于后续统一维护。
      代码位置：`metrics/rows/sink_general.py`、`metrics/rows/sink_mq.py`、
      `metrics/rows/coordinator.py`、`metrics/rows/ddl.py`、
      `metrics/rows/event_service.py`、`metrics/rows/tikv.py`
- [ ] 统一对外可见标题的风格，清理明显的标题卫生问题。
      例如尾随空格、大小写风格混乱、`CheckpointTs` 这种偏代码风格的名字。
      这里必须遵守兼容性约束，任何对外可见 row title 或 panel title 的修改，
      都要先经过兼容性评审。
      当前例子：`metrics/rows/event_store.py`、`metrics/rows/sink_mq.py`、
      `metrics/rows/lag_analyze.py`
- [ ] 规范手写 PromQL 的 selector 顺序，并在确实能减少重复、又不掩盖业务
      语义时，优先使用共享 helper，例如 `expr_*`。
      目标是减少无意义 diff，提高 agent 驱动 dashboard 修改时的确定性与可维护性
- [ ] 为已经达成共识的 authoring 约定补充自动化一致性检查。
      重点可以包括标题风格、percentile/average legend 命名规则，以及其他
      约定好的书写规范，避免后续再次漂移

## P2：后续修改时必须遵守的边界

- [ ] 修 legacy row 内部变量名时，只能重命名两类 panel：
      已经有显式 `key=` 的 panel；或者先补一个兼容用 `key=`，再改变量名。
      现有很多 panel 的稳定 identity 默认来自本地变量名，所以“为了可读性改名”
      不是无风险重构
- [ ] Histogram panel 继续保持显式写法。
      作者应明确决定展示 quantile、avg、heatmap bucket，避免再引入会隐式
      展开多条 query 的 builder helper
