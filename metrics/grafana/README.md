# TiCDC Dashboard As Code

这份文档是当前 TiCDC Grafana dashboard Python 化改造的稳定维护说明。

此前为推动这次改造而产生的过程性设计稿、实现计划、兼容性跟踪文档，
都已经收敛整理。后续如果需要了解：

- dashboard 代码应该改哪里
- 如何新增或修改一个 panel
- 哪些兼容性约束不能破
- agent 应该如何协助同步 dashboard
- 当前还剩哪些后续工作

其中，稳定说明以本文件为准；阶段性待办请看 `metrics/grafana/TODO.md`。

## 目标

TiCDC dashboard 的源码真相现在是 Python，而不是手写 Grafana JSON。

这意味着：

- 人工维护的输入是 `metrics/` 下的 Python 代码
- `metrics/grafana/*.json` 和对应 `.sha256` 都是生成产物
- dashboard 变更应该评审业务语义和 authoring 代码，而不是评审大段 JSON diff

## 当前架构

当前目录结构的职责边界如下：

- `metrics/dashboard.py`
  负责 dashboard 顶层装配，只管理 row 顺序和 dashboard 级别元信息
- `metrics/rows/*.py`
  一行一个文件，是日常编辑的主要入口
- `metrics/builders.py`
  author-facing builder 层，负责 `dashboard -> row -> panel -> query` 的增量式写法
- `metrics/queries.py`
  常用 PromQL 表达式 helper
- `metrics/dsl/`
  内部 spec/render 层，负责最终 Grafana JSON 渲染
- `metrics/annotations.py`
  annotation 定义
- `metrics/templating.py`
  templating 定义
- `metrics/dashboard_meta.py`
  dashboard 标题、UID、version 等元信息
- `metrics/panel_ids.py`
  稳定 panel ID 逻辑
- `metrics/grafana/panel_ids.json`
  checked-in panel ID registry

当前 authoring 模型只有一个：

- 一个 dashboard 包含多个 row
- 一个 row 包含多个 panel
- 一个 panel 包含多个 query

不要再引入第二套业务 DSL。

## 该改哪里

大多数 dashboard 修改，只需要关心这几个位置：

- `metrics/rows/*.py`
- `metrics/dashboard.py`
- `metrics/builders.py`
- `metrics/queries.py`

通常的最短路径是：

1. 找到对应的 row 文件
2. 创建或修改一个 panel 本地变量
3. 给 panel 添加 query
4. 把 panel 加回 row
5. 重新生成 dashboard 产物

## 不要改哪里

不要手工编辑以下文件：

- `metrics/grafana/ticdc_new_arch.json`
- `metrics/grafana/ticdc_new_arch.json.sha256`
- `metrics/grafana/ticdc_new_arch_next_gen.json`
- `metrics/grafana/ticdc_new_arch_next_gen.json.sha256`
- `metrics/grafana/ticdc_new_arch_with_keyspace_name.json`
- `metrics/grafana/ticdc_new_arch_with_keyspace_name.json.sha256`
- `metrics/grafana/panel_ids.json`

这些文件都应该由 Python 代码生成或维护。

## Python 作用域

这套 Python 工作流当前只管理 metrics dashboard 相关代码：

- `metrics/` 下的源码
- `metrics/` 下的 dashboard 生成和校验入口
- `metrics/tests/` 下的 Python 测试

它不管理仓库里其他 Python 代码，例如 `tests/integration_tests/` 下的辅助脚本。

## 环境与命令

推荐环境：

- Python `3.12+`
- `uv`
- 项目 `.venv`

推荐初始化：

```bash
uv sync --group dev
```

日常编辑循环：

```bash
uv run python metrics/generate_dashboards.py
uv run python metrics/check_dashboards.py
uv run python -m unittest discover -s metrics/tests -p 'test_*.py' -v
```

完整验证：

```bash
uv run python metrics/generate_dashboards.py
uv run ty check
uv run ruff format --check metrics
uv run ruff check metrics
uv run python metrics/check_dashboards.py
uv run python -m unittest discover -s metrics/tests -p 'test_*.py' -v
make check
```

常用 `make` 入口：

- `make metrics-python-sync`
- `make metrics-python-typecheck`
- `make metrics-python-generate`
- `make metrics-python-check`
- `make metrics-python-test`

## 推荐写法

推荐使用的 authoring surface：

- `dashboard(...)`
- `row(...)`
- `graph(...)`
- `heatmap(...)`
- `table(...)`
- `dashboard.add_row(...)`
- `row.add_panel(...)`
- `row.add_panels(...)`
- `row.add_half_panel(...)`
- `panel.add_query(...)`
- `panel.add_auto_query(...)`
- `panel.add_range_query(...)`
- `panel.add_auto_range_query(...)`
- `table(...).add_label_query(...)`

这些方法都返回 `self`，允许链式调用。但推荐风格仍然是：

- 先定义 panel 变量
- 再逐步加 query
- 最后把 panel 加回 row

而不是把整行压成一个又长又嵌套的表达式。

### 最小示例

```python
from metrics.builders import graph, row
from metrics.queries import expr_sum_rate


def build_sink_row():
    row_builder = row("Sink")

    batch_rows = graph(
        "Batch Rows",
        unit="ops",
        min="0",
        description="Rows written by the sink per second.",
    ).add_query(
        expr_sum_rate(
            "ticdc_sink_batch_row_count_count",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}",
    )

    row_builder.add_panel(batch_rows)
    return row_builder.build()
```

dashboard 装配也应该保持同样直接：

```python
from metrics.builders import dashboard as dashboard_builder


def build_dashboard_spec():
    spec = dashboard_builder(
        title=BASE_DASHBOARD_TITLE,
        uid=BASE_DASHBOARD_UID,
        variables=build_templating(),
        annotations=build_annotations(),
    )
    spec.add_row(build_summary_row())
    spec.add_row(build_sink_row())
    return spec.build()
```

## Prometheus metric 形态

这里只需要处理三类 metric：

- Counter
  通常用 `expr_sum_rate(...)`、`expr_increase(...)`，或者必要时手写 `rate(...)`
- Gauge
  通常用 `expr_sum(...)`、`expr_avg(...)`、`expr_max(...)`、`expr_simple(...)`
- Histogram
  通常显式使用 `expr_histogram_quantile(...)`、`expr_histogram_avg(...)`，
  或者 bucket heatmap

Histogram panel 里到底展示 `quantile`、`avg` 还是两者同时展示，应该由
row 作者显式决定。不要再用 panel builder 隐式展开多条 query。

## 设计原则

只有一条核心规则：

把机械性样板收进内部，把监控意图留在外部。

好的抽象应该隐藏这些重复机械细节：

- target `format` 默认值
- instant / range query 接线
- 默认 `refId` 分配
- Grafana JSON 渲染细节

不好的抽象会把真正重要的监控意图也藏起来，例如：

- `changefeed_metric_graph(...)` 这类业务特化 wrapper
- `changefeed.sum(...)` 这种第二层 DSL
- 需要作者重新思考 Grafana JSON 结构的 helper

结论很简单：

- 减少样板是对的
- 隐藏业务语义是错的

## 目录与命名约定

- 一行一个文件，放在 `metrics/rows/`
- 每个 row 文件只导出一个 `build_xxx_row()`
- row 顺序只在 `metrics/dashboard.py` 里定义
- panel 要尽量用直白、稳定的本地变量名
- query helper 尽量让读者一眼就能看出 metric 意图
- 原始 PromQL 只作为 escape hatch

## 稳定 panel ID 约束

已有 panel ID 通过稳定 authoring identity 保持不变：

- row identity：默认来自 `build_xxx_row`
- panel identity：默认来自本地变量名

这意味着：

- 改 row 的可见标题，不必改 panel ID
- 改 panel 的可见标题，不必改 panel ID
- 插入新 panel，只会分配新的更大 ID
- 删除 panel，不会让其他 panel 重新编号

`metrics/grafana/panel_ids.json` 会保留这种稳定映射。

### 什么时候需要显式 `key=...`

只有一种典型场景：

- 你需要保留老的 panel identity
- 但又要重命名本地变量，或者存在重复标题 panel

对于新 panel：

- 本地变量名要起得直白、稳定
- 不要为了省事用 `_2`、`tmp`、`panel_a` 这类没有业务含义的名字

对于已有 panel：

- 如果没有显式 `key=...`，不要随便重命名本地变量
- 如果必须重命名，先补兼容 `key=`，再改变量名

## 兼容性约束

后续 dashboard 修改必须同时满足这些兼容性要求：

### 1. 布局约束

- 不要无意改变 row 顺序
- 不要无意把 panel 移到别的 row
- 不要无意改变既有布局

### 2. Annotation 约束

- 不要无意删除 annotation
- 不要无意改变 annotation 行为

### 3. Panel ID 约束

- 不要因为重排、重命名、query 重写而让已有 panel ID 漂移

### 4. Canonical JSON 边界

- JSON 允许做格式规范化
- 但不能借格式规范化之名偷偷改语义

### 5. 语义修复约束

如果某个 query 变更是在修历史 bug：

- 保留这个修复
- 在变更说明里写清楚
- 尽量补一个定向测试

## Agent 协作方式

推荐把 agent 当成“dashboard 同步执行者”，而不是“盲目生成 dashboard 的工具”。

当 TiCDC Prometheus metric 变化时，建议流程：

1. 先改业务代码里的 metric
2. 让 agent 读取当前 diff
3. 让 agent 改 `metrics/` 下的 Python 源码，而不是手改 JSON
4. 让 agent 重新生成 dashboard 并跑验证
5. 人工评审 panel 语义是否正确

人通常只需要给 agent 少量信息：

- 这个 metric 属于哪个 row
- 它应该是 graph、table 还是 heatmap
- 它应该并入已有 panel，还是新增 panel

### 推荐 prompt

```text
Please inspect the current TiCDC Prometheus metric changes in this workspace
and sync the Grafana dashboard accordingly.

Requirements:
1. Read the current code diff first and identify added, removed, renamed, or
   label-changed metrics.
2. Update the Python dashboard source under metrics/, not the generated JSON.
3. Prefer reusing an existing panel. Only create a new panel when the new
   metric expresses a new observation that does not fit an existing panel.
4. Keep existing panel IDs stable. Do not let panel reordering, title changes,
   or query refactors change existing panel IDs.
5. After editing, run:
   - python3 metrics/generate_dashboards.py
   - python3 metrics/check_dashboards.py
   - ./.venv/bin/ruff format --check metrics
   - ./.venv/bin/ruff check metrics
   - ./.venv/bin/ty check
   - python3 -m unittest discover -s metrics/tests -p 'test_*.py' -v
6. In the final summary, explain which row and panel were changed, and why.
```

必要时再补一个很短的人类提示，例如：

```text
This metric belongs to the Scheduler row.
It is a histogram and should be shown as p99 plus avg.
Do not create a new row.
```

## 当前测试职责

`metrics/tests/` 现在主要覆盖三层：

- `test_ticdc_dsl.py`
  primitive DSL 和 builder 核心行为
- `test_ticdc_dashboard_rows.py`
  row 级别语义与 reference dashboard 的归一化比对
- `test_ticdc_dashboard_tools.py`
  generator、checksum、dashboard 构建和关键兼容性行为
- `test_panel_ids.py`
  稳定 panel ID 行为

测试的重点应该是“防回归的真实行为”，而不是锁死 README 文案、Makefile 字符串、
内部导入路径或纯类型注解这种低价值 contract。

## Language Server / 类型检查

项目当前用 `ty` 做 Python 类型检查，配置只覆盖 dashboard tooling：

- `metrics/`

这样可以让编辑器语言服务和类型检查只关注这一小块 Python 代码，不把仓库里其他
Python 脚本一起拉进来。

## 当前待办

阶段性待办、优先级和后续边界规则统一维护在：

- `metrics/grafana/TODO.md`

## 结论

README 只负责长期稳定的维护说明。

对于 dashboard 维护者来说，最重要的不是继续发明新的 helper，而是：

- 保持 authoring 简单直接
- 保持 panel 语义清晰
- 保持兼容性约束稳定
- 让新人在 10 分钟内知道应该改哪里、怎么验证、哪些东西不能碰
