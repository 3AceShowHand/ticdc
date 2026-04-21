# TiCDC Dashboard Python Code Review 导读

这份文档是给 reviewer 用的，不是 Python 教程全集。

目标只有两个：

1. 带你按一个合适的顺序 review 当前 `metrics/` 下的代码。
2. 让你在 review 的过程中，顺手看懂这套代码里用到的现代 Python 写法。

## 先建立代码地图

先记住一句话：

> 这套代码的日常维护入口是 `metrics/rows/*.py`，其他层大多是在替 row 文件收敛机械样板。

当前目录分层可以这样理解：

- `metrics/dashboard.py`
  dashboard 顶层装配。它只负责把各个 row 按顺序拼起来，并接上 dashboard 元信息、templating、annotations。
- `metrics/rows/*.py`
  一行一个文件。绝大多数业务修改都应该发生在这里。
- `metrics/queries.py`
  PromQL helper 层。它的职责是把反复出现的 selector、`sum by (...)`、`rate(...)`、histogram 常见写法收敛起来。
- `metrics/builders.py`
  给 dashboard 作者用的 builder API。这里负责提供 `dashboard -> row -> panel -> query` 这种渐进式写法。
- `metrics/dsl/api.py`
  更底层的不可变 spec 构造器。它是 builder 的下层，不是日常改 panel 的主要入口。
- `metrics/dsl/render.py`
  最终 JSON renderer。它把 spec 渲染成 Grafana 认识的 JSON 字典。
- `metrics/panel_ids.py`
  稳定 panel ID 管理。它保证已有 panel 的 ID 不会因为插入、删除别的 panel 而漂移。
- `metrics/dashboard_meta.py`
  dashboard 级别的标题、UID、version、datasource 常量。

如果把这套代码和传统手写 JSON 对比，可以这样理解：

- `rows/*.py` 负责表达“我想监控什么”
- `queries.py` 负责表达“这类 PromQL 重复模式怎么统一写”
- `builders.py` 负责表达“作者应该怎么舒服地写”
- `render.py` 负责表达“Grafana JSON 最终长什么样”

## 推荐 review 顺序

更合理的顺序是：

1. 先看 `metrics/grafana/README.md`
   先确认当前这套 dashboard-as-code 的目标、边界、约束是什么。
2. 再看 `metrics/dashboard.py`
   只建立顶层认识：有哪些 row、顺序是什么、dashboard 元信息从哪里来。
3. 然后挑一个具体 row 文件看
   推荐先看 `metrics/rows/changefeed.py`，因为它同时包含 graph、table、普通 query、带 selector 的 query，代表性比较强。
4. 看 `metrics/queries.py`
   搞清楚 row 文件里那些 `expr_*` helper 到底展开成什么 PromQL。
5. 再看 `metrics/builders.py`
   搞清楚 `row(...)`、`graph(...)`、`add_query(...)` 这些作者 API 最终会生成什么 spec。
6. 最后按需看 `metrics/panel_ids.py` 和 `metrics/dsl/render.py`
   只有在你需要检查 panel ID 稳定性、布局落地方式、Grafana JSON 兼容性时，再深入这两层。

这个顺序背后的原因很简单：

- 先看业务语义
- 再看 PromQL 复用
- 最后才看内部实现

这样 review 时最不容易迷路。

## 你应该先看懂什么，不该先纠结什么

第一次 review 时，优先看懂下面这几件事：

- 这个 row 想监控什么问题
- panel 标题、单位、query、legend 是否一致
- query helper 有没有真的减少重复，而不是把语义藏起来
- panel 布局和 row 内组织是否清晰
- 这次改动会不会影响稳定 panel identity 和 panel ID

第一次 review 时，不需要先纠结这些事情：

- `dataclass` 的所有细枝末节
- Python 类型系统的所有语法
- Grafana JSON 每一个字段的历史背景
- `render.py` 每一个默认值为什么这样选

先抓住主干，再看细节。

## 逐层怎么 review

### 1. `metrics/dashboard.py` 看什么

这个文件的职责非常单一：装配整个 dashboard。

你看它时，重点不是 Python 语法，而是下面这些问题：

- row 顺序有没有被意外改动
- 有没有新增或删除 row
- dashboard `title`、`uid`、`version` 是否正确
- 是否仍然接上了 templating、annotations
- 是否仍然使用稳定 panel ID resolver

这个文件如果变复杂，通常不是好信号。因为它本来就应该只做“组装”，不应该承载业务逻辑。

### 2. `metrics/rows/*.py` 看什么

这是最重要的一层。

以 `metrics/rows/changefeed.py` 为例，一个 row 文件大致应该长这样：

```python
def build_changefeed_row() -> RowSpec:
    row_builder = row("Changefeed")

    changefeed_checkpoint_lag = graph(
        "Changefeed Checkpoint Lag",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    row_builder.add_panel(changefeed_checkpoint_lag)
    return row_builder.build()
```

你可以把它按四步来读：

1. `row("Changefeed")`
   创建一个 row builder。
2. `graph(...)`
   创建一个 panel builder，并声明标题、单位、最小值、描述等展示属性。
3. `.add_query(...)`
   往 panel 里加 query。真正的业务语义通常就在这里。
4. `row_builder.add_panel(...)`
   把 panel 放回 row，最后 `build()` 得到不可变 spec。

review row 文件时，重点看这些问题：

- panel 标题是否准确表达指标含义
- `unit`、`min`、`decimals` 是否合理
- query 是否真的对应 panel 标题
- `by_labels` 是否和 legend 一致
- `scope`、`selectors` 是否选对
- 这个 panel 应该是 graph、heatmap 还是 table
- row 内 panel 排列是否易读

对 reviewer 来说，row 文件应当尽量接近“看监控意图”的体验，而不是“解一层层抽象”的体验。

### 3. `metrics/queries.py` 看什么

这个文件的作用不是“发明新 DSL”，而是收敛 PromQL 重复样板。

最常见的 helper 有这些：

- `expr_sum(...)`
- `expr_avg(...)`
- `expr_max(...)`
- `expr_rate(...)`
- `expr_sum_rate(...)`
- `expr_histogram_quantile(...)`
- `expr_histogram_avg(...)`

review 这层时，关键问题是：

- 这个 helper 是否真的减少重复
- 它有没有把业务语义藏得太深
- 默认 `scope` 是否安全
- 默认窗口如 `1m` 是否合理
- 当 `by_labels` 改变时，返回表达式的 legend 推断是否仍然成立

这里有一个特别值得看懂的小对象：`Expr`。

它不是简单的字符串别名，而是“带一点元数据的 PromQL 表达式对象”：

```python
@dataclass(frozen=True, slots=True)
class Expr:
    text: str
    by_labels: tuple[str, ...] = ()
```

你可以这样理解：

- `text` 是最终 PromQL 文本
- `by_labels` 记录这条表达式当前按哪些 label 聚合

这让后续代码可以做两件事：

- 用 `call()` 继续包一层函数，比如 `rate(...)`
- 用 `op()` 做表达式组合，比如乘 `100`

例如：

```python
expr_sum_rate(...).op("*", "100")
```

这表示先得到一个表达式，再拼成 `(<expr>) * 100` 这种效果。你不必把它想得太神秘，它本质上仍然是在安全地拼 PromQL。

### 4. `metrics/builders.py` 看什么

这层是当前 author-facing API 的核心。

它解决的问题是：row 作者不想直接手写 `GraphPanelSpec(...)`、`TargetSpec(...)` 这些底层对象，而想用更顺手的增量式写法：

```python
panel = graph("Flush Duration", unit="s", min="0")
panel.add_auto_query(...)
panel.add_auto_query(...)
row_builder.add_panel(panel)
```

review 这层时，重点看这几件事：

- API 是否真的比直接写 spec 更简单
- 有没有过度“聪明”的隐式行为
- 方法名是否清楚表达作用
- 默认值是否符合大多数 panel 的需求
- 这层有没有把 Grafana 兼容细节泄漏给 row 作者

其中有三个方法特别值得分清：

- `add_query(...)`
  使用该 panel 类型的默认 query 行为。
- `add_auto_query(...)`
  不显式输出 target `format` 字段。
- `add_range_query(...)`
  显式生成 `instant=False` 的 range query。

大多数情况下，review 时你应该问：

- 这里为什么要用 `add_query`，而不是 `add_auto_query`
- 这里是不是确实需要 range query
- row 作者是否被迫理解太多 Grafana target 细节

#### 这里最容易踩坑的点：稳定 identity

`builders.py` 里有一段比较“魔法”的逻辑：如果 panel 没有显式 `key=...`，就尝试从 row builder 函数里的本地变量名推断 panel key。

例如：

```python
flush_duration = graph("Flush Duration")
```

这里默认会把 `flush_duration` 视为 panel 的稳定身份之一。

这样做的原因是：

- panel 标题可能会改
- 但已存在 panel 的 ID 不应该因为改标题就变化

所以 review 时一定要留意：

- 改 panel 标题，一般没问题
- 改 panel 本地变量名，可能会影响稳定 panel identity
- 如果某个老 panel 已经进入稳定兼容期，重命名变量前必须确认影响

### 5. `metrics/panel_ids.py` 看什么

这个文件处理的是“Grafana 兼容性”，不是 Python 语法技巧。

当前策略是：

- 用 `row.key + panel.key` 作为稳定身份
- 如果一个 panel 身份已经存在，就沿用原来的 ID
- 新 panel 只拿新的、更大的 ID
- 删除 panel 不会让已有 panel 重新编号

review 这里时，重点看：

- 身份是否唯一
- 新增 panel 时 ID 是否单调递增
- 删除 panel 后旧 ID 是否被保留，不影响其他 panel
- 改标题是否不会改 ID

如果你看到“只是重命名一个局部变量”的改动，这里就是你需要回头确认的地方。

### 6. `metrics/dsl/api.py` 和 `metrics/dsl/render.py` 看什么

这两层一般只在两种场景下需要重点看：

1. authoring API 本身要改
2. 生成 JSON 和 master 上的 dashboard 行为不一致

`dsl/api.py` 比较像“低噪音 spec 构造器”。

它的特点是：

- 参数尽量 keyword-only
- 返回的都是简单 spec 对象
- 不做太多业务推断

`dsl/render.py` 则是把 spec 落成 Grafana JSON。

review renderer 时重点看：

- panel `type` 是否正确
- target JSON 是否正确渲染
- `yaxes`、`gridPos`、`templating`、`annotations` 是否保持兼容
- dashboard `version` 是否正确输出
- 默认值有没有意外改变已有 dashboard 行为

如果某次改动导致生成 JSON diff 很大，但 row 文件只改了一点点，就要重点检查这里。

## 现代 Python 速读

下面这些语法是当前代码里真实会遇到的。知道“它是干什么的”就够了，不必一次吃透。

### `from __future__ import annotations`

它让类型注解延迟求值。

你可以先把它理解成一句“让类型写起来更自由、更现代”的兼容声明。看到它不用紧张，它不会改变业务逻辑。

### `type SelectorLike = ...`

这是 Python 3.12 的 type alias 写法。

例如：

```python
type SelectorLike = LabelMatcher | str
```

意思只是：

> `SelectorLike` 这个名字，代表“要么是 `LabelMatcher`，要么是 `str`”

它只是给类型起别名，不是定义新类。

### `A | B`

这就是现代 Python 的联合类型写法，等价于更老一点的 `Union[A, B]`。

例如：

```python
key: str | None = None
```

意思是这个值要么是字符串，要么是 `None`。

### `@dataclass(frozen=True, slots=True)`

这是现代 Python 很常见的数据对象写法。

你可以这样理解：

- `dataclass`
  自动生成初始化、比较、显示等样板代码
- `frozen=True`
  这个对象创建后不允许再改，适合做 spec 或值对象
- `slots=True`
  限制对象属性集合，让结构更固定，也更省一点内存

在这套代码里，它通常意味着：

> 这是一个结构清晰、偏不可变的数据对象，不是拿来塞复杂逻辑的“大类”。

### `Literal[...]`

例如：

```python
type ScopeName = Literal["instance", "changefeed", "cluster", "none"]
```

意思是：

> 这个参数只能取这几个固定字符串之一

它能帮助 reviewer 更快理解“允许的输入范围”。

### `Self`

`Self` 表示“返回当前类实例本身”。

例如 builder 里的：

```python
def add_panel(...) -> Self:
```

意思是：

> 这个方法返回调用它的对象自己，所以可以继续链式调用

### 关键字参数和 `*`

例如：

```python
def add_query(
    self,
    expr: str | Expr,
    *,
    legend: str | None = None,
    ref: str | None = None,
) -> Self:
```

这里的 `*` 表示：后面的参数必须写成关键字参数。

也就是说要这样调用：

```python
panel.add_query(expr, legend="x", ref="B")
```

而不是：

```python
panel.add_query(expr, "x", "B")
```

这类写法的优点是可读性更强，review 时也更不容易看错参数含义。

### `Path`

`pathlib.Path` 是现代 Python 里更推荐的路径对象。

它比早期纯字符串拼路径更清晰，例如：

```python
registry_path = resolved_root / PANEL_ID_REGISTRY_FILE
```

这就是“路径拼接”，不是除法。

### `dict[str, object]`

这是现代 Python 的内建泛型写法，等价于老一点的 `Dict[str, object]`。

不用紧张，它只是类型注解，不影响运行逻辑。

## 用真实代码练一次 review

推荐拿 `metrics/rows/changefeed.py` 做第一遍练习。

你可以按下面顺序问自己问题：

### 第一步：这行代码想监控什么

看 row 标题和 panel 标题：

- `Changefeed Checkpoint`
- `Changefeed Resolved Ts`
- `Changefeed Checkpoint Lag`
- `Changefeed Error Details`

只看标题，你已经能大概猜到这行在覆盖：

- 进度
- lag
- 错误详情

如果一个 row 文件在“只看标题”时都看不出监控意图，那通常就值得追问。

### 第二步：检查 query 和标题是否一致

例如 `Changefeed Checkpoint Lag` 这个 panel，query 用的是：

- `ticdc_owner_checkpoint_ts_lag`
- `by_labels=["namespace", "changefeed"]`
- `scope="cluster"`
- 再加上 namespace/changefeed selector

这和标题是对得上的。它确实是在看每个 changefeed 的 checkpoint lag。

如果标题写 checkpoint lag，query 却是 resolved ts lag，那就是明显问题。

### 第三步：检查 legend 是否和 group by 一致

如果 query 是按 `namespace, changefeed` 聚合的，那么 legend 至少不应该只展示 `instance`。

一个很常见的 review 问题就是：

- `by_labels` 写了三项
- legend 却只展示其中一项

这样图上多条线可能根本分不清谁是谁。

### 第四步：检查是不是把复杂度藏错地方了

复杂度应该尽量藏在 `queries.py` 和 builder 默认值里，不应该逼 row 作者自己手写一长串重复 selector。

所以当你看到下面这种代码时，需要问一句：

```python
add_query(
    'sum(rate(metric{...}[1m])) by (...)',
    legend="...",
)
```

它是不是本可以被 `expr_sum_rate(...)` 之类的 helper 吸收掉。

如果答案是“可以”，那说明抽象还有优化空间。

### 第五步：检查会不会误伤稳定 identity

如果一个现有 panel 没有显式 `key=...`，它可能在依赖本地变量名推断稳定 identity。

所以这种改动要特别小心：

```python
checkpoint_lag = graph(...)
```

改成：

```python
lag_panel = graph(...)
```

这在视觉上像是一次普通重命名，但对 panel ID 体系来说，可能不是。

## 一份实用的 review checklist

review 当前 dashboard Python 代码时，可以直接按下面清单过一遍：

- 这次改动主要发生在正确的层吗
- 业务语义是否主要留在 `rows/*.py`，而不是散落在更底层
- 新增 query 是否优先使用了现有 `expr_*` helper
- 如果新增了新 helper，它是否真的减少重复，而不是制造新概念
- `scope` 是否正确
- `selectors` 是否正确
- `by_labels` 和 legend 是否匹配
- graph / heatmap / table 选型是否合理
- `unit`、`min`、`decimals` 是否和指标一致
- row 内 panel 布局是否清晰
- 是否误改了稳定 row/panel identity
- 是否可能影响已有 panel ID
- dashboard `version` 是否按约定递增
- 生成出的 JSON diff 是真实行为变化，还是只是等价重写

## 看到这些 Python 写法时，不要慌

如果你很久没写 Python，最容易被“新语法外观”吓到，但这套代码里真正重要的不是语法花样，而是职责边界。

你可以先用下面这套心智模型：

- `dataclass` 大多只是结构化数据
- `Self` 大多只是为了链式调用
- `Literal` 大多只是限制字符串枚举值
- `type X = ...` 大多只是类型别名
- `| None` 大多只是“这个参数可空”

先把它们当成“帮助阅读的注释增强版”，通常就够了。

## 如果你只想用 10 分钟完成第一轮 review

那就按这个最短路径来：

1. 看 `metrics/grafana/README.md`
2. 看 `metrics/dashboard.py`
3. 挑一个 row 文件，从上到下读完
4. 遇到 `expr_*` helper 时去 `metrics/queries.py` 查定义
5. 只在需要确认布局、panel ID、JSON 兼容性时，再看 `builders.py`、`panel_ids.py`、`dsl/render.py`

这样就够做第一轮高质量 review 了。

## 最后给 reviewer 的一句建议

review 这套代码时，最值得挑错的不是“Python 有没有写得很炫”，而是下面三件事：

- 监控语义是否清楚
- 重复样板是否真的被收敛
- 兼容性约束是否被稳定维护

如果这三件事守住了，这套代码就会继续保持正确、上手简单、易维护、易理解、易管理。
