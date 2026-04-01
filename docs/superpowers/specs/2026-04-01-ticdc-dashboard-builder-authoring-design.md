# TiCDC Dashboard Builder Authoring Refactor Design

## Status

Status: Proposed (Approved)
Date: 2026-04-01
Owner: TiCDC Metrics

## Background / Context

TiCDC dashboard authoring in this repository currently spans two layers:

- the primitive immutable DSL under `metrics/dsl/`
- the TiCDC author-facing helper layer under `metrics/grafana/common.py`

The current split is directionally correct, but the editing surface is still
too close to the primitive layer. Representative files such as
`metrics/grafana/ticdc_new_arch/rows/server.py`,
`metrics/grafana/ticdc_new_arch/rows/execution.py`, and
`metrics/grafana/ticdc_new_arch/rows/overview.py` still require authors to
assemble nested `row(...) -> graph(...) -> targets=[target(...)]` structures
directly. Those files also still repeat layout and query boilerplate such as:

- `targets=[...]`
- `height=...`
- `span=...`
- `format="time_series"`
- repeated per-query `ref="B"` bookkeeping

The current row module organization also remains too coarse. The directory
`metrics/grafana/ticdc_new_arch/rows/` is grouped by business area instead of
by actual Grafana row, so a single source file can define several unrelated
rows. That increases search cost and makes localized panel edits harder than
they need to be.

The dashboard assembly point in
`metrics/grafana/ticdc_new_arch/dashboard.py` adds another maintenance burden.
Rows are built through grouped functions such as `build_overview_rows()` and
then re-ordered by `EXPECTED_ROW_TITLES` from `metrics/grafana/common.py`.
That creates two sources of truth for row ordering:

- grouped row-builder functions in `metrics/grafana/ticdc_new_arch/rows/`
- the order list in `metrics/grafana/common.py`

The result is a dashboard authoring model that is more explicit than the legacy
JSON, but still not local enough or simple enough for routine maintenance.

## Problem Statement

The current TiCDC Grafana authoring model does not reduce the management burden
of adding or modifying panels enough.

The primary problems are:

1. Author-facing code is still written in terms of nested immutable spec
   assembly rather than additive editing.
2. Row source files are not aligned with the actual dashboard unit of change.
3. Dashboard row order is maintained in more than one place.
4. The helper layer still exposes too much panel/query boilerplate while not
   providing a simple editing path for common changes.

The required refactor must make row editing read like a direct dashboard
construction workflow:

- find one row file
- create one panel variable
- add queries to that panel explicitly
- append the panel to the row
- append rows to the dashboard in display order

At the same time, the refactor must not hide dashboard meaning inside
business-specific helper APIs or implicit mutable context.

## Goals

- Introduce an additive builder-style authoring API for dashboards, rows,
  panels, and queries.
- Keep the primitive spec and render layers explicit and stable.
- Split `metrics/grafana/ticdc_new_arch/rows/` to one file per Grafana row.
- Make `metrics/grafana/ticdc_new_arch/dashboard.py` the single source of truth
  for row order.
- Remove routine boilerplate such as explicit target list assembly and query
  ref allocation from row authoring code.
- Preserve explicit PromQL intent through `expr_*` helpers and direct query
  definitions.
- Keep the common case short without introducing hidden context.
- Make the resulting API predictable enough that a row author can edit one row
  file without understanding the entire DSL implementation.

## Non-Goals

- Do not introduce business-specific helper APIs such as
  `changefeed_metric_graph(...)`.
- Do not introduce scope objects such as `changefeed.sum(...)`.
- Do not replace explicit PromQL intent with a `title + metric name` registry.
- Do not move mutable builder state into `metrics/dsl/specs.py`.
- Do not redesign Grafana semantics, panel meanings, or metric selection.
- Do not require implicit "current row" or "current panel" context.
- Do not preserve grouped row-builder entrypoints as a long-term architecture.

## Current State

### Layering

The current primitive layer is defined in:

- `metrics/dsl/specs.py`
  - `DashboardSpec`
  - `RowSpec`
  - `GraphPanelSpec`
  - `TimeSeriesPanelSpec`
  - `HeatmapPanelSpec`
  - `TablePanelSpec`
  - `TargetSpec`
- `metrics/dsl/api.py`
  - `dashboard(...)`
  - `row(...)`
  - `graph(...)`
  - `timeseries(...)`
  - `heatmap(...)`
  - `table(...)`
  - `target(...)`
- `metrics/dsl/render.py`
  - the renderer that converts immutable specs into Grafana JSON

The current TiCDC authoring layer is centered in:

- `metrics/grafana/common.py`
  - `Expr`
  - `expr_*` helpers
  - matcher helpers such as `eq(...)` and `regex(...)`
  - panel aliases such as `graph_panel(...)`
  - layout helper `Layout`

### Dashboard Assembly

`metrics/grafana/ticdc_new_arch/dashboard.py` currently:

1. calls grouped functions such as `build_overview_rows()`
2. merges returned rows into a map keyed by row title
3. rebuilds final order from `EXPECTED_ROW_TITLES`

This means the dashboard entrypoint is not a direct declaration of the
dashboard structure. It is a reconciliation step across grouped builders and an
external title list.

### Row Organization

`metrics/grafana/ticdc_new_arch/rows/__init__.py` currently exports grouped
functions:

- `build_execution_rows`
- `build_log_service_rows`
- `build_misc_rows`
- `build_overview_rows`
- `build_server_rows`
- `build_sinks_rows`

Those grouped functions are backed by files such as:

- `metrics/grafana/ticdc_new_arch/rows/overview.py`
- `metrics/grafana/ticdc_new_arch/rows/server.py`
- `metrics/grafana/ticdc_new_arch/rows/log_service.py`
- `metrics/grafana/ticdc_new_arch/rows/sinks.py`
- `metrics/grafana/ticdc_new_arch/rows/execution.py`
- `metrics/grafana/ticdc_new_arch/rows/misc.py`

Several of those files define multiple rows, which means row ownership is not
local to a single module.

### Authoring Shape

Current row authoring still usually looks like this:

```python
row(
    "Server",
    [
        graph(
            "CPU Usage",
            targets=[
                target(
                    expr=expr_rate(...),
                    legend="{{instance}}",
                    format="time_series",
                ),
                target(
                    expr=expr_simple(...),
                    legend="quota-{{instance}}",
                    ref="B",
                    format="time_series",
                ),
            ],
            unit="percentunit",
            height=9,
            span=12,
        ),
    ],
)
```

This is explicit, but it is still more like assembling a low-level object graph
than maintaining one Grafana row.

## Proposed Design

### Overview

Introduce a dedicated additive builder layer above the immutable spec layer.
The builder layer becomes the normal authoring surface for TiCDC row modules.
The immutable spec layer remains the handoff boundary to the renderer.

The design has four core rules:

1. Builders live in the authoring layer, not the spec layer.
2. Each row lives in exactly one source file.
3. Dashboard row order is declared once in `dashboard.py`.
4. `add_*` methods only append children; they never create hidden context.

### Architecture

```text
row source file
  -> authoring builders
    -> immutable specs
      -> renderer
        -> Grafana JSON

dashboard.py
  -> add_row(build_summary_row())
  -> add_row(build_server_row())
  -> add_row(...)
```

### Builder Placement

The new builder API belongs in the TiCDC authoring layer. The recommended
module boundary is:

- `metrics/dsl/specs.py`
  - remains immutable
  - no mutable builder state
- `metrics/dsl/api.py`
  - may keep low-level constructors for direct spec assembly and tests
  - does not become the main TiCDC authoring surface
- `metrics/grafana/common.py`
  - remains the main author-facing utility module for expressions and helpers
- `metrics/grafana/ticdc_new_arch/builders.py`
  - new additive builder types for dashboard/row/panel/query authoring

The exact module name can vary, but the builder API must remain above the
primitive spec layer.

### Builder Types

The authoring layer introduces these logical objects:

- `DashboardBuilder`
- `RowBuilder`
- `GraphPanelBuilder`
- `TimeSeriesPanelBuilder`
- `HeatmapPanelBuilder`
- `TablePanelBuilder`

Each builder owns only one level of structure.

#### DashboardBuilder

Responsibilities:

- store dashboard metadata needed for final `DashboardSpec`
- collect rows in final display order
- build `DashboardSpec`

Required API:

- `add_row(row: RowBuilder | RowSpec) -> DashboardBuilder`
- `build() -> DashboardSpec`

`add_row(...)` returns the dashboard object itself so authors can chain repeated
row additions if they want to. The method does not set any hidden current-row
state.

#### RowBuilder

Responsibilities:

- own one row title
- own row-level layout defaults
- collect panels in row order
- build `RowSpec`

Required API:

- `add_graph(panel: GraphPanelBuilder | GraphPanelSpec) -> RowBuilder`
- `add_timeseries(panel: TimeSeriesPanelBuilder | TimeSeriesPanelSpec) -> RowBuilder`
- `add_heatmap(panel: HeatmapPanelBuilder | HeatmapPanelSpec) -> RowBuilder`
- `add_table(panel: TablePanelBuilder | TablePanelSpec) -> RowBuilder`
- `build() -> RowSpec`

Row builder methods return the row itself. They do not create or track a
"current panel".

#### Panel Builders

Responsibilities:

- store panel metadata
- collect queries for that panel
- apply stable query defaults that are mechanically determined by panel type
- build final panel specs

Required API:

- `add_query(expr: str | Expr, *, legend: str | None = None, ref: str | None = None, hide: bool = False, format: str | None = None, instant: bool | None = None) -> PanelBuilder`
- `build() -> GraphPanelSpec | TimeSeriesPanelSpec | HeatmapPanelSpec | TablePanelSpec`

`panel.add_query(...)` returns the panel object itself. It does not allocate any
global state and does not mutate parent objects.

### Query Construction Rule

The new API removes explicit `targets=[target(...)]` assembly from normal row
files. Query creation is performed through `panel.add_query(...)`.

Normal authoring becomes:

```python
cpu = graph_panel("CPU Usage", unit="percentunit")
cpu.add_query(expr_rate(...), legend="{{instance}}")
cpu.add_query(expr_simple(...), legend="quota-{{instance}}")
row.add_graph(cpu)
```

The following inline style remains technically possible because `add_query(...)`
returns the panel builder:

```python
row.add_graph(
    graph_panel("CPU Usage").add_query(...).add_query(...)
)
```

However, this repository should not recommend that style. The preferred author
workflow is explicit sequential construction:

1. find the row file
2. create a panel variable
3. add queries to the panel
4. append the panel to the row

This keeps edits visually direct and avoids burying panel contents inside inline
expressions.

### Defaulting Rules

The builder layer may absorb only mechanical boilerplate.

#### Allowed Defaults

- automatic query `ref` allocation as `A`, `B`, `C`, ...
- panel-type-based default query `format`
  - graph and timeseries: `time_series`
  - heatmap: `heatmap`
  - table: no special forced format unless current rendering requires one
- row-level default `height`
- row-level default `span`
- row-level `collapsed=True` default

These defaults are stable because they do not infer metric semantics.

#### Explicit-Only Fields

The following must remain explicit in row modules because they affect chart
meaning:

- `expr`
- `legend`
- panel type
- `unit`
- `description`
- thresholds
- overrides
- table transformations
- histogram percentile or average intent
- any special layout override on a specific panel

The builder layer must not infer those from metric names.

### Expression Helper Boundary

`metrics/grafana/common.py` should continue to own:

- generic `expr_*` helpers
- matcher helpers such as `eq(...)`, `regex(...)`, `not_regex(...)`

It must not grow new business helpers such as:

- `changefeed_metric_graph(...)`
- `sink_batch_histogram_panel(...)`
- `changefeed.sum(...)`

The repository keeps explicit metric semantics in row files and avoids turning
the authoring surface into a hidden business-specific DSL.

### Row File Layout

The row directory is refactored to one row per file. Examples:

- `metrics/grafana/ticdc_new_arch/rows/summary.py`
- `metrics/grafana/ticdc_new_arch/rows/lag_summary.py`
- `metrics/grafana/ticdc_new_arch/rows/dataflow.py`
- `metrics/grafana/ticdc_new_arch/rows/server.py`
- `metrics/grafana/ticdc_new_arch/rows/changefeed.py`
- `metrics/grafana/ticdc_new_arch/rows/lag_analyze.py`

Each file exports one public builder function:

```python
def build_server_row() -> RowSpec:
    ...
```

The implementation shape inside each file is standardized:

```python
def build_server_row() -> RowSpec:
    row = row_builder("Server", default_height=9)

    cpu = graph_panel("CPU Usage", unit="percentunit")
    cpu.add_query(expr_rate(...), legend="{{instance}}")
    cpu.add_query(expr_simple(...), legend="quota-{{instance}}")
    row.add_graph(cpu)

    memory = graph_panel("Memory Usage", unit="bytes")
    memory.add_query(expr_simple(...), legend="process-{{instance}}")
    row.add_graph(memory)

    return row.build()
```

This structure matches how authors actually edit dashboards.

### Dashboard Assembly

`metrics/grafana/ticdc_new_arch/dashboard.py` becomes the single declaration of
row order.

Target structure:

```python
dashboard = dashboard_builder(
    title=...,
    uid=...,
    variables=build_templating(),
    annotations=build_annotations(),
)

dashboard.add_row(build_summary_row())
dashboard.add_row(build_lag_summary_row())
dashboard.add_row(build_dataflow_row())
dashboard.add_row(build_server_row())
...

return render_dashboard(dashboard.build())
```

This removes the current "group builders plus reorder by title" flow.

As a result:

- `EXPECTED_ROW_TITLES` no longer needs to be a second order definition
- `rows/__init__.py` becomes thin re-export glue or can be removed entirely
- adding a new row requires one new row file and one new `dashboard.add_row(...)`
  call

### Validation

Row order validation should derive from the same row list used for assembly.
The implementation must not maintain a separate manual title sequence for
correctness checks.

If the dashboard identity validator remains in `metrics/grafana/common.py`, it
should accept an expected ordered row-title list generated from the dashboard
builder input rather than an unrelated constant.

## Detailed Design

### Data Flow

The intended construction flow is:

```text
DashboardBuilder
  add_row(RowBuilder or RowSpec)
    RowBuilder
      add_graph(GraphPanelBuilder or GraphPanelSpec)
        GraphPanelBuilder
          add_query(Expr or str, legend=..., ...)
      add_table(...)
      add_heatmap(...)
  build()
    -> DashboardSpec
    -> render_dashboard(...)
```

### Build Invariants

The builder layer must preserve these invariants:

1. A dashboard preserves row insertion order exactly.
2. A row preserves panel insertion order exactly.
3. A panel preserves query insertion order exactly.
4. `build()` produces immutable specs and does not expose mutable internal
   lists.
5. `add_*` methods never depend on hidden current-child context.
6. Automatic query `ref` allocation is deterministic and local to one panel.

### Migration Strategy

Migration should be full and not leave mixed authoring models behind inside the
TiCDC new-arch dashboard.

Recommended order:

1. add the builder implementation and tests
2. convert dashboard assembly to explicit `add_row(...)`
3. split grouped row files to one row per file
4. migrate each row file to the new authoring pattern
5. remove obsolete grouped row-builder exports and ordering constants
6. update README examples to the new pattern

The end state should not keep a half-migrated mixture of:

- grouped row-list builders
- `Layout(...).row([...])`
- builder-based row modules

Temporary compatibility wrappers may exist during the patch series, but the
final codebase must expose one primary authoring model.

### Compatibility Considerations

The renderer and immutable spec structures should remain compatible with the
current generated JSON shape. This refactor is about authoring structure, not
dashboard behavior.

Compatibility constraints:

- final rendered JSON must remain stable aside from intended source-driven row
  or panel reordering
- panel query order must remain unchanged
- existing `expr_*` behavior must remain usable from row files

### Failure Modes

Potential implementation failures include:

- accidental row reordering during migration
- query `ref` changes that alter panel behavior or table transforms
- hidden builder defaults that alter panel meaning
- mixed old/new authoring paths that cause duplicated rows or divergent output

The design avoids these by:

- keeping row order explicit in `dashboard.py`
- constraining defaults to mechanical fields only
- building immutable specs as the renderer handoff
- testing both builder behavior and final rendered dashboard structure

## Performance Considerations

This refactor is not on a hot runtime path. It runs during dashboard generation.
Still, the design should avoid unnecessary complexity:

- builder objects should be thin wrappers around Python lists and metadata
- `build()` should perform straightforward list copying into immutable spec
  objects
- no registry lookup or late semantic inference should be introduced
- avoid repeated deep conversions during every `add_*` call; defer spec
  materialization until `build()`

The main performance goal is simplicity rather than micro-optimization.

## Testing Strategy

### Unit Tests

Add focused tests for:

- `DashboardBuilder.add_row(...)` preserves insertion order
- `RowBuilder.add_graph(...)` and peers preserve panel order
- `PanelBuilder.add_query(...)` preserves query order
- automatic query ref assignment
- explicit ref override behavior
- row-level default layout propagation
- panel-level explicit layout override behavior
- `build()` returns immutable spec objects with expected values

### Integration Verification

Existing dashboard generation and validation commands must continue to pass:

- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `python3 scripts/gen-ticdc-dashboards`
- `python3 scripts/check-ticdc-dashboard.py`
- `./scripts/check-ticdc-dashboard.sh`

Add or update tests to ensure:

- the expected row order is produced from `dashboard.py`
- the generated JSON remains stable for representative migrated rows

## Observability / Operations

The generated dashboard artifacts remain the primary operational output. The
refactor does not add runtime observability features.

For debugging authoring problems, the important signals remain:

- unit test failures in the builder layer
- dashboard generation failures
- dashboard structure validation failures
- diff review of generated JSON artifacts

## Rollout Plan

1. Land the builder layer and its tests.
2. Convert `dashboard.py` to explicit ordered row assembly.
3. Split row modules to one row per file.
4. Migrate row files to explicit sequential panel construction.
5. Remove grouped row-builder APIs and stale ordering constants.
6. Regenerate dashboard JSON and validation artifacts.
7. Update authoring documentation in `metrics/grafana/README.md`.

Rollback is straightforward because the change is source-only. Reverting the
patch series restores the previous authoring model and generated artifacts.

## Alternatives Considered

### 1. Keep Function Constructors and Only Add More Helpers

Rejected because it leaves the main authoring workflow as nested spec assembly.
It reduces some typing but does not address row locality or additive editing.

### 2. Add Hidden Current-Row and Current-Panel Context

Rejected because it makes the API shorter by adding invisible state. That
reduces predictability and makes future edits harder to reason about.

### 3. Introduce Business-Specific Metric Helpers

Rejected because it hides dashboard semantics behind a second domain-specific
language. The result would be shorter code, but less transparent code.

### 4. Keep Grouped Row Files and Only Re-export One-Row Builders

Rejected because it preserves the main navigation problem. The unit of change
must be the row file itself.

## Open Questions / Future Work

No functional open questions remain for the builder refactor itself.

Future cleanup is possible after migration if additional generic panel defaults
prove repetitive, but any such follow-up must keep the same rule: absorb
boilerplate, not intent.

## References

- `metrics/dsl/specs.py`
- `metrics/dsl/api.py`
- `metrics/dsl/render.py`
- `metrics/grafana/common.py`
- `metrics/grafana/README.md`
- `metrics/grafana/ticdc_new_arch/dashboard.py`
- `metrics/grafana/ticdc_new_arch/rows/__init__.py`
- `metrics/grafana/ticdc_new_arch/rows/overview.py`
- `metrics/grafana/ticdc_new_arch/rows/server.py`
- `metrics/grafana/ticdc_new_arch/rows/execution.py`
- `docs/superpowers/specs/2026-04-01-ticdc-tikv-style-authoring-boundary-design.md`
