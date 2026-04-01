# TiCDC TiKV-Style Authoring Boundary Design

## Summary

This document defines a narrower and clearer authoring boundary for TiCDC
Grafana dashboards.

The goal is not to introduce business-specific helpers such as
`changefeed_metric_graph(...)`, nor to reduce dashboard authoring to a hidden
`title + metric` registry. The goal is to make TiCDC row modules read like TiKV
dashboard source files:

- layout is managed centrally
- panel builders stay explicit
- expression helpers stay explicit
- repeated scope, group-by, legend, and layout boilerplate disappears

The intended authoring style is:

```python
layout = Layout("Scheduler", panel_height=6)

with query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
    layout.row(
        [
            graph_panel(
                title="Slowest Table Replication State",
                unit="none",
                min=0,
                targets=[
                    target(expr_sum("ticdc_scheduler_slow_table_replication_state"))
                ],
                description=SLOWEST_TABLE_REPLICATION_STATE_DESCRIPTION,
            ),
        ]
    )
```

That keeps the TiKV-style `graph_panel(...) + target(expr_*(...))` skeleton
while moving repeated defaults into `metrics/grafana/common.py`.

## Background

The current TiCDC dashboard-as-code work already split the system into two
layers:

- `metrics.dsl`: internal primitive rendering layer
- `metrics.grafana.common`: author-facing helper layer

This is the right high-level direction, but the current helper layer still
leaks too much configuration into row modules. In representative row files such
as `metrics/grafana/ticdc_new_arch/rows/execution.py`, authors still repeat the
same parameters many times:

- `scope="changefeed"` or another repeated scope
- `by_labels=[...]`
- `legend_format="..."`
- `span=12`
- `height=6` or `height=7`

As a result, a simple panel still reads like a low-level query assembly task
instead of a dashboard authoring task.

TiKV's dashboard source is clearer for two reasons:

1. `common.py` owns layout defaults through `Layout.row(...)`
2. `common.py` owns query defaults through expression and target helpers

TiKV does not hide all authoring decisions. It still exposes
`graph_panel(...)`, `target(...)`, and `expr_sum(...)`. The clarity comes from
having strong defaults for the common case and explicit escape hatches for the
unusual case.

TiCDC should adopt the same authoring shape.

## Goals

- Make TiCDC row modules read like TiKV row modules.
- Keep `metrics/grafana/common.py` as the primary authoring boundary.
- Keep `metrics.dsl` as the internal rendering layer.
- Remove repeated layout parameters from normal row code.
- Remove repeated scope, group-by, and legend boilerplate from normal row code.
- Preserve explicit `expr_*` helpers in author-facing code.
- Preserve the ability to write unusual panels without fighting the API.
- Allow incremental migration of row modules.

## Non-Goals

- No business-specific panel helpers such as `changefeed_metric_graph(...)`.
- No scope objects with business methods such as `changefeed.sum(...)`.
- No `title + metric` registry that infers all panel behavior from hidden
  metadata.
- No attempt to hide all PromQL semantics from row authors.
- No dashboard semantic redesign.
- No requirement to migrate every row module in one change.

## Design Principles

- Common cases should be short.
- Special cases should remain explicit.
- Defaults must be visible at row scope, not hidden in a distant registry.
- The author-facing API should feel like TiKV, even if the internal renderer is
  different.
- `common.py` should absorb repetition, not intent.

## Proposed Design

### 1. Layout Owns Panel Placement

Introduce a TiKV-style `Layout` helper in `metrics/grafana/common.py`.

Responsibilities:

- hold one row title
- hold a default panel height for that row section
- distribute panel widths evenly across a 24-column Grafana row
- assign `x` positions automatically
- support repeated row patterns if needed later

Normal row code should stop setting `span`, `width`, `height`, and `x` on every
panel. Those details should be inferred by `Layout.row(...)`.

Panel builders should still allow explicit overrides for unusual cases, but the
expected path for common rows is:

```python
layout = Layout("Scheduler", panel_height=6)
layout.row([panel_a, panel_b])
```

This mirrors TiKV and removes a large amount of row-local noise without
introducing business-specific abstractions.

### 2. Query Defaults Are Declared Once Per Local Section

Introduce a thin query-default mechanism in `metrics/grafana/common.py`:

```python
with query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
    ...
```

The purpose of this mechanism is limited:

- provide a default TiCDC scope
- provide default group-by labels
- provide any other truly repetitive query defaults later if needed

It is not a business object and must not grow methods such as `.sum(...)` or
`.rate(...)`.

The row author still writes `expr_sum(...)`, `expr_rate(...)`,
`expr_histogram_quantile(...)`, and so on. The defaults only fill in omitted
arguments.

For example:

```python
with query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
    target(expr_sum("ticdc_scheduler_slow_table_replication_state"))
```

behaves like today's more verbose form:

```python
target(
    expr_sum(
        "ticdc_scheduler_slow_table_replication_state",
        scope="changefeed",
        by_labels=["namespace", "changefeed"],
    )
)
```

This keeps the author-facing structure explicit while eliminating the repeated
arguments that currently dominate row files.

### 3. Expression Helpers Must Support Delayed Rendering

The current `Expr` implementation is effectively an eager string wrapper. That
is too early for row-local defaults, because `scope` and `by_labels` have
already been baked into the string before `target(...)` sees the expression.

To support TiKV-style defaults, expression helpers should return an internal
expression object that stores semantic intent rather than a final string:

- metric name
- aggregate function
- transform function
- selectors
- range window
- explicit scope override, if any
- explicit group-by override, if any
- operator composition for arithmetic expressions

That object should render to PromQL only after the effective defaults are
resolved.

This is an internal implementation detail. Row authors should still think in
terms of:

- `expr_simple(...)`
- `expr_sum(...)`
- `expr_rate(...)`
- `expr_histogram_quantile(...)`
- `expr_operator(...)`

The delayed-rendering object only exists so the author-facing API can become
shorter without becoming magical.

### 4. Explicit Arguments Override Defaults

Defaults should reduce repetition, not remove control.

The precedence rule should be simple:

1. explicit arguments on the current expression
2. active `query_defaults(...)`
3. built-in fallback defaults in `common.py`

Examples:

- `expr_sum("m")` inside `query_defaults(scope="changefeed", by_labels=[...])`
  uses the active defaults
- `expr_sum("m", scope="cluster")` inside that block uses `scope="cluster"`
- `expr_sum("m", by_labels=["namespace", "changefeed", "state"])` overrides the
  default group-by labels for that expression only

This keeps special panels readable and avoids forcing authors to break out of
the new authoring model whenever a row mixes scopes or grouping dimensions.

### 5. `target(...)` Should Infer Legend Format for the Common Case

Today row authors often repeat a legend that is mechanically derived from
`by_labels`.

For helper-built expressions, `target(...)` should infer a legend format when
the caller does not specify one:

- `by_labels=["instance"]` -> `{{instance}}`
- `by_labels=["namespace", "changefeed"]` ->
  `{{namespace}}-{{changefeed}}`
- `by_labels=["namespace", "changefeed", "state"]` ->
  `{{namespace}}-{{changefeed}}-{{state}}`

Explicit `legend_format=` must still win.

That keeps the common case short while preserving explicit legends for panels
that need prefixes, suffixes, or non-mechanical label presentation such as:

- `add-{{namespace}}-{{changefeed}}-{{mode}}`
- `avg-{{namespace}}-{{changefeed}}-{{instance}}`

### 6. Panel Builders Keep the TiKV-Style Skeleton

The author-facing panel builders should remain explicit and boring:

- `graph_panel(...)`
- `timeseries_panel(...)`
- `heatmap_panel(...)`
- `table_panel(...)`
- `target(...)`

They should receive stronger defaults from layout and query context, but they
should not be replaced by business helpers.

This is the core boundary:

- row authors compose panels with panel builders and expression helpers
- `common.py` owns authoring defaults and readability helpers
- `metrics.dsl` owns JSON/spec rendering details

### 7. Raw PromQL Escape Hatch Must Remain Available

Some existing panels are unusual enough that a helper expression may not be the
best fit, for example:

- scalar expressions
- multi-stage arithmetic
- boolean comparisons
- special instant/range combinations

The design must keep an escape hatch:

- raw PromQL strings can still be passed to `target(...)`
- explicit legend, scope, and layout overrides still work

The new authoring model should make the common path clean without making the
uncommon path impossible.

## Example: Before and After

### Current Style

```python
graph(
    "Slowest Table Replication State",
    targets=[
        series_target(
            expr=expr_sum(
                "ticdc_scheduler_slow_table_replication_state",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            ),
            legend_format="{{namespace}}-{{changefeed}}",
        )
    ],
    description=SLOWEST_TABLE_REPLICATION_STATE_DESCRIPTION,
    unit="none",
    min="0",
    span=12,
    height=6,
)
```

### Intended Style

```python
layout = Layout("Scheduler", panel_height=6)

with query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
    layout.row(
        [
            graph_panel(
                title="Slowest Table Replication State",
                targets=[
                    target(
                        expr_sum("ticdc_scheduler_slow_table_replication_state")
                    )
                ],
                description=SLOWEST_TABLE_REPLICATION_STATE_DESCRIPTION,
                unit="none",
                min=0,
            ),
        ]
    )
```

The author still writes:

- the panel type
- the metric expression helper
- the panel title
- the user-visible unit and description

The author no longer repeats:

- row layout settings
- routine scope selection
- routine group-by labels
- routine legend rendering

## Internal Layering

The layers after this change should be:

```text
row module
  -> metrics.grafana.common
     -> metrics.dsl
        -> Grafana JSON
```

Responsibilities:

- row module:
  - choose panels
  - choose titles
  - choose user-visible descriptions and units
  - choose any exceptional selectors or legends
- `metrics.grafana.common`:
  - layout defaults
  - query defaults
  - expression helper objects
  - target defaults
  - panel defaults
- `metrics.dsl`:
  - immutable panel/target specs
  - Grafana JSON rendering

`metrics.dsl` should not continue to grow as the main author-facing surface.

## Migration Plan

### Phase 1: Lock the New Boundary with Tests

Add focused failing tests for:

- `Layout.row(...)` width distribution and height assignment
- `query_defaults(...)` applying default scope and `by_labels`
- explicit expression arguments overriding defaults
- automatic legend inference in `target(...)`
- raw PromQL passthrough remaining valid

### Phase 2: Implement the New `common.py`

Refactor `metrics/grafana/common.py` to provide:

- `Layout`
- `query_defaults(...)`
- delayed-rendering expression objects
- `target(...)` legend inference
- panel helpers with layout-friendly defaults

Preserve current helper names where practical so row migration diffs stay
focused on authoring simplification rather than broad API churn.

### Phase 3: Migrate Representative Rows

Migrate at least one row file that currently demonstrates the problem clearly:

- `metrics/grafana/ticdc_new_arch/rows/execution.py`

This file contains repeated `scope`, `by_labels`, `legend_format`, `span`, and
`height` values and is therefore the best first proof that the new boundary is
working.

After that, migrate additional representative rows such as:

- `metrics/grafana/ticdc_new_arch/rows/overview.py`
- `metrics/grafana/ticdc_new_arch/rows/sinks.py`

### Phase 4: Document the Authoring Style

Update `metrics/grafana/README.md` so it clearly states:

- `metrics.grafana.common` is the author-facing layer
- `metrics.dsl` is the internal rendering layer
- new row code should prefer `Layout`, `query_defaults`, `graph_panel`,
  `target`, and `expr_*`
- raw PromQL remains available for special cases

## Testing and Verification

Verification should focus on both behavior and authoring clarity.

Required checks:

- focused unit tests for the new `common.py` behavior
- semantic row tests showing migrated rows render the same dashboard content
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `PYTHONDONTWRITEBYTECODE=1 python3 scripts/check-ticdc-dashboard.py`
- `python3 scripts/gen-ticdc-dashboards`

Success criteria:

- rendered dashboard JSON remains semantically unchanged for migrated panels
- representative rows no longer repeat `span`, `height`, `scope`, `by_labels`,
  and mechanical `legend_format` values in the common case
- unusual panels still remain expressible without awkward workarounds

## Alternatives Considered

### Business-Specific Graph Helpers

Example:

```python
changefeed_metric_graph(...)
```

Rejected because it hides too much query intent and pushes the author-facing API
toward a growing set of special-case helpers.

### Scope Objects with Methods

Example:

```python
changefeed.sum("metric")
```

Rejected because it turns scope into a business object instead of keeping the
TiKV-style `expr_*` helper model.

### Title Plus Metric Only

Example:

```python
graph_metric("Title", "metric_name")
```

Rejected because the missing information must then move into a central metadata
registry. That would hide authoring rules in a less readable place and diverge
from TiKV's explicit style.

## Risks and Mitigations

### Risk: Defaults Become Too Implicit

Mitigation:

- keep defaults row-local through explicit `with query_defaults(...):`
- keep explicit overrides available on every expression helper
- avoid a repository-wide hidden registry

### Risk: Delayed Rendering Breaks Existing Call Sites

Mitigation:

- keep `target(...)` accepting raw strings
- preserve existing helper names
- add compatibility tests for mixed old and new usage during migration

### Risk: Mixed-Scope Panels Become Harder To Read

Mitigation:

- explicit expression arguments override defaults
- nested or separate `query_defaults(...)` blocks can be used for sections that
  need different defaults

## Decision

Adopt a TiKV-style authoring boundary for TiCDC:

- explicit `Layout`
- explicit `graph_panel(...) + target(expr_*(...))`
- row-local query defaults
- automatic legend inference for mechanical cases
- no business helper expansion
- no hidden metric registry

This gives TiCDC the same clarity benefits as TiKV without forcing TiCDC's more
varied scopes into a business-specific API.
