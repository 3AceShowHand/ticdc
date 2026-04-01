# TiCDC TiKV-Style Authoring Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor TiCDC dashboard authoring so row modules follow a TiKV-style `Layout + graph_panel(...) + target(expr_*(...))` pattern with row-local query defaults instead of repeating `scope`, `by_labels`, `legend_format`, `span`, and `height`.

**Architecture:** Keep `metrics.dsl` as the internal renderer and keep `metrics/grafana/common.py` as the author-facing façade. Add TiKV-style layout management, row-local query defaults, and delayed expression rendering in `common.py`, then migrate representative rows so common panels read declaratively while unusual panels still have explicit escape hatches.

**Tech Stack:** Python 3, `unittest`, existing TiCDC renderer under `metrics/dsl`, author-facing helpers in `metrics/grafana/common.py`, TiKV reference implementation in `/root/tikv/metrics/grafana/common.py`

---

## File Map

- Modify: `scripts/tests/test_ticdc_dsl.py`
  - Lock behavior for `Layout`, `query_defaults(...)`, delayed rendering, legend inference, and raw PromQL passthrough.
- Modify: `scripts/tests/test_ticdc_dashboard_rows.py`
  - Keep semantic row render coverage for migrated rows and add focused assertions when a representative row is migrated.
- Modify: `metrics/grafana/common.py`
  - Implement the new author-facing boundary: delayed-rendering expressions, query-default context, layout defaults, and target legend inference.
- Modify: `metrics/grafana/ticdc_new_arch/rows/execution.py`
  - Migrate the `Scheduler` and `TiKV` rows away from repeated layout and query boilerplate.
- Modify: `metrics/grafana/ticdc_new_arch/rows/overview.py`
  - Migrate one representative overview section that currently repeats common `scope/by/legend/height`.
- Modify: `metrics/grafana/ticdc_new_arch/rows/sinks.py`
  - Migrate at least one sink row if the new boundary is still ergonomic outside execution/overview.
- Modify: `metrics/grafana/README.md`
  - Document `metrics.grafana.common` as the author-facing API and show the preferred TiKV-style pattern.

## Implementation Notes

- Follow `@test-driven-development` for new helper behavior:
  - add a failing test
  - run it and verify the failure is for the expected reason
  - implement the minimum code
  - rerun the same test until it passes
- Treat row migration as refactoring guarded by existing semantic row tests. Do not add brittle source-text assertions just to force a red test.
- Keep the public authoring skeleton explicit:
  - `Layout(...)`
  - `graph_panel(...)`
  - `target(...)`
  - `expr_*`
- Do not introduce:
  - `changefeed_metric_graph(...)`
  - business scope objects like `changefeed.sum(...)`
  - a hidden metric metadata registry
- Prefer compatibility over churn. If old call sites can be supported during migration without distorting the design, keep them working until all representative rows are migrated.

### Task 1: Lock the New Common-Layer Behavior with Failing Tests

**Files:**
- Modify: `scripts/tests/test_ticdc_dsl.py`

- [ ] Add a failing test for row-local query defaults applying TiCDC scope and group-by labels.

```python
def test_common_query_defaults_apply_scope_and_by_labels(self):
    common = require_module(self, "metrics.grafana.common")

    with common.query_defaults(
        scope="changefeed",
        by_labels=["namespace", "changefeed"],
    ):
        expr = common.expr_sum("ticdc_scheduler_slow_table_replication_state")

    self.assertEqual(
        'sum(ticdc_scheduler_slow_table_replication_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        str(expr),
    )
```

- [ ] Run the focused test file and verify it fails because `query_defaults` does not exist yet or because `expr_sum(...)` still renders without the default scope/group-by.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- FAIL in the new `query_defaults` test
- failure points at missing symbol or incorrect rendered PromQL

- [ ] Add a failing test for `target(...)` inferring `legend_format` from the effective `by_labels`.

```python
def test_common_target_infers_legend_from_effective_by_labels(self):
    common = require_module(self, "metrics.grafana.common")

    with common.query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
        target = common.target(common.expr_sum("ticdc_scheduler_slow_table_replication_state"))

    self.assertEqual("{{namespace}}-{{changefeed}}", target.legend)
```

- [ ] Rerun the focused test file and verify the new legend inference test fails for the expected reason.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- FAIL in the legend inference test
- failure points at `None` or the wrong legend text

- [ ] Add a failing test for `Layout` applying row height automatically without panel-local `span/height`.

```python
def test_common_layout_uses_row_panel_height_defaults(self):
    common = require_module(self, "metrics.grafana.common")
    render = require_module(self, "metrics.dsl.render")

    layout = common.Layout(title="Scheduler", panel_height=6)
    with common.query_defaults(scope="instance", by_labels=["instance"]):
        layout.row(
            [
                common.graph_panel(title="A", targets=[common.target(common.expr_sum("metric_a"))]),
                common.graph_panel(title="B", targets=[common.target(common.expr_sum("metric_b"))]),
            ]
        )

    rendered = render.render_row(layout.row_panel, row_index=0, start_panel_id=1)
    self.assertEqual(6, rendered["panels"][0]["gridPos"]["h"])
    self.assertEqual(6, rendered["panels"][1]["gridPos"]["h"])
```

- [ ] Rerun the focused test file and verify the new layout-default test fails because `Layout` still hardcodes the current behavior or ignores the requested panel height.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- FAIL in the new layout-default test
- failure points at panel height remaining `7`

- [ ] Add a failing test for raw PromQL passthrough staying valid when `query_defaults(...)` is active.

```python
def test_common_target_keeps_raw_promql_passthrough(self):
    common = require_module(self, "metrics.grafana.common")

    with common.query_defaults(scope="changefeed", by_labels=["namespace", "changefeed"]):
        target = common.target('scalar(max(pd_cluster_tso{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}))')

    self.assertEqual(
        'scalar(max(pd_cluster_tso{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}))',
        target.expr,
    )
```

- [ ] Rerun the focused test file and verify the passthrough test fails only if the new defaults leak into raw PromQL strings.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- current tests still fail only in the newly added cases
- no unrelated regressions

### Task 2: Implement Delayed Rendering, Query Defaults, and Layout Defaults in `common.py`

**Files:**
- Modify: `metrics/grafana/common.py`

- [ ] Add a lightweight query-default context mechanism, preferably with `contextvars.ContextVar` plus a small `contextmanager`.

```python
@dataclass(frozen=True)
class QueryDefaults:
    scope: ScopeName | None = None
    by_labels: tuple[str, ...] = ()


@contextmanager
def query_defaults(*, scope: ScopeName | None = None, by_labels: Sequence[str] = ()):
    ...
```

- [ ] Run the focused tests and confirm the new `query_defaults` tests still fail until expressions start resolving against the active defaults.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- FAIL in the tests that depend on unresolved expression rendering

- [ ] Replace the eager string-like expression implementation with a delayed-rendering expression object that stores intent instead of final PromQL text.

```python
@dataclass(frozen=True)
class Expr:
    kind: str
    metric: str | Expr | None = None
    op: str | None = None
    args: tuple[Expr | str, ...] = ()
    selectors: tuple[SelectorLike, ...] = ()
    scope: ScopeName | None = None
    by_labels: tuple[str, ...] | None = None
    window: str | None = None

    def render(self, defaults: QueryDefaults | None = None) -> str:
        ...
```

- [ ] Update `expr_simple(...)`, `expr_sum(...)`, `expr_rate(...)`, `expr_histogram_quantile(...)`, `expr_histogram_avg(...)`, `expr_operator(...)`, and related helpers to return the delayed-rendering expression object while preserving their current call signatures.

- [ ] Update `__str__` or equivalent rendering so existing explicit call sites like `str(common.expr_sum(..., scope="changefeed", by_labels=[...]))` continue to work.

- [ ] Update `target(...)` and `heatmap_target(...)` so they:
  - resolve effective defaults when rendering helper-built expressions
  - auto-generate legend text from the effective `by_labels` when `legend_format` is omitted
  - keep raw PromQL strings unchanged

- [ ] Update `Layout` so it accepts `panel_height` and uses that default when assigning `gridPos`.

- [ ] Preserve escape hatches:
  - explicit `scope=` on an expression overrides the active context
  - explicit `by_labels=` on an expression overrides the active context
  - explicit `legend_format=` overrides inferred legend text
  - raw strings passed to `target(...)` bypass helper resolution

- [ ] Run the focused common-layer test file until it passes cleanly.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dsl.py' -v
```

Expected:

- PASS for all `DSLPrimitiveTest` cases
- no warnings or unexpected tracebacks

- [ ] Commit the common-layer refactor once the focused tests are green.

Run:

```bash
git add scripts/tests/test_ticdc_dsl.py metrics/grafana/common.py
git commit -m "metrics: add TiKV-style TiCDC authoring defaults"
```

Expected:

- a single commit containing the new common-layer behavior and its unit tests

### Task 3: Migrate the `Scheduler` Row in `execution.py`

**Files:**
- Modify: `metrics/grafana/ticdc_new_arch/rows/execution.py`
- Test: `scripts/tests/test_ticdc_dashboard_rows.py`

- [ ] Read the existing `Scheduler` row and group panels by repeated defaults before editing.

Target groups to look for:

- `scope="changefeed"` with `by_labels=["namespace", "changefeed"]`
- `scope="changefeed"` with `by_labels=["namespace", "changefeed", "mode"]`
- row-local `height=6`
- legends that are purely mechanical versus legends with prefixes like `add-...`

- [ ] Refactor the row to use `Layout("Scheduler", panel_height=6)`.

- [ ] Wrap common panel clusters in `with query_defaults(...):` blocks instead of repeating `scope=` and `by_labels=` on every expression.

- [ ] Remove explicit `legend_format=` where it is mechanically derived from `by_labels` and should now be inferred by `target(...)`.

- [ ] Keep explicit `legend_format=` only where the rendered legend adds extra text not derivable from labels, such as:
  - `add-...`
  - `move-...`
  - `split-...`
  - `merge-...`
  - `avg-...`

- [ ] Preserve all semantic query details for unusual panels by keeping explicit overrides or raw PromQL where needed.

- [ ] Run the focused row render test and verify the migrated `Scheduler` and `TiKV` rows still match the reference dashboard.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dashboard_rows.py' -v
```

Expected:

- PASS for `test_execution_rows_match_reference_after_normalization`
- no row layout or query regressions

- [ ] Commit the execution row migration.

Run:

```bash
git add metrics/grafana/ticdc_new_arch/rows/execution.py scripts/tests/test_ticdc_dashboard_rows.py
git commit -m "metrics: migrate execution rows to TiKV-style authoring"
```

Expected:

- the commit is mostly boilerplate removal plus new `Layout/query_defaults` usage

### Task 4: Migrate One Overview Section with the Same Boundary

**Files:**
- Modify: `metrics/grafana/ticdc_new_arch/rows/overview.py`
- Test: `scripts/tests/test_ticdc_dashboard_rows.py`

- [ ] Pick one overview section that currently repeats common `scope`, `by_labels`, `legend_format`, and height settings.

Recommended targets:

- `Summary`
- `Lag Summary`
- `Dataflow`

- [ ] Refactor only the representative subset needed to prove the new boundary works outside `execution.py`.

- [ ] Prefer `Layout(panel_height=6 or 7)` plus `query_defaults(...)` blocks over panel-local `span`/`height` and expression-local repeated scope/group-by.

- [ ] Keep special panels explicit if they use `scope="none"` or unusual raw PromQL.

- [ ] Run the focused row render tests and confirm overview rows still match the reference dashboard.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dashboard_rows.py' -v
```

Expected:

- PASS for `test_overview_rows_match_reference_after_normalization`
- existing execution-row test stays green

- [ ] Commit the representative overview migration.

Run:

```bash
git add metrics/grafana/ticdc_new_arch/rows/overview.py
git commit -m "metrics: migrate overview rows to shared authoring defaults"
```

Expected:

- commit contains authoring simplification without semantic query changes

### Task 5: Extend the Migration to a Sink Row Only If the Abstraction Still Feels Clean

**Files:**
- Modify: `metrics/grafana/ticdc_new_arch/rows/sinks.py`
- Test: `scripts/tests/test_ticdc_dashboard_rows.py`

- [ ] Review one sink row with repeated height and straightforward expressions.

Recommended candidate:

- `Sink - General`

- [ ] Migrate only the panels that benefit from the new boundary without forcing awkward defaults onto histogram-heavy or special-case panels.

- [ ] If the sink row becomes harder to read with `query_defaults(...)`, stop after `execution.py` and `overview.py` and leave `sinks.py` for a later follow-up.

- [ ] Run the focused row render tests if any sink row is changed.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_ticdc_dashboard_rows.py' -v
```

Expected:

- PASS for `test_sink_rows_match_reference_after_normalization`

- [ ] Commit sink-row migration only if it improves readability.

Run:

```bash
git add metrics/grafana/ticdc_new_arch/rows/sinks.py
git commit -m "metrics: migrate sink rows to shared authoring defaults"
```

Expected:

- skip this commit entirely if no sink changes are warranted

### Task 6: Document the Authoring Boundary

**Files:**
- Modify: `metrics/grafana/README.md`

- [ ] Update the README to make `metrics.grafana.common` the clearly documented author-facing layer.

- [ ] Replace examples that push authors toward `metrics.dsl.ticdc` or repeated panel-local layout with examples that show:
  - `Layout(...)`
  - `with query_defaults(...):`
  - `graph_panel(...)`
  - `target(expr_sum(...))`

- [ ] Explicitly document the three allowed styles:
  - common path: `Layout + query_defaults + graph_panel + expr_*`
  - special case: explicit `scope/by_labels/legend_format` override
  - escape hatch: raw PromQL in `target(...)`

- [ ] Run the focused test suite after README changes only if code examples were copied from live code and might have influenced imports or modules.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected:

- PASS if any code-side edits accompanied the doc update

- [ ] Commit the README update.

Run:

```bash
git add metrics/grafana/README.md
git commit -m "docs: document TiKV-style TiCDC authoring boundary"
```

Expected:

- documentation reflects the new preferred authoring pattern

### Task 7: Full Verification Before Completion

**Files:**
- Verify only

- [ ] Run the full Python test suite.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected:

- PASS for all dashboard DSL and row render tests

- [ ] Run the dashboard structural checker.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check-ticdc-dashboard.py
```

Expected:

- PASS with no duplicate panel IDs or layout-overlap failures

- [ ] Regenerate dashboard artifacts.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/gen-ticdc-dashboards
```

Expected:

- generated JSON and checksum files update only if intended
- no unexpected unrelated artifact diffs

- [ ] Re-run the dashboard structural checker after regeneration.

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check-ticdc-dashboard.py
```

Expected:

- PASS on regenerated artifacts

- [ ] Review the final diff for authoring-boundary goals.

Checklist:

- `execution.py` and the representative overview section no longer repeat
  `scope`, `by_labels`, `legend_format`, `span`, and `height` for common cases
- no business-specific helpers were introduced
- no metric registry was introduced
- raw PromQL escape hatches still exist where needed

- [ ] Make a final integration commit if the work spans multiple partial commits and needs one last doc/test update.

Run:

```bash
git status --short
git log --oneline -n 5
```

Expected:

- working tree is understood
- recent commits tell a readable implementation story
