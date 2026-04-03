# TiCDC Dashboard As Code

Python is the source of truth for the TiCDC Grafana dashboard. The checked-in
JSON files remain in the repository, but they are generated artifacts rather
than authoring inputs.

## What To Edit

- Author-facing PromQL helpers: `metrics/queries.py`
- Mutable authoring builders: `metrics/builders.py`
- Dashboard assembly: `metrics/dashboard.py`
- Row definitions, one file per row: `metrics/rows/*.py`
- Templating: `metrics/templating.py`
- Annotations: `metrics/annotations.py`
- Dashboard metadata: `metrics/dashboard_meta.py`

Do not manually edit:

- `metrics/grafana/ticdc_new_arch.json`
- `metrics/grafana/ticdc_new_arch.json.sha256`
- `metrics/grafana/ticdc_new_arch_next_gen.json`
- `metrics/grafana/ticdc_new_arch_next_gen.json.sha256`
- `metrics/grafana/ticdc_new_arch_with_keyspace_name.json`
- `metrics/grafana/ticdc_new_arch_with_keyspace_name.json.sha256`
- `metrics/grafana/panel_ids.json`

Those files are regenerated from Python.

`metrics/grafana/panel_ids.json` is machine-maintained. It preserves Grafana
panel IDs across panel insertion, deletion, and reordering. Do not edit it by
hand.

## Quick Start

For most dashboard changes, only three places matter:

- `metrics/rows/*.py`: one file per row
- `metrics/dashboard.py`: row order
- `metrics/builders.py`: shared authoring helpers

The shortest path for a newcomer is:

1. Find the target row file under `metrics/rows/`.
2. Create or edit one panel in a local variable.
3. Add one or more queries to that panel.
4. Add the panel back to the row.
5. Regenerate the dashboard JSON.

Typical edit loop:

```bash
uv run python metrics/generate_dashboards.py
uv run python metrics/check_dashboards.py
uv run python -m unittest discover -s metrics/tests -p 'test_*.py' -v
```

## Python Scope

The Python workflow documented here currently covers only the metrics dashboard
tooling:

- source modules under `metrics/`
- dashboard generation and validation entry points under `metrics/`
- dashboard unit tests under `metrics/tests/`

It does not manage other Python code in this repository, especially the
integration-test helpers under `tests/integration_tests/`.

For this metrics tooling, the supported runtime is Python 3.12 or newer. The
repository pins `3.12` in `.python-version`, and the recommended environment
manager is `uv`.

Helpful `make` shortcuts for this workflow:

- `make metrics-python-sync`
- `make metrics-python-typecheck`
- `make metrics-python-generate`
- `make metrics-python-check`
- `make metrics-python-test`

## Authoring Workflow

1. Install `uv` and create the local tool environment:

```bash
uv sync --group dev
```

2. Edit the Python source files under `metrics/`.
3. Regenerate dashboard artifacts:

```bash
uv run python metrics/generate_dashboards.py
```

4. Validate the generated artifacts and lint the Python source:

```bash
uv run ty check
uv run ruff format --check metrics
uv run ruff check metrics
uv run python metrics/check_dashboards.py
./scripts/check-ticdc-dashboard.sh
```

5. Run the focused Python test suite:

```bash
uv run python -m unittest discover -s metrics/tests -p 'test_*.py' -v
```

6. Before finishing a change, run:

```bash
make check
```

## Agent Workflow

When a TiCDC Prometheus metric changes, the recommended workflow is:

1. Change the business metric in the TiCDC code.
2. Ask an agent to sync the dashboard from the current code diff.
3. Let the agent modify Python source under `metrics/`, not generated JSON.
4. Let the agent regenerate dashboards and run validation.
5. Review the business meaning of the panel change, not the raw JSON.

The agent should be treated as a dashboard sync operator, not as a blind
dashboard generator.

That means:

- the source of truth for metric semantics is still the TiCDC business code
- the source of truth for dashboard authoring is still the Python code under
  `metrics/`
- generated JSON stays as an artifact

In most cases, the human should only need to tell the agent:

- which subsystem or row the metric belongs to
- whether it should become a graph, table, or heatmap
- whether it should extend an existing panel or create a new one

The human should not need to manually:

- write Grafana JSON
- assign panel IDs
- update checksums
- hand-author repetitive PromQL boilerplate

## Agent Prompt Template

Use this prompt when syncing dashboard changes after a metric diff:

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

When the diff alone is not enough, append one short human hint, for example:

```text
This metric belongs to the Scheduler row.
It is a histogram and should be shown as p99 plus avg.
Do not create a new row.
```

Use one small hint only when needed. Avoid turning dashboard changes into a
manual specification exercise.

## Builder API

The recommended editing surface is:

- `dashboard(...)`
- `row(...)`
- `graph(...)`, `heatmap(...)`, `table(...)`
- `dashboard.add_row(...)`
- `row.add_panel(...)`, `row.add_panels(...)`
- `row.add_half_panel(...)`
- `panel.add_query(...)`
- `panel.add_auto_query(...)`
- `panel.add_range_query(...)`
- `panel.add_auto_range_query(...)`
- `graph(...).add_histogram(...)`
- `table(...).add_label_query(...)`

These methods return `self`, so chaining is supported, but the preferred style
is still explicit sequential construction with named local variables.

Use them with one mental model only:

- A dashboard contains rows.
- A row contains panels.
- A panel contains queries.

The renderer in `metrics/dsl/render.py` is the only layer that should know the
final Grafana JSON structure.

Treat `metrics/dsl/` as internal implementation. Most dashboard changes should
not need edits there.

## Stable Panel IDs

Existing panel IDs are preserved by stable authoring identities:

- row identity: inferred from `build_xxx_row`
- panel identity: inferred from the local variable name assigned to
  `graph(...)`, `heatmap(...)`, or `table(...)`

This means:

- changing a visible row title does not need to change panel IDs
- changing a visible panel title does not need to change panel IDs
- inserting a new panel only allocates a new larger ID
- deleting a panel does not renumber any remaining panel

Use explicit `key=` only when you intentionally want to preserve identity across
an authoring rename, for example when renaming a local panel variable.

For new panels, choose the local variable name carefully. It should be plain
English and stable, because it becomes the default checked-in panel identity.

## Recommended Pattern

When adding a panel, follow this order:

1. Find or create the target row builder.
2. Create a panel builder in a local variable.
3. Add one or more queries to the panel.
4. Add the panel to the row.
5. Return the built row from the row module.

For dashboard assembly:

1. Create the dashboard builder.
2. Add rows in display order.
3. Build once at the end.

## Minimal Example

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

Dashboard assembly should stay equally direct:

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

Useful query shortcuts:

```python
latency = graph("Flush Duration", unit="s", min="0").add_histogram(
    "ticdc_sink_cloud_storage_flush_duration_seconds",
    by_labels=["namespace", "changefeed", "instance"],
    scope="changefeed",
)

errors = table("Changefeed Error Details").add_label_query(
    'max by (namespace, changefeed, state, code, message) (...)',
    columns=["namespace", "changefeed", "state", "code", "message"],
)
```

`add_auto_query(...)` means: omit Grafana's target `format` field and keep the
current dashboard-compatible behavior.

`add_range_query(...)` means: force a Prometheus range query without making the
row author write `instant=False`.

`add_label_query(...)` means: build a table from labels without making the row
author write Grafana table transformations by hand.

## Prometheus Metric Shapes

TiCDC dashboards only need to deal with three Prometheus metric families:

- Counter: usually render with `expr_sum_rate(...)`, `expr_increase(...)`, or a
  raw `rate(...)` expression when needed.
- Gauge: usually render with `expr_sum(...)`, `expr_avg(...)`, `expr_max(...)`,
  or `expr_simple(...)`.
- Histogram: usually render with `expr_histogram_quantile(...)`,
  `expr_histogram_avg(...)`, or a heatmap panel over the `_bucket` series.

The goal is not to create a new helper for every business concept. The goal is
to keep authoring down to:

- panel title
- metric name
- the small amount of aggregation or selector intent that the panel actually
  needs

## Design Rule

Push boilerplate inward, keep intent outward.

Good abstractions remove repeated Grafana mechanics such as:

- target format defaults
- instant vs range query wiring
- default ref assignment
- immutable spec rendering

Bad abstractions hide the monitoring intent itself, such as:

- row-specific helper functions that only wrap one metric
- opaque scope objects that invent a second DSL
- helper layers that force authors to think about JSON structure again

Avoid writing long PromQL as one unreadable line unless it is genuinely simple.

## Repository Conventions

- Use one Python module per row under `metrics/rows/`.
- Keep one exported builder per file, for example `build_scheduler_row()`.
- Keep dashboard row order in `metrics/dashboard.py`.
- Build panels step by step with local variables. Do not compress an entire row
  into one chained expression.
- Prefer `expr_*` helpers from `metrics/queries.py` when they make the metric
  intent clearer.
- Use raw PromQL only as an escape hatch.
- Do not add business-specific wrapper APIs such as
  `changefeed_metric_graph(...)` or scope objects such as `changefeed.sum(...)`.

## Code Organization Guidelines

To keep this codebase friendly for future editors:

- Keep row modules declarative. Avoid helper abstractions that hide what panel
  is being built.
- Keep helpers in `metrics/dsl/` generic. Keep TiCDC-specific authoring helpers
  in flat top-level modules under `metrics/`.
- Prefer clear names over clever names.
- Use type hints consistently.
- Keep functions small enough that a reader can understand one panel block at a
  glance.
- Keep comments rare, but use them when a layout trick or metric choice would be
  surprising without context.

## Verification Checklist

For any dashboard change, use this checklist:

```bash
uv run python metrics/generate_dashboards.py
uv run ty check
uv run ruff format --check metrics
uv run ruff check metrics
uv run python metrics/check_dashboards.py
uv run python -m unittest discover -s metrics/tests -p 'test_*.py' -v
make check
```

## Current Tests

The Python tests under `metrics/tests/` protect three layers:

- `test_ticdc_dsl.py`: primitive DSL behavior
- `test_ticdc_dashboard_rows.py`: semantic row-by-row comparison against the
  checked-in base dashboard JSON
- `test_ticdc_dashboard_tools.py`: generator, checksum, and workflow contracts

If you change the DSL surface, update these tests first or at the same time.

## Language Server

This repository keeps `ty` configuration in `pyproject.toml` for the dashboard
tooling only:

- `metrics/`

That gives editors a project-level Python language server and type-checking
baseline without pulling unrelated repository Python into the same analysis
scope. Install the editor integration on your machine, then let it use the
project `.venv` created by `uv sync --group dev`.
