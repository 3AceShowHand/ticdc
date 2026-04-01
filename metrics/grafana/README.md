# TiCDC Dashboard As Code

Python is the source of truth for the TiCDC Grafana dashboard. The checked-in
JSON files remain in the repository, but they are generated artifacts rather
than authoring inputs.

## What To Edit

- Author-facing PromQL and panel helpers: `metrics/grafana/common.py`
- Builder layer used by row authors: `metrics/grafana/ticdc_new_arch/builders.py`
- Dashboard assembly: `metrics/grafana/ticdc_new_arch/dashboard.py`
- Row definitions, one file per row: `metrics/grafana/ticdc_new_arch/rows/*.py`
- Templating: `metrics/grafana/ticdc_new_arch/templating.py`
- Internal immutable spec and renderer: `metrics/dsl/`
- Python style baseline: `pyproject.toml`

Do not manually edit:

- `metrics/grafana/ticdc_new_arch.json`
- `metrics/grafana/ticdc_new_arch.json.sha256`
- `metrics/nextgengrafana/*.json`
- `metrics/nextgengrafana/*.sha256`

Those files are regenerated from Python.

## Authoring Workflow

1. Edit the Python source files under `metrics/grafana/` and `metrics/dsl/`.
2. Regenerate dashboard artifacts:

```bash
python3 scripts/gen-ticdc-dashboards
```

3. Validate the generated artifacts:

```bash
python3 scripts/check-ticdc-dashboard.py
./scripts/check-ticdc-dashboard.sh
```

4. Run the focused Python test suite:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

5. Before finishing a change, run:

```bash
make check
```

## Mental Model

The authoring model is intentionally small and explicit:

- A dashboard contains rows.
- A row contains panels.
- A panel contains queries.
- Queries are PromQL expressions plus Grafana target options.

The repository uses a mutable builder layer for authoring and an immutable spec
layer for rendering:

- Use `metrics.grafana.ticdc_new_arch.builders` when writing dashboard code.
- Use `metrics.dsl` only when extending the spec model or renderer.

The renderer in `metrics/dsl/render.py` is the only layer that should know the
final Grafana JSON structure.

## Repository Conventions

- Use one Python module per row under `metrics/grafana/ticdc_new_arch/rows/`.
- Keep one exported builder per file, for example `build_scheduler_row()`.
- Assemble the dashboard in `dashboard.py` with ordered `add_row(...)` calls.
- Build panels step by step with local variables. Do not compress everything
  into one chained expression.
- Prefer `expr_*` helpers from `common.py` over raw PromQL when they keep the
  intent clearer.
- Use raw PromQL only as an escape hatch for cases that are genuinely clearer
  in plain PromQL.
- Do not add business-specific wrapper APIs such as
  `changefeed_metric_graph(...)` or scope objects such as `changefeed.sum(...)`.
  Those hide intent and make maintenance harder.

## Builder API

The recommended editing surface is:

- `dashboard(...)`
- `row(...)`
- `graph(...)`, `timeseries(...)`, `heatmap(...)`, `table(...)`
- `dashboard.add_row(...)`
- `row.add_graph(...)`, `row.add_timeseries(...)`, `row.add_heatmap(...)`,
  `row.add_table(...)`
- `panel.add_query(...)`

These methods return `self`, so chaining is supported, but the preferred style
is still explicit sequential construction with named local variables.

## Recommended Authoring Pattern

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
from metrics.dsl.specs import RowSpec
from metrics.grafana.common import expr_sum_rate
from metrics.grafana.ticdc_new_arch.builders import graph, row


def build_sink_row() -> RowSpec:
    row_builder = row("Sink", default_height=6, default_span=12)

    batch_rows = graph(
        "Batch Rows",
        unit="ops",
        min="0",
        description="Rows written by the sink per second.",
    )
    batch_rows.add_query(
        expr_sum_rate(
            "ticdc_sink_batch_row_count_count",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        legend_format="{{namespace}}-{{changefeed}}-{{instance}}",
    )

    row_builder.add_graph(batch_rows)
    return row_builder.build()
```

Dashboard assembly should stay equally direct:

```python
from ticdc_new_arch.builders import dashboard as dashboard_builder


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
- default ref assignment
- default row panel height and span
- immutable spec rendering

Bad abstractions hide the monitoring intent itself, such as:

- row-specific helper functions that only wrap one metric
- opaque scope objects that invent a second DSL
- helper layers that force authors to think about JSON structure again

Avoid writing long PromQL as one unreadable line unless it is genuinely simple.

## Code Organization Guidelines

To keep this codebase friendly for future editors:

- Keep row modules declarative. Avoid helper abstractions that hide what panel
  is being built.
- Keep helpers in `metrics/dsl/` generic. If a helper only makes sense for one
  row, keep it in that row module.
- Prefer clear names over clever names.
- Use type hints consistently.
- Keep functions small enough that a reader can understand one panel block at a
  glance.
- Keep comments rare, but use them when a layout trick or metric choice would be
  surprising without context.

## Verification Checklist

For any dashboard change, use this checklist:

```bash
python3 scripts/gen-ticdc-dashboards
python3 scripts/check-ticdc-dashboard.py
python3 -m unittest discover -s scripts -p 'test_*.py' -v
make check
```

## Current Tests

The Python tests under `scripts/tests/` protect three layers:

- `test_ticdc_dsl.py`: primitive DSL behavior
- `test_ticdc_dashboard_rows.py`: semantic row-by-row comparison against the
  checked-in base dashboard JSON
- `test_ticdc_dashboard_tools.py`: generator, checksum, and workflow contracts

If you change the DSL surface, update these tests first or at the same time.
