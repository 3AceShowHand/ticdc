# TiCDC Grafana Dashboard Compatibility Follow-up

- Status: Proposed (approved constraints captured)
- Date: 2026-04-01
- Owner Team: TiCDC Observability

## Background / Context

TiCDC dashboard authoring has been refactored from hand-written dashboard assembly to a
new additive builder model. The current implementation centers on:

- `metrics/dashboard.py`
- `metrics/builders.py`
- `metrics/rows/*.py`
- `metrics/queries.py`
- `metrics/annotations.py`
- `metrics/compatibility.py`
- `metrics/dsl/render.py`
- `scripts/gen-ticdc-dashboards`
- `scripts/check-ticdc-dashboard.py`

The new model improves maintainability for dashboard authors:

- one row per file
- explicit `dashboard.add_row(...)`
- explicit `row.add_graph(...)`, `row.add_table(...)`, `row.add_timeseries(...)`
- explicit `panel.add_query(...)`

The repository-level generation and validation flow is currently healthy:

- dashboard generation is deterministic
- checksum validation passes
- row overlap and duplicate panel ID checks pass
- Python unit tests for the DSL and dashboard tooling pass

However, the branch-to-upstream dashboard diff is still large. That diff is caused by a mix
of source-code reorganization, JSON canonicalization, panel ID rewriting, and a small number
of real compatibility regressions. This follow-up document records which changes are
acceptable and which must be fixed before the new authoring model can be considered
compatible with the existing TiCDC dashboard.

## Problem Statement

The new TiCDC Grafana authoring model should reduce dashboard maintenance cost without
creating unnecessary operator-facing changes.

The current branch still has three classes of compatibility risk:

1. Layout changes that move existing panels to new rows.
2. Behavioral changes that remove dashboard features such as annotations.
3. Artifact instability caused by sequentially regenerated panel IDs.

At the same time, not every large JSON diff is a compatibility problem. Many changes are
caused by the new renderer emitting a smaller and more canonical dashboard representation.
The follow-up work must separate these cases and only preserve the parts that matter to
operators and downstream integrations.

## Goals

- Keep the new additive authoring model and one-row-per-file structure.
- Preserve the existing operator-facing dashboard layout.
- Keep `Changefeed Error Details` in the `Changefeed` row.
- Restore existing operational annotations unless there is an explicit reason to remove them.
- Choose a stable and compatibility-friendly panel ID strategy.
- Treat query text formatting and JSON canonicalization separately from semantic changes.
- Add automated checks so future dashboard refactors cannot silently break compatibility.

## Non-Goals

- Reverting the new builder-based authoring model.
- Preserving every historical Grafana-export noise field if it is not functionally needed.
- Freezing the exact byte-for-byte JSON representation produced by Grafana exports.
- Blocking intentional metric query bug fixes when those fixes are explicitly reviewed.

## Current State (As-Is)

### Authoring and Rendering Flow

The current dashboard build path is:

```text
Row modules under rows/*.py
        |
        v
metrics/dashboard.py
        |
        v
metrics/dsl/render.py
        |
        v
metrics/grafana/ticdc_new_arch.json
metrics/grafana/ticdc_new_arch_next_gen.json
metrics/grafana/ticdc_new_arch_with_keyspace_name.json
```

Relevant code pointers:

- Dashboard assembly: `metrics/dashboard.py: build_dashboard_spec`
- Row builders: `metrics/builders.py: DashboardBuilder`, `RowBuilder`
- Renderer: `metrics/dsl/render.py: render_dashboard`, `render_row`, `render_panel`
- Dashboard validation: `scripts/check-ticdc-dashboard.py`
- Dashboard tool tests: `scripts/tests/test_ticdc_dashboard_tools.py`

### Originally Reported Regressions

#### 1. Annotations were removed

This has been fixed in the flat layout by restoring the historical annotations in:

- `metrics/annotations.py: build_annotations`

Upstream dashboard artifacts contained the following annotations:

- built-in dashboard annotation
- `Latency spike`
- `Server down`
- `All TiCDC alerts`
- `Resolved region drop`

The `spike_threshold` template variable remains in:

- `metrics/templating.py: build_templating`

This means the branch currently retains configuration UI for an annotation-driven workflow
that no longer exists.

#### 2. `Changefeed Error Details` moved to the wrong row

This has been fixed in the flat layout. The panel now lives in:

- `metrics/rows/changefeed.py: build_changefeed_row`

The upstream dashboard placed `Changefeed Error Details` under the `Changefeed` row. The
panel query itself is unchanged, but its position in the dashboard has changed. This is a
layout compatibility regression and is not acceptable.

#### 3. Panel IDs were regenerated sequentially

This has been fixed by routing dashboard rendering through the checked-in compatibility map:

- `metrics/compatibility.py: EXPECTED_PANEL_IDS`, `resolve_panel_id`
- `metrics/dsl/render.py: render_dashboard`

The historical dashboard artifacts used stable, sparse panel IDs inherited from previous
Grafana exports. Replacing them with position-based IDs has two problems:

- any external `viewPanel=<id>` links can break
- reordering rows or panels renumbers unrelated panels

The top-level dashboard `id` should remain environment-specific and should not be treated as
a compatibility contract. Nested panel IDs are the compatibility-sensitive part.

### Confirmed Current Non-Issues

The following categories explain much of the large JSON diff but are not automatically
compatibility bugs:

#### 1. Source-code reorganization

The refactor from grouped row modules to one-row-per-file intentionally creates a large code
diff. This is expected and desirable if the generated dashboard remains compatible.

#### 2. Canonical JSON rendering

The new renderer omits many Grafana-export fields that are not authoring-friendly and are not
required for repository-level generation:

- `__requires`
- `gnetId`
- top-level `iteration`
- top-level `links`
- top-level `style`
- top-level `tags`
- top-level `timezone`
- top-level `version`
- many panel-level export defaults such as `pluginVersion`, `aliasColors`, `fillGradient`,
  `seriesOverrides`, `renderer`, and similar fields

These omissions are acceptable if the imported dashboard behavior remains unchanged on the
target Grafana version.

#### 3. PromQL text normalization

Many query diffs are caused by whitespace cleanup, label ordering, `by (...)` ordering, or
equivalent literal formatting such as `0.90` becoming `0.9`. These are not compatibility
issues by themselves.

## Query-Level Semantic Changes

Not all query changes are formatting-only. The current branch includes both valid bug fixes
and query changes that require explicit review.

### Intentional Bug Fixes That Should Be Preserved

These changes appear correct and should be treated as reviewed fixes, not regressions:

- `metrics/rows/sink_mq.py`
  - `Claim Check Send Message Duration Percentile`
  - average query now uses `_sum / _count` with proper TiCDC scope instead of the previous
    broken expression
- `metrics/rows/pulsar_sink.py`
  - `Pulsar Client Producer Latency`
  - average query now uses `_sum / _count` instead of dividing bucket rates

### Query Changes That Need Explicit Audit

These changes may be correct, but they must not be silently accepted:

- Dynamic stream average panels removed `le` from the aggregation labels
  - likely a correctness fix for histogram average computation
  - still a semantic change and should be reviewed as such
- Matcher rewrites such as exact-match versus regex-match normalization
  - usually harmless, but must be reviewed when template variables support `All`
- Any query changes that affect panel scope, grouping dimensions, or legend shape

The compatibility policy should be:

- formatting-only changes are accepted
- query bug fixes are accepted only after explicit review
- any operator-visible grouping or scope change must be called out in the change summary and
  covered by tests

## Required Compatibility Contract

### 1. Layout Invariants

The new authoring model must preserve the existing dashboard layout unless a layout change is
explicitly proposed and reviewed.

Required invariants:

- row order remains unchanged
- row titles remain unchanged
- panel order within a row remains unchanged unless explicitly reviewed
- `Changefeed Error Details` remains in the `Changefeed` row
- panels must not move between rows as a side effect of source-code refactoring

### 2. Annotation Invariants

The branch must restore the historical annotation behavior unless there is a separate design
decision to remove it.

Required invariants:

- built-in dashboard annotation remains present if needed by Grafana behavior
- `Latency spike` annotation is restored
- `Server down` annotation is restored
- `All TiCDC alerts` annotation is restored
- `Resolved region drop` annotation is restored
- `spike_threshold` remains only if the dashboard still uses it

### 3. Panel ID Strategy

The panel ID strategy must prioritize compatibility over implementation convenience.

Required rules:

- nested panel IDs must be stable across re-generation
- nested panel IDs must not depend on row order or panel insertion order
- existing historical panel IDs should be reused when the corresponding panel still exists
- new panels should receive a new stable ID once and keep it thereafter
- dashboard top-level `id` should remain omitted or environment-specific

Recommended implementation direction:

- maintain a checked-in compatibility map keyed by stable logical identity
- default to `(row_title, panel_title)` and use an explicit panel `key` when titles are duplicated
- seed the map from the historical upstream dashboard artifacts
- validate uniqueness and stability in tests

The current flat implementation already needs explicit keys for duplicate titles in rows such
as `Event Service`, `Event Store`, `TiKV`, and `Pulsar Sink`.

### 4. Canonicalization Boundary

Canonical rendering is allowed, but only outside the compatibility contract.

Allowed to change:

- whitespace
- PromQL formatting
- non-functional export defaults
- top-level Grafana export metadata that is environment-specific or redundant

Not allowed to change silently:

- row placement
- panel order
- panel IDs
- operational annotations
- metric scope and grouping semantics

## Proposed Follow-Up Work

### Work Item 1: Restore dashboard layout parity

- move `Changefeed Error Details` out of `metrics/rows/summary.py`
- add it back to `metrics/rows/changefeed.py`
- preserve the original row and panel ordering

### Work Item 2: Restore annotation parity

- re-encode the historical annotation definitions in `metrics/annotations.py`
- validate that `spike_threshold` is still wired to the latency-spike annotation
- remove dead template variables if annotation behavior intentionally changes later

### Work Item 3: Introduce stable panel IDs

- add a checked-in panel ID registry for TiCDC dashboard panels
- update `metrics/dsl/render.py` so nested panel IDs come from that registry rather than sequential allocation
- keep row container IDs out of scope unless Grafana import behavior proves they matter

### Work Item 4: Add compatibility-focused tests

Add tests that verify compatibility-sensitive behavior instead of only structural validity.

Required test coverage:

- `Changefeed Error Details` is located in the `Changefeed` row
- row titles and row order match the reference layout
- annotation names and count match the approved set
- stable panel IDs match the checked-in compatibility map
- panel IDs do not change when unrelated rows or panels are reordered in source
- reviewed semantic query changes are explicitly asserted

### Work Item 5: Keep accepted bug fixes explicit

When query semantics intentionally change to fix existing bugs:

- keep the fix
- record it in the change summary
- add a targeted test for the corrected expression

This prevents future refactors from reintroducing the old broken query while also making it
clear that the change was deliberate.

## Testing Strategy

The compatibility follow-up should be considered complete only when all of the following pass:

- `python3 scripts/gen-ticdc-dashboards`
- `python3 scripts/check-ticdc-dashboard.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`

Additional compatibility checks should be added to the unit test suite:

- layout parity tests
- annotation parity tests
- stable panel ID tests
- selected query regression tests for reviewed bug fixes

If a target Grafana environment is available, a manual import smoke test is recommended to
confirm that canonicalized JSON still behaves correctly on the expected Grafana version.

## Rollout and Risk Assessment

### High Risk

- panel ID renumbering that breaks external dashboard links
- lost annotations that remove operational debugging signals
- row moves that break established operator workflows

### Medium Risk

- silently accepted query semantic changes without test coverage
- leaving stale template variables that no longer affect dashboard behavior

### Low Risk

- removal of export-only Grafana metadata
- PromQL whitespace and formatting normalization

## Alternatives Considered

### Alternative 1: Accept sequential panel IDs

Rejected.

This is simple to implement but breaks stability whenever panels are inserted, deleted, or
reordered. It also risks breaking external `viewPanel` links.

### Alternative 2: Preserve full Grafana-export JSON shape

Rejected.

This would reduce diff size anxiety but would reintroduce a large amount of export noise into
the source of truth. The authoring model should stay concise and maintainable.

### Alternative 3: Accept row moves if the panel query is unchanged

Rejected.

Dashboard layout is part of the operator-facing contract. Query parity alone is not enough.

## References

- `metrics/dashboard.py`
- `metrics/builders.py`
- `metrics/rows/changefeed.py`
- `metrics/rows/summary.py`
- `metrics/rows/sink_mq.py`
- `metrics/rows/pulsar_sink.py`
- `metrics/annotations.py`
- `metrics/templating.py`
- `metrics/compatibility.py`
- `metrics/dsl/render.py`
- `scripts/gen-ticdc-dashboards`
- `scripts/check-ticdc-dashboard.py`
- `scripts/tests/test_ticdc_dashboard_tools.py`
- `scripts/tests/test_ticdc_dashboard_rows.py`
- historical reference dashboard artifact: `upstream/master:metrics/grafana/ticdc_new_arch.json`
