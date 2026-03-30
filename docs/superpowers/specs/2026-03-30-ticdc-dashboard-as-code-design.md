# TiCDC Dashboard-as-Code Design

## Summary

This document defines a compatible migration of TiCDC Grafana dashboards from
hand-maintained JSON files to a Python source model similar to TiKV's dashboard
generation flow.

The goal is to make Python the source of truth for the base TiCDC dashboard
while preserving the current dashboard behavior, next-gen derived dashboards,
Grafana import semantics, and CI entry points.

## Background

Today TiCDC stores three dashboard JSON files in the repository:

- `metrics/grafana/ticdc_new_arch.json`
- `metrics/nextgengrafana/ticdc_new_arch_next_gen.json`
- `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json`

The current workflow is:

1. `metrics/grafana/ticdc_new_arch.json` is edited directly.
2. `scripts/generate-next-gen-metrics.sh` derives the two next-gen dashboards
   from the base JSON using `sed` and `jq`.
3. `scripts/check-ticdc-dashboard.sh` checks the base dashboard for duplicate
   panel IDs and layout overlap.
4. `make check` runs `check-ticdc-dashboard` and `generate-next-gen-grafana`.

This has three problems:

- The base dashboard JSON is difficult to read and review.
- Generated artifacts are not explicitly tied back to a higher-level source.
- CI can detect some structural problems, but it cannot tell whether JSON was
  updated manually instead of being regenerated from a canonical source.

TiKV already solves this class of problem with:

- a Python DSL layer (`common.py`)
- a Python dashboard source (`*.dashboard.py`)
- a generator script that produces JSON and checksum files
- a validation script that rejects manual JSON edits

TiCDC will adopt the same workflow shape, but not necessarily the same Python
implementation library. In this phase the priority is compatibility and
deterministic output, not matching TiKV's exact helper stack.

## Goals

- Make the base TiCDC dashboard generated from Python source.
- Keep the current dashboard content and behavior effectively unchanged.
- Preserve the existing next-gen derived dashboard behavior.
- Add generated artifact checksums so CI can reject manual JSON edits.
- Keep `make check` and existing script entry points stable for developers.
- Allow small structural cleanups during migration:
  - fix duplicate panel IDs
  - fix layout overlap
  - extract obvious repeated helpers

## Non-Goals

- No dashboard information architecture redesign.
- No row or panel regrouping beyond minimal layout fixes.
- No renaming of existing templating variables such as `namespace`,
  `changefeed`, `ticdc_instance`, or `tikv_instance`.
- No rewrite of next-gen derivation logic into Python in this phase.
- No changes to the semantic meaning of existing PromQL queries unless
  required to preserve current behavior after generation.

## Constraints

The migration must preserve the following behavior:

- Grafana import compatibility, including `__inputs` and datasource name
  `DS_TEST-CLUSTER`.
- The current title, UID, templating variable names, annotation behavior, row
  order, panel titles, and query semantics of the base dashboard.
- The current string-based transformation points used by
  `scripts/generate-next-gen-metrics.sh`, especially replacements involving
  `namespace` and `tidb_cluster`.
- Existing CI and developer entry points in the Makefile.

### Invariant Checklist

Unless changed atomically with the next-gen derivation script and its tests, the
generated base dashboard must preserve these concrete invariants:

- base dashboard path: `metrics/grafana/ticdc_new_arch.json`
- datasource input name: `DS_TEST-CLUSTER`
- current base title: `test-cluster-TiCDC-New-Arch`
- current base UID: `YiGL8hBZ0aac`
- current templating variables:
  - `k8s_cluster`
  - `tidb_cluster`
  - `namespace`
  - `changefeed`
  - `ticdc_instance`
  - `tikv_instance`
  - `spike_threshold`
  - `runtime_instance`
- the literal token `namespace` must still exist in the base JSON where the
  next-gen derivation expects to rewrite it to `keyspace_name`
- the literal token `tidb_cluster` must still exist in the base JSON where the
  sharedscope derivation expects to rewrite it to `sharedpool_id`
- the current userscope UID replacement in
  `scripts/generate-next-gen-metrics.sh` must still have a valid source anchor
  unless the script is updated atomically
- the current userscope title rewrite in
  `scripts/generate-next-gen-metrics.sh` must either continue to be harmless or
  be updated atomically if the script is simplified during implementation

## Proposed Architecture

### Source of Truth

Introduce Python dashboard source files under `metrics/grafana/`:

- `metrics/grafana/common.py`
- `metrics/grafana/ticdc_new_arch.dashboard.py`

`ticdc_new_arch.dashboard.py` becomes the source of truth for the base
dashboard. `metrics/grafana/ticdc_new_arch.json` remains checked in, but only as
generated output.

### Generated Artifacts

The repository continues to check in all three JSON dashboards:

- `metrics/grafana/ticdc_new_arch.json`
- `metrics/nextgengrafana/ticdc_new_arch_next_gen.json`
- `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json`

Add one checksum file per generated JSON:

- `metrics/grafana/ticdc_new_arch.json.sha256`
- `metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256`
- `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256`

### Generator Entry Point

Add a single generation entry point:

- `scripts/gen-ticdc-dashboards`

Responsibilities:

1. Run with `python3` from the host environment; no `pip` downloads are
   required during generation.
2. Require a minimum host interpreter of Python 3.10 so dict insertion order
   and stdlib behavior are well defined across developer and CI environments.
3. Generate `metrics/grafana/ticdc_new_arch.json` from
   `metrics/grafana/ticdc_new_arch.dashboard.py`.
4. Call `scripts/generate-next-gen-metrics.sh` to derive the two next-gen
   dashboards from the generated base JSON.
5. Generate `.sha256` files for all three JSON outputs.

This keeps the current next-gen derivation pipeline intact while moving the
base dashboard to dashboard-as-code.

The generator should be stdlib-only. The dashboard Python source should build
plain Python dictionaries/lists and serialize them with `json.dump`. This keeps
the implementation deterministic and avoids introducing a separate Python
dependency bootstrap problem into `make check`.

Formatting of Python source is out of scope for the generator in this phase.
The generator should not install or invoke formatters. If formatting automation
is needed later, it should be added as a separate developer-facing step.

### Validation Entry Point

Keep the existing shell entry point:

- `scripts/check-ticdc-dashboard.sh`

Expand its responsibilities:

1. Verify all dashboard checksum files.
2. Run structural validation for all generated dashboards:
   - duplicate panel IDs
   - overlapping layout in each container

Retain `scripts/check-ticdc-dashboard.py` as the structural validator, but make
it accept multiple dashboard files instead of only the base JSON.

## File Layout

New files:

- `metrics/grafana/common.py`
- `metrics/grafana/ticdc_new_arch.dashboard.py`
- `metrics/grafana/ticdc_new_arch.json.sha256`
- `metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256`
- `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256`
- `metrics/grafana/README.md`
- `scripts/gen-ticdc-dashboards`

Modified files:

- `scripts/check-ticdc-dashboard.sh`
- `scripts/check-ticdc-dashboard.py`
- `scripts/generate-next-gen-metrics.sh`
- `Makefile`
- `metrics/grafana/ticdc_new_arch.json`
- `metrics/nextgengrafana/ticdc_new_arch_next_gen.json`
- `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json`

## Python Source Model

The Python support layer should be intentionally smaller than TiKV's initial
DSL. It only needs to cover the constructs already used by the TiCDC
dashboard.

Expected helper categories:

- datasource input helper
- template helper
- row/layout helper
- panel helper for graph/time-series/stat/heatmap as needed
- lightweight expression helpers for repeated PromQL fragments

This phase should not attempt to build a full generic framework. The target is
readability and faithful reproduction of the current JSON, not maximal reuse.

## Generation Flow

The intended generation flow is:

```text
metrics/grafana/common.py
metrics/grafana/ticdc_new_arch.dashboard.py
            |
            v
scripts/gen-ticdc-dashboards
            |
            +--> metrics/grafana/ticdc_new_arch.json
            |
            +--> scripts/generate-next-gen-metrics.sh
            |        |
            |        +--> metrics/nextgengrafana/ticdc_new_arch_next_gen.json
            |        +--> metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json
            |
            +--> *.sha256 for all three JSON files
```

JSON serialization should be deterministic:

- preserve explicit key insertion order from the Python source model
- use stable indentation matching repository JSON style
- write a trailing newline at end of file
- do not rely on key sorting to achieve determinism

Checksum files should use the same line format expected by `sha256sum -c`, so
the validation script can verify them directly.

## Validation Flow

The intended validation flow is:

```text
scripts/check-ticdc-dashboard.sh
    |
    +--> verify sha256 for all generated dashboard JSON files
    +--> python structural validation for all generated dashboard JSON files
```

Checksum validation alone is not sufficient to enforce "Python source of truth"
because a developer could edit both a JSON file and its `.sha256` file.
Freshness enforcement must come from regeneration plus repository diff checks.

The Makefile should continue to expose stable targets:

- `make generate-next-gen-grafana`
- `make check-ticdc-dashboard`
- `make check`

Internally:

- `generate-next-gen-grafana` should invoke `scripts/gen-ticdc-dashboards`
  instead of only the current next-gen derivation step.
- `check-ticdc-dashboard` should validate all generated dashboards, not only
  the base one.
- `make check` should continue to regenerate dashboard artifacts and then fail
  if `git diff --exit-code` is non-empty. That regeneration step, not checksum
  comparison alone, is what enforces that Python remains the source of truth.

## Migration Plan

### Phase 1: Introduce Python generation for the base dashboard

- Add `common.py` and `ticdc_new_arch.dashboard.py`.
- Recreate the current base dashboard in Python.
- Regenerate `metrics/grafana/ticdc_new_arch.json`.
- Verify that differences are limited to expected normalization or minimal
  cleanup.

### Phase 2: Fold existing next-gen derivation into the unified generator

- Add `scripts/gen-ticdc-dashboards`.
- Make it generate the base JSON first, then call
  `scripts/generate-next-gen-metrics.sh`.
- Ensure the next-gen outputs remain semantically stable.

### Phase 3: Add artifact integrity checks

- Generate `.sha256` files for all three dashboards.
- Extend `scripts/check-ticdc-dashboard.sh` to verify checksums before
  structural checks.

### Phase 4: Update documentation and CI wiring

- Add `metrics/grafana/README.md`.
- Update Makefile targets to use the new unified generation flow.
- Ensure `make check` fails on:
  - stale generated JSON
  - stale next-gen JSON
  - checksum mismatch
  - duplicate panel IDs
  - layout overlap

## Testing Strategy

Verification for this change should include:

- Run the unified generator on a clean tree and confirm a second run produces no
  diff.
- Run the unified generator and confirm it updates all three dashboards and all
  three checksum files deterministically.
- Run the dashboard checker on all three JSON files.
- Run `make check` and confirm no uncommitted diffs remain after generation.
- Compare key properties of the generated base dashboard against the current
  JSON:
  - title
  - UID
  - datasource input shape and name
  - templating variable names and counts
  - panel count
  - representative PromQL expressions
- Compare the two next-gen outputs before and after migration to confirm that
  differences are limited to acceptable formatting or known cleanup.

## Risks and Mitigations

### Risk: Generated base JSON changes too much

Mitigation:

- Use the current JSON as the semantic baseline.
- Migrate panel-by-panel.
- Accept only minimal cleanup in this phase.

### Risk: Next-gen derivation script stops matching the base JSON

Mitigation:

- Preserve current templating variable names and replacement anchors.
- Keep next-gen derivation logic unchanged in this phase.
- Run the derivation step as part of the unified generator every time.

### Risk: Grafana import semantics change

Mitigation:

- Preserve datasource input shape and datasource variable naming.
- Verify `__inputs` and `__requires` fields in generated JSON.

### Risk: Developers continue editing JSON manually

Mitigation:

- Add README guidance.
- Add checksum validation in CI.
- Keep generated JSON checked in so diffs remain reviewable.

## Alternatives Considered

### Keep JSON as the source of truth

Rejected because it does not provide the same capability as TiKV and does not
improve maintainability meaningfully.

### Rewrite next-gen derivation into Python in the same change

Rejected for this phase because it mixes workflow migration with behavioral
refactoring and creates a much larger review surface.

### Generate only the base dashboard, leave next-gen files unmanaged

Rejected because the request is for a complete workflow and CI solution, not a
partial one.

## Decision

Adopt a compatible dashboard-as-code migration:

- Python becomes the source of truth for the base TiCDC dashboard.
- Existing next-gen dashboard derivation remains in place for now.
- A new unified generator produces all JSON artifacts and checksums.
- CI validates both artifact freshness and dashboard structure.

This delivers the same class of capability as TiKV while keeping migration risk
appropriate for a compatibility-first phase.
