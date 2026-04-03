# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Top-level dashboard assembly.

Most dashboard edits stop in `metrics/rows/*.py`. This file is only responsible
for ordering rows and wiring dashboard-wide metadata, templating, and
annotations.
"""

from __future__ import annotations

from pathlib import Path

from metrics.annotations import build_annotations
from metrics.builders import dashboard as dashboard_builder
from metrics.dashboard_meta import (
    BASE_DASHBOARD_TITLE,
    BASE_DASHBOARD_UID,
    DASHBOARD_VERSION,
)
from metrics.dsl.render import PanelIdResolver, render_dashboard
from metrics.dsl.specs import DashboardSpec
from metrics.panel_ids import (
    PANEL_ID_REGISTRY_FILE,
    build_panel_id_resolver,
    load_panel_id_registry,
)
from metrics.rows import (
    build_active_active_row,
    build_changefeed_row,
    build_coordinator_row,
    build_dataflow_row,
    build_ddl_row,
    build_dispatcher_row,
    build_dynamic_stream_row,
    build_event_service_row,
    build_event_store_row,
    build_lag_analyze_row,
    build_lag_summary_row,
    build_log_puller_row,
    build_maintainer_row,
    build_message_center_row,
    build_pulsar_sink_row,
    build_redo_row,
    build_runtime_row,
    build_scheduler_row,
    build_schema_store_row,
    build_server_row,
    build_sink_cloud_storage_row,
    build_sink_general_row,
    build_sink_mq_row,
    build_sink_transaction_row,
    build_summary_row,
    build_tikv_row,
)
from metrics.templating import build_templating

# Single source of row display order in the rendered dashboard.
ROW_BUILDERS = [
    build_summary_row,
    build_lag_summary_row,
    build_dataflow_row,
    build_server_row,
    build_changefeed_row,
    build_lag_analyze_row,
    build_coordinator_row,
    build_maintainer_row,
    build_log_puller_row,
    build_event_store_row,
    build_schema_store_row,
    build_event_service_row,
    build_message_center_row,
    build_dispatcher_row,
    build_dynamic_stream_row,
    build_sink_general_row,
    build_sink_transaction_row,
    build_sink_mq_row,
    build_sink_cloud_storage_row,
    build_scheduler_row,
    build_tikv_row,
    build_active_active_row,
    build_redo_row,
    build_runtime_row,
    build_ddl_row,
    build_pulsar_sink_row,
]


def build_dashboard_spec() -> DashboardSpec:
    """Build the immutable dashboard spec before rendering JSON."""

    spec = dashboard_builder(
        title=BASE_DASHBOARD_TITLE,
        uid=BASE_DASHBOARD_UID,
        version=DASHBOARD_VERSION,
        variables=build_templating(),
        annotations=build_annotations(),
    )
    for build_row in ROW_BUILDERS:
        spec.add_row(build_row())
    return spec.build()


def build_dashboard() -> dict[str, object]:
    """Render the final Grafana JSON payload used by generation scripts."""

    return render_dashboard(
        build_dashboard_spec(),
        panel_id_resolver=load_stable_panel_id_resolver(),
    )


def build_dashboard_with_panel_ids(
    spec: DashboardSpec,
    panel_id_resolver: PanelIdResolver | None,
) -> dict[str, object]:
    """Render one dashboard spec with an explicit panel ID resolver."""

    return render_dashboard(spec, panel_id_resolver=panel_id_resolver)


def load_stable_panel_id_resolver(
    repo_root: Path | None = None,
) -> PanelIdResolver | None:
    """Load the checked-in stable panel ID registry when it exists."""

    resolved_root = Path(__file__).resolve().parents[1] if repo_root is None else repo_root
    registry_path = resolved_root / PANEL_ID_REGISTRY_FILE
    if not registry_path.exists():
        return None
    return build_panel_id_resolver(load_panel_id_registry(registry_path))
