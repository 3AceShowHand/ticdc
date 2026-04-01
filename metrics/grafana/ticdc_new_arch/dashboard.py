# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl import render_dashboard

from common import BASE_DASHBOARD_TITLE, BASE_DASHBOARD_UID, validate_dashboard_identity
from ticdc_new_arch.annotations import build_annotations
from ticdc_new_arch.builders import dashboard as dashboard_builder
from ticdc_new_arch.rows import (
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
    build_schema_store_row,
    build_scheduler_row,
    build_server_row,
    build_sink_cloud_storage_row,
    build_sink_general_row,
    build_sink_mq_row,
    build_sink_transaction_row,
    build_summary_row,
    build_tikv_row,
)
from ticdc_new_arch.templating import build_templating


def build_dashboard_spec():
    spec = dashboard_builder(
        title=BASE_DASHBOARD_TITLE,
        uid=BASE_DASHBOARD_UID,
        variables=build_templating(),
        annotations=build_annotations(),
    )
    (
        spec.add_row(build_summary_row())
        .add_row(build_lag_summary_row())
        .add_row(build_dataflow_row())
        .add_row(build_server_row())
        .add_row(build_changefeed_row())
        .add_row(build_lag_analyze_row())
        .add_row(build_coordinator_row())
        .add_row(build_maintainer_row())
        .add_row(build_log_puller_row())
        .add_row(build_event_store_row())
        .add_row(build_schema_store_row())
        .add_row(build_event_service_row())
        .add_row(build_message_center_row())
        .add_row(build_dispatcher_row())
        .add_row(build_dynamic_stream_row())
        .add_row(build_sink_general_row())
        .add_row(build_sink_transaction_row())
        .add_row(build_sink_mq_row())
        .add_row(build_sink_cloud_storage_row())
        .add_row(build_scheduler_row())
        .add_row(build_tikv_row())
        .add_row(build_active_active_row())
        .add_row(build_redo_row())
        .add_row(build_runtime_row())
        .add_row(build_ddl_row())
        .add_row(build_pulsar_sink_row())
    )
    return spec.build()


def build_dashboard() -> dict[str, object]:
    spec = build_dashboard_spec()
    rendered = render_dashboard(spec)
    validate_dashboard_identity(
        rendered,
        expected_row_titles=[row.title for row in spec.rows],
    )
    return rendered
