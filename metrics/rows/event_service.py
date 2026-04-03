# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    legend_for,
    regex,
)


def build_event_service_row() -> RowSpec:
    row_builder = row("Event Service")

    scan_window_interval = graph(
        "Scan window interval",
        description="The lag between changefeed checkpoint ts and the lac1 ts of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_event_service_scan_window_interval",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        legend="{{instance}}{{namespace}}-{{changefeed}}",
    )

    scan_window_base_ts = graph(
        "Scan window base ts",
        description="",
        unit="none",
    ).add_query(
        expr_max(
            "ticdc_event_service_scan_window_base_ts",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
        legend="{{instance}}{{namespace}}-{{changefeed}}",
        ref="B",
    )

    event_service_scan_duration_heatmap = heatmap(
        "Event Service Scan Duration",
        description="",
        unit="s",
        key="scan_duration_heatmap",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_event_service_scan_duration_bucket",
            by_labels=["le"],
            scope="instance",
        ),
        format="heatmap",
    )

    event_service_scan_duration_graph = (
        graph(
            "Event Service Scan Duration",
            description="",
            unit="s",
            min="0",
            key="scan_duration_graph",
        )
        .add_range_query(
            expr_histogram_avg(
                "ticdc_event_service_scan_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="avg"),
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_event_service_scan_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="p999"),
        )
    )

    event_service_scanned_entry_count = graph(
        "Event Service Scanned Entry Count",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_scanned_count_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    event_service_scanned_transaction_count = graph(
        "Event Service Scanned Transaction Count",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_scanned_txn_count_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    event_service_scanned_entry_bytes_s = (
        graph(
            "Event Service Scanned Entry Bytes / s",
            unit="bytes",
            min="0",
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_event_store_scan_bytes",
                by_labels=["instance"],
                scope="instance",
                selectors=[eq("type", "scanned")],
            ),
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_event_store_scan_bytes",
                by_labels=["instance"],
                scope="instance",
                selectors=[eq("type", "skipped")],
            ),
            legend=legend_for("instance", suffix="skipped"),
        )
    )

    event_service_scanned_transaction_bytes_s = graph(
        "Event Service Scanned Transaction Bytes / s",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_scanned_dml_size_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    event_service_finished_scan_task_count = graph(
        "Event Service Finished Scan Task Count",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_scan_task_count",
            by_labels=["instance"],
            scope="instance",
        ),
        legend=legend_for("instance", prefix="finished-scan-task"),
    )

    event_service_resolved_ts_lag = graph(
        "Event Service Resolved Ts Lag",
        description="",
        unit="s",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_event_service_resolved_ts_lag",
            by_labels=["instance", "type"],
            scope="instance",
        ),
        legend=legend_for("instance", "type", suffix="resolvedts"),
    )

    event_service_pending_scan_task = graph(
        "Event Service Pending Scan Task",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_pending_scan_task_count",
            by_labels=["namespace", "instance"],
            scope="instance",
            selectors=[regex("namespace", "$namespace")],
        ),
        legend="pending-task-{{instance}}",
    )

    event_service_dispatcher_status = graph(
        "Event Service Dispatcher Status",
        min="0",
    ).add_auto_query(
        expr_simple(
            "ticdc_event_service_dispatcher_status_count",
            scope="instance",
            selectors=[regex("namespace", "$namespace")],
        ),
        legend="{{instance}}-dispatcherStatus-{{status}}",
        ref="B",
    )

    event_service_available_memory = graph(
        "Event Service Available Memory",
        description="Event Service Available Memory",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum(
            "ticdc_event_service_available_memory_quota",
            by_labels=["instance", "changefeed"],
            scope="changefeed",
        ),
    )

    event_service_channel_size = graph(
        "Event Service Channel Size",
        description="",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_channel_size",
            by_labels=["instance", "type"],
            scope="instance",
            selectors=[regex("namespace", "$namespace")],
        ),
    )

    scanned_entry_count_s = graph(
        "Scanned Entry Count / s",
        description="The number of entries scanned by event store.",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_scanned_count_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    reset_dispatcher_s = graph(
        "Reset Dispatcher / s",
        description="The number of event dispatcher reset operations performed",
        unit="ops",
        min="0",
    ).add_auto_range_query(
        expr_sum_rate(
            "ticdc_event_service_reset_dispatcher_count",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    skip_scan_count_s = graph(
        "Skip Scan Count / s",
        description="The rate of skip scan count in eventService",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_skip_scan_count_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    intterrupt_scan_count_s = graph(
        "Intterrupt Scan Count / s",
        description="The rate of intterupt scan count in eventService",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_service_interrupt_scan_count_count_sum",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    decode_dmlevent_duration = (
        graph(
            "Decode DMLEvent Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            expr_histogram_avg(
                "ticdc_event_decode_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="avg"),
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_event_decode_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="p999"),
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.95,
                "ticdc_event_decode_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="p95"),
        )
    )

    eventservice_output_different_dml_event_types_s = graph(
        "EventService Output Different DML Event Types / s",
        description="The total number of different DML event types that EventService outputs",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum_rate(
            "ticdc_event_service_send_dml_type_count",
            by_labels=["instance", "dml_type"],
            scope="instance",
        ),
    )

    row_builder.add_panels(
        scan_window_interval,
        scan_window_base_ts,
    )

    row_builder.add_panels(
        event_service_scan_duration_heatmap,
        event_service_scan_duration_graph,
    )

    row_builder.add_panels(
        event_service_scanned_entry_count,
        event_service_scanned_transaction_count,
    )

    row_builder.add_panels(
        event_service_scanned_entry_bytes_s,
        event_service_scanned_transaction_bytes_s,
    )

    row_builder.add_panels(
        event_service_finished_scan_task_count,
        event_service_resolved_ts_lag,
    )

    row_builder.add_panels(
        event_service_pending_scan_task,
        event_service_dispatcher_status,
    )

    row_builder.add_panels(
        event_service_available_memory,
        event_service_channel_size,
    )

    row_builder.add_panels(
        scanned_entry_count_s,
        reset_dispatcher_s,
    )

    row_builder.add_panels(
        skip_scan_count_s,
        intterrupt_scan_count_s,
    )

    row_builder.add_panels(
        decode_dmlevent_duration,
        eventservice_output_different_dml_event_types_s,
    )

    return row_builder.build()
