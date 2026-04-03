# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_event_service_row() -> RowSpec:
    row_builder = row("Event Service")

    scan_window_interval = graph(
        "Scan window interval",
        description="The lag between changefeed checkpoint ts and the lac1 ts of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        'max(ticdc_event_service_scan_window_interval{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance)',
        legend="{{instance}}{{namespace}}-{{changefeed}}",
    )

    scan_window_base_ts = graph(
        "Scan window base ts",
        description="",
        unit="none",
    ).add_query(
        'max(ticdc_event_service_scan_window_base_ts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance)',
        legend="{{instance}}{{namespace}}-{{changefeed}}",
        ref="B",
    )

    event_service_scan_duration_heatmap = heatmap(
        "Event Service Scan Duration",
        description="",
        unit="s",
        key="scan_duration_heatmap",
    ).add_range_query(
        'sum(rate(ticdc_event_service_scan_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
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
            'sum(rate(ticdc_event_service_scan_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_event_service_scan_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_event_service_scan_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p999",
        )
    )

    event_service_scanned_entry_count = graph(
        "Event Service Scanned Entry Count",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_scanned_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    event_service_scanned_transaction_count = graph(
        "Event Service Scanned Transaction Count",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_scanned_txn_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    event_service_scanned_entry_bytes_s = (
        graph(
            "Event Service Scanned Entry Bytes / s",
            unit="bytes",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_event_store_scan_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", type="scanned"}[1m])) by (instance)',
            legend="{{instance}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_event_store_scan_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", type="skipped"}[1m])) by (instance)',
            legend="{{instance}}-skipped",
        )
    )

    event_service_scanned_transaction_bytes_s = graph(
        "Event Service Scanned Transaction Bytes / s",
        unit="bytes",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_scanned_dml_size_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    event_service_finished_scan_task_count = graph(
        "Event Service Finished Scan Task Count",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_scan_task_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="finished-scan-task-{{instance}}",
    )

    event_service_resolved_ts_lag = graph(
        "Event Service Resolved Ts Lag",
        description="",
        unit="s",
        min="0",
    ).add_query(
        'sum(ticdc_event_service_resolved_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance, type)',
        legend="{{instance}}-{{type}}-resolvedts",
    )

    event_service_pending_scan_task = graph(
        "Event Service Pending Scan Task",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_pending_scan_task_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", instance=~"$ticdc_instance"}[1m])) by (namespace, instance)',
        legend="pending-task-{{instance}}",
    )

    event_service_dispatcher_status = graph(
        "Event Service Dispatcher Status",
        min="0",
    ).add_auto_query(
        'ticdc_event_service_dispatcher_status_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", instance=~"$ticdc_instance"}',
        legend="{{instance}}-dispatcherStatus-{{status}}",
        ref="B",
    )

    event_service_available_memory = graph(
        "Event Service Available Memory",
        description="Event Service Available Memory",
        unit="bytes",
        min="0",
    ).add_auto_query(
        'sum(ticdc_event_service_available_memory_quota{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (instance, changefeed)',
        legend="{{instance}}-{{changefeed}}",
    )

    event_service_channel_size = graph(
        "Event Service Channel Size",
        description="",
        min="0",
        decimals=0,
    ).add_auto_query(
        'sum(rate(ticdc_event_service_channel_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    scanned_entry_count_s = graph(
        "Scanned Entry Count / s",
        description="The number of entries scanned by event store.",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_scanned_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    reset_dispatcher_s = graph(
        "Reset Dispatcher / s",
        description="The number of event dispatcher reset operations performed",
        unit="ops",
        min="0",
    ).add_auto_range_query(
        'sum(rate(ticdc_event_service_reset_dispatcher_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    skip_scan_count_s = graph(
        "Skip Scan Count / s",
        description="The rate of skip scan count in eventService",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_skip_scan_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    intterrupt_scan_count_s = graph(
        "Intterrupt Scan Count / s",
        description="The rate of intterupt scan count in eventService",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_service_interrupt_scan_count_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    decode_dmlevent_duration = (
        graph(
            "Decode DMLEvent Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            'sum(rate(ticdc_event_decode_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_event_decode_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_event_decode_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p999",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_event_decode_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p95",
        )
    )

    eventservice_output_different_dml_event_types_s = graph(
        "EventService Output Different DML Event Types / s",
        description="The total number of different DML event types that EventService outputs",
        min="0",
        decimals=0,
    ).add_query(
        'sum(rate(ticdc_event_service_send_dml_type_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, dml_type)',
        legend="{{instance}}-{{dml_type}}",
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
