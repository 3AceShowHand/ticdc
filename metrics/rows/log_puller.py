# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_log_puller_row() -> RowSpec:
    row_builder = row("Log Puller")

    input_events_s = graph(
        "Input Events / s",
        description="The number of KV client dispatched event per second",
        min="0",
    ).add_query(
        'sum(rate(ticdc_kvclient_pull_event_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
        legend="{{instance}}-{{type}}",
        ref="B",
    )

    unresolved_region_request_count = graph(
        "Unresolved Region Request Count ",
        description="To prevent excessive accumulation of region request tasks on the TiKV side, CDC rate-limits how many requests it initiates.",
        min="0",
    ).add_auto_query(
        'ticdc_subscription_client_requested_region_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}-count",
    )

    region_request_finish_scan_duration = (
        graph(
            "Region Request Finish Scan Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            'histogram_quantile(0.99, sum(rate(ticdc_subscription_client_region_request_finish_scan_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p99",
            format="heatmap",
        )
        .add_auto_query(
            'sum(rate(ticdc_subscription_client_region_request_finish_scan_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_subscription_client_region_request_finish_scan_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
    )

    subscribed_region_count = graph(
        "Subscribed Region Count",
        description="To prevent excessive accumulation of region request tasks on the TiKV side, CDC rate-limits how many requests it initiates.",
        min="0",
    ).add_auto_query(
        'ticdc_subscription_client_subscribed_region_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}-count",
    )

    memory_quota = graph(
        "Memory Quota",
        description="Log puller memory quota",
        unit="bytes",
        min="0",
    ).add_auto_query(
        'sum(ticdc_dynamic_stream_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"log-puller"}) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    resolved_ts_batch_size_regions = heatmap(
        "Resolved Ts Batch Size (Regions)",
        description="The size of batch resolved regions count",
        unit="none",
    ).add_range_query(
        'sum(rate(ticdc_kvclient_batch_resolved_event_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
        format="heatmap",
    )

    region_event_handle_duration = (
        graph(
            "Region Event Handle Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            'histogram_quantile(0.99, sum(rate(ticdc_subscription_client_region_event_handle_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p99",
            format="heatmap",
        )
        .add_auto_query(
            'sum(rate(ticdc_subscription_client_region_event_handle_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_subscription_client_region_event_handle_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
    )

    region_event_consume_callback_duration = (
        graph(
            "Region Event Consume Callback Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            'histogram_quantile(0.99, sum(rate(ticdc_subscription_client_consume_kv_events_callback_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance, type))',
            legend="{{instance}}-{{type}}-p99",
            format="heatmap",
        )
        .add_auto_query(
            'sum(rate(ticdc_subscription_client_consume_kv_events_callback_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type) / sum(rate(ticdc_subscription_client_consume_kv_events_callback_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
            legend="{{instance}}-{{type}}-avg",
        )
    )

    dropped_resolve_lock_tasks_s = graph(
        "Dropped Resolve Lock Tasks / s",
        description="Dropped resolve lock tasks when resolveLockTaskCh is full.",
        unit="ops",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_subscription_client_resolve_lock_task_drop_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    row_builder.add_panels(
        input_events_s,
        unresolved_region_request_count,
    )

    row_builder.add_panels(
        region_request_finish_scan_duration,
        subscribed_region_count,
    )

    row_builder.add_panels(
        memory_quota,
        resolved_ts_batch_size_regions,
    )

    row_builder.add_panels(
        region_event_handle_duration,
        region_event_consume_callback_duration,
    )

    row_builder.add_half_panel(dropped_resolve_lock_tasks_s)

    return row_builder.build()
