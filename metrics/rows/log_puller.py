# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    legend_for,
    regex,
)


def build_log_puller_row() -> RowSpec:
    row_builder = row("Log Puller")

    input_events_s = graph(
        "Input Events / s",
        description="The number of KV client dispatched event per second",
        min="0",
    ).add_query(
        expr_sum_rate(
            "ticdc_kvclient_pull_event_count",
            by_labels=["instance", "type"],
            scope="instance",
        ),
        ref="B",
    )

    unresolved_region_request_count = graph(
        "Unresolved Region Request Count ",
        description="To prevent excessive accumulation of region request tasks on the TiKV side, CDC rate-limits how many requests it initiates.",
        min="0",
    ).add_auto_query(
        expr_simple("ticdc_subscription_client_requested_region_count", scope="instance"),
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
            expr_histogram_quantile(
                0.99,
                "ticdc_subscription_client_region_request_finish_scan_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="p99"),
            format="heatmap",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_subscription_client_region_request_finish_scan_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="avg"),
        )
    )

    subscribed_region_count = graph(
        "Subscribed Region Count",
        description="To prevent excessive accumulation of region request tasks on the TiKV side, CDC rate-limits how many requests it initiates.",
        min="0",
    ).add_auto_query(
        expr_simple("ticdc_subscription_client_subscribed_region_count", scope="instance"),
        legend="{{instance}}-count",
    )

    memory_quota = graph(
        "Memory Quota",
        description="Log puller memory quota",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum(
            "ticdc_dynamic_stream_memory_usage",
            by_labels=["instance", "type"],
            scope="instance",
            selectors=[regex("module", "log-puller")],
        ),
    )

    resolved_ts_batch_size_regions = heatmap(
        "Resolved Ts Batch Size (Regions)",
        description="The size of batch resolved regions count",
        unit="none",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_kvclient_batch_resolved_event_size_bucket",
            by_labels=["le"],
            scope="instance",
        ),
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
            expr_histogram_quantile(
                0.99,
                "ticdc_subscription_client_region_event_handle_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="p99"),
            format="heatmap",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_subscription_client_region_event_handle_duration",
                by_labels=["instance"],
                scope="instance",
            ),
            legend=legend_for("instance", suffix="avg"),
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
            expr_histogram_quantile(
                0.99,
                "ticdc_subscription_client_consume_kv_events_callback_duration",
                by_labels=["instance", "type"],
                scope="instance",
            ),
            legend=legend_for("instance", "type", suffix="p99"),
            format="heatmap",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_subscription_client_consume_kv_events_callback_duration",
                by_labels=["instance", "type"],
                scope="instance",
            ),
            legend=legend_for("instance", "type", suffix="avg"),
        )
    )

    dropped_resolve_lock_tasks_s = graph(
        "Dropped Resolve Lock Tasks / s",
        description="Dropped resolve lock tasks when resolveLockTaskCh is full.",
        unit="ops",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_subscription_client_resolve_lock_task_drop_count",
            by_labels=["instance"],
            scope="instance",
        ),
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
