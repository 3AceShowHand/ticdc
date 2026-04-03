# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_avg,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    neq,
    regex,
)


def build_tikv_row() -> RowSpec:
    row_builder = row("TiKV")
    total_scan_tasks = expr_sum(
        "tikv_cdc_scan_tasks",
        by_labels=["instance"],
        scope="tikv_instance",
        selectors=[eq("type", "total")],
    )
    finished_scan_tasks = expr_sum(
        "tikv_cdc_scan_tasks",
        by_labels=["instance"],
        scope="tikv_instance",
        selectors=[regex("type", "abort|finish")],
    )
    current_tso = expr_max("pd_cluster_tso", scope="cluster")
    min_resolved_ts_seconds = expr_avg(
        f"{expr_simple('tikv_cdc_min_resolved_ts', scope='tikv_instance')} / 1000",
        by_labels=["instance"],
        scope="none",
    )
    old_value_cache_access = expr_sum_rate(
        "tikv_cdc_old_value_cache_access",
        by_labels=["instance"],
        scope="tikv_instance",
    )
    old_value_cache_miss = expr_sum_rate(
        "tikv_cdc_old_value_cache_miss",
        by_labels=["instance"],
        scope="tikv_instance",
    )
    old_value_cache_miss_none = expr_sum_rate(
        "tikv_cdc_old_value_cache_miss_none",
        by_labels=["instance"],
        scope="tikv_instance",
    )
    old_value_cache_bytes = expr_sum(
        "tikv_cdc_old_value_cache_bytes",
        by_labels=["instance"],
        scope="tikv_instance",
    )
    old_value_cache_length = expr_sum(
        "tikv_cdc_old_value_cache_length",
        by_labels=["instance"],
        scope="tikv_instance",
    )

    grpc_message_count = graph(
        "gRPC Message Count", description="The count of different kinds of gRPC message", unit="ops"
    ).add_query(
        expr_sum_rate(
            "tikv_grpc_msg_duration_seconds_count",
            by_labels=["type"],
            scope="tikv_instance",
            selectors=[neq("type", "kv_gc")],
        )
    )

    cdc_network_traffic = graph(
        "CDC Network Traffic",
        description="Outbound network traffic of TiKV CDC component",
        unit="Bps",
    ).add_query(
        expr_sum_rate(
            "tikv_cdc_grpc_message_sent_bytes",
            by_labels=["instance", "type"],
            scope="tikv_instance",
            window="30s",
        )
    )

    row_builder.add_panels(grpc_message_count, cdc_network_traffic)

    cdc_cpu = (
        graph(
            "CDC CPU",
            description="CPU usage of TiKV CDC component",
            unit="percentunit",
        )
        .add_query(
            expr_sum_rate(
                "tikv_thread_cpu_seconds_total",
                by_labels=["instance"],
                scope="tikv_instance",
                selectors=[regex("name", "cdc_.*|cdc")],
            ),
            legend_format="{{instance}}-endpoint",
        )
        .add_query(
            expr_sum_rate(
                "tikv_thread_cpu_seconds_total",
                by_labels=["instance"],
                scope="tikv_instance",
                selectors=[regex("name", "cdcwkr.*")],
            ),
            legend_format="{{instance}}-workers",
        )
        .add_query(
            expr_sum_rate(
                "tikv_thread_cpu_seconds_total",
                by_labels=["instance"],
                scope="tikv_instance",
                selectors=[regex("name", "tso")],
            ),
            legend_format="{{instance}}-tso",
        )
    )

    cdc_memory_quota = (
        graph(
            "CDC Memory Quota",
            description="The TiKV-CDC memory quota usage per TiKV instance",
            unit="bytes",
            min="0",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_sink_memory_capacity",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-quota-capacity",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_sink_memory_bytes",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-quota",
        )
    )

    row_builder.add_panels(cdc_cpu, cdc_memory_quota)

    captured_region_count = (
        graph(
            "Captured Region Count",
            description="The memory usage per TiKV instance",
            unit="none",
            min="0",
        )
        .add_query(
            expr_avg(
                "tikv_cdc_captured_region_total",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="tikv-{{instance}}-total",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_region_resolve_status",
                by_labels=["instance", "status"],
                scope="tikv_instance",
            ),
            legend_format="tikv-{{instance}}-{{status}}",
        )
    )

    initial_scan_tasks_status = (
        graph(
            "Initial Scan Tasks Status",
            description="The number of incremental scan task in different status.",
            unit="none",
            min="0",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_scan_tasks",
                by_labels=["type", "instance"],
                scope="tikv_instance",
                selectors=[eq("type", "ongoing")],
            ),
            legend_format="{{instance}}-{{type}}",
        )
        .add_query(
            f"{total_scan_tasks} - {finished_scan_tasks}",
            legend_format="{{instance}}-pending",
        )
    )

    row_builder.add_panels(captured_region_count, initial_scan_tasks_status)

    incremental_scan_duration_percentile = (
        graph("Incremental Scan Duration Percentile", description="", unit="s", min="0")
        .add_query(
            expr_histogram_quantile(
                0.9999,
                "tikv_cdc_scan_duration_seconds",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-p9999",
        )
        .add_auto_query(
            expr_histogram_avg(
                "tikv_cdc_scan_duration_seconds", by_labels=["instance"], scope="tikv_instance"
            ),
            legend_format="{{instance}}-avg",
        )
    )

    initial_scan_duration = heatmap(
        "Initial Scan Duration", description="The time consumed to CDC incremental scan", unit="s"
    ).add_range_query(
        expr_sum_rate(
            "tikv_cdc_scan_duration_seconds_bucket", by_labels=["le"], scope="tikv_instance"
        )
    )

    row_builder.add_panels(
        incremental_scan_duration_percentile,
        initial_scan_duration,
    )

    incremental_scan_sink_duration_percentile = (
        graph(
            "Incremental Scan Sink Duration Percentile",
            description="The time cost on sink incremental scan data",
            unit="s",
            min="0",
        )
        .add_query(
            expr_histogram_quantile(
                0.9999,
                "tikv_cdc_scan_sink_duration_seconds",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-p9999",
        )
        .add_auto_query(
            expr_histogram_avg(
                "tikv_cdc_scan_sink_duration_seconds", by_labels=["instance"], scope="tikv_instance"
            ),
            legend_format="{{instance}}-avg",
        )
    )

    cdc_total_scan_bytes = graph(
        "CDC Total Scan Bytes",
        description="The total bytes of TiKV CDC incremental scan",
        unit="bytes",
    ).add_query(
        expr_sum(
            "tikv_cdc_scan_bytes_total",
            by_labels=["instance"],
            scope="tikv_instance",
        ),
        legend_format="tikv-{{instance}}",
    )

    row_builder.add_panels(
        incremental_scan_sink_duration_percentile,
        cdc_total_scan_bytes,
    )

    incremental_scan_speed = graph(
        "Incremental Scan Speed",
        description="The speed of TiKV CDC incremental scan",
        unit="binBps",
        min="0",
    ).add_query(
        expr_sum_rate(
            "tikv_cdc_scan_bytes_total",
            by_labels=["instance"],
            scope="tikv_instance",
            window="30s",
        ),
        legend_format="tikv-{{instance}}",
    )

    incremental_scan_disk_speed = graph(
        "Incremental Scan Disk Speed",
        description="The speed of TiKV CDC incremental scan read from the disk",
        unit="binBps",
        min="0",
    ).add_query(
        expr_sum_rate(
            "tikv_cdc_scan_disk_read_bytes_total",
            by_labels=["instance"],
            scope="tikv_instance",
            window="30s",
        ),
        legend_format="tikv-{{instance}}",
    )

    row_builder.add_panels(
        incremental_scan_speed,
        incremental_scan_disk_speed,
    )

    min_resolved_ts = (
        graph("Min Resolved Ts", description="The min resolved ts of each TiKV", unit="s", min="0")
        .add_range_query(
            f"scalar({current_tso}) / 1000 - {min_resolved_ts_seconds} > 0",
            legend_format="{{instance}}-min-resolved-lag",
        )
        .add_query(
            current_tso,
            legend_format="current-ts",
        )
        .add_query(
            expr_avg(
                "tikv_cdc_min_resolved_ts",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-min-resolved-ts",
        )
    )

    resolved_ts_lag_duration_percentile = graph(
        "Resolved Ts Lag Duration Percentile",
        description="",
        unit="s",
    ).add_query(
        expr_histogram_quantile(
            0.99999,
            "tikv_cdc_resolved_ts_gap_seconds",
            by_labels=["instance"],
            scope="tikv_instance",
        ),
        legend_format="{{instance}}-p9999",
    )

    row_builder.add_panels(
        min_resolved_ts,
        resolved_ts_lag_duration_percentile,
    )

    old_value_cache_hit = (
        graph(
            "Old Value Cache Hit",
            description="",
            unit="percentunit",
            min="0",
        )
        .add_query(
            f"({old_value_cache_access} - {old_value_cache_miss}) / {old_value_cache_access}",
            legend_format="hit-rate-{{instance}}",
        )
        .add_query(
            f"-{old_value_cache_miss}",
            legend_format="miss-{{instance}}",
            ref="C",
        )
        .add_query(
            f"-{old_value_cache_miss_none}",
            legend_format="miss-none-{{instance}}",
            ref="D",
        )
    )

    min_resolved_region = graph(
        "Min Resolved Region",
        description="The ID of the min resolved region of each TiKV",
        unit="none",
        min="0",
    ).add_query(
        expr_avg(
            "tikv_cdc_min_resolved_ts_region",
            by_labels=["instance"],
            scope="tikv_instance",
        ),
        legend_format="{{instance}}-min-resolved-region",
    )

    row_builder.add_panels(old_value_cache_hit, min_resolved_region)

    old_value_seek_duration_heatmap = heatmap(
        "Old Value Seek Duration",
        key="old_value_seek_duration_heatmap",
        description="The time consumed to get an old value (both from cache and from disk)",
        unit="s",
    ).add_range_query(
        expr_sum_rate("tikv_cdc_old_value_duration_bucket", by_labels=["le"], scope="tikv_instance")
    )

    old_value_cache_size = (
        graph(
            "Old Value Cache Size",
            description="The total number of cache entries in the old value cache.",
            unit="bytes",
            min="0",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_old_value_cache_length", by_labels=["instance"], scope="tikv_instance"
            ),
            legend_format="{{instance}}-len",
        )
        .add_query(
            f"{old_value_cache_bytes} / {old_value_cache_length}",
            legend_format="{{instance}}-avg entry bytes",
        )
        .add_query(
            expr_sum(
                "tikv_cdc_old_value_cache_memory_quota",
                by_labels=["instance"],
                scope="tikv_instance",
            ),
            legend_format="{{instance}}-quota",
        )
        .add_auto_query(
            expr_sum(
                "tikv_cdc_old_value_cache_bytes", by_labels=["instance"], scope="tikv_instance"
            ),
            legend_format="{{instance}}-usage",
        )
    )

    row_builder.add_panels(old_value_seek_duration_heatmap, old_value_cache_size)

    old_value_seek_duration_graph = (
        graph(
            "Old Value Seek Duration",
            key="old_value_seek_duration_graph",
            description="",
            unit="s",
            min="0",
        )
        .add_query(
            expr_histogram_quantile(
                0.999,
                "tikv_cdc_old_value_duration",
                by_labels=["instance", "tag"],
                scope="tikv_instance",
            ),
            legend="{{instance}}-99%-{{tag}}",
        )
        .add_query(
            expr_histogram_avg(
                "tikv_cdc_old_value_duration",
                by_labels=["instance", "tag"],
                scope="tikv_instance",
            ),
            legend="{{instance}}-avg-{{tag}}",
        )
    )

    old_value_seek_operation = graph(
        "Old Value Seek Operation", description="", unit="ops", min="0"
    ).add_query(
        expr_sum_rate(
            "tikv_cdc_old_value_scan_details",
            by_labels=["instance", "cf", "tag"],
            scope="tikv_instance",
        )
    )

    row_builder.add_panels(
        old_value_seek_duration_graph,
        old_value_seek_operation,
    )

    return row_builder.build()
