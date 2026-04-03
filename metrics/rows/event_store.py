# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_event_store_row() -> RowSpec:
    row_builder = row("Event Store")

    resolved_ts_lag = graph(
        "Resolved Ts Lag",
        unit="s",
        min="0",
    ).add_auto_query(
        'ticdc_event_store_resolved_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}",
    )

    register_dispatcher_startts_lag = (
        graph(
            "Register Dispatcher StartTs Lag",
            description="The lag of startTs when registering a dispatcher.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(1.0, sum(rate(ticdc_event_store_register_dispatcher_start_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-max",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_event_store_register_dispatcher_start_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p95",
        )
        .add_auto_query(
            'histogram_quantile(0.8, sum(rate(ticdc_event_store_register_dispatcher_start_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p80",
            hide=True,
        )
    )

    subscriptions_resolved_ts_lag = (
        graph(
            "Subscriptions Resolved Ts Lag",
            description="The Resolved Ts lag of subscriptions for event store.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(1, sum(rate(ticdc_event_store_subscription_resolved_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-max",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_event_store_subscription_resolved_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p95",
        )
        .add_auto_query(
            'histogram_quantile(0.8, sum(rate(ticdc_event_store_subscription_resolved_ts_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p80",
            hide=True,
        )
    )

    subscriptions_data_gc_lag = (
        graph(
            "Subscriptions Data GC Lag",
            description="The data gc lag of subscriptions for event store.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(1, sum(rate(ticdc_event_store_subscription_data_gc_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-max",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_event_store_subscription_data_gc_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p95",
        )
        .add_auto_query(
            'histogram_quantile(0.8, sum(rate(ticdc_event_store_subscription_data_gc_lag_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p80",
            hide=True,
        )
    )

    input_event_count_s = graph(
        "Input Event Count / s",
        description="The number of events received by event store.",
        min="0",
    ).add_query(
        'sum(rate(ticdc_event_store_input_event_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    input_bytes_s = graph(
        "Input Bytes / s",
        description="The number of bytes written by event store.",
        unit="binBps",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_write_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    write_requests_s = graph(
        "Write Requests / s",
        description="The number of write requests received by event store",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_write_requests_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    write_worker_busy_ratio = graph(
        "Write Worker Busy Ratio",
        description="Busy ratio for event store write worker.",
        unit="percent",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_write_worker_io_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, db, worker) / sum(rate(ticdc_event_store_write_worker_total_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, db, worker) * 100',
        legend="{{instance}}-db-{{db}}-worker-{{worker}}",
    )

    compressed_rows_s = graph(
        "Compressed Rows / s",
        description="The number of rows compressed by event store per second.",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_compressed_rows_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    write_duration = (
        graph(
            "Write Duration",
            description="The time of commit batch to sorter",
            unit="s",
        )
        .add_range_query(
            'histogram_quantile(1, sum(rate(ticdc_event_store_write_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-max",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_event_store_write_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p99",
            hide=True,
        )
        .add_auto_query(
            'sum(rate(ticdc_event_store_write_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_event_store_write_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
    )

    write_queue_duration = (
        graph(
            "Write Queue Duration",
            description="Each event's duration staying in write queue",
            unit="s",
            min="0",
        )
        .add_range_query(
            'histogram_quantile(0.99, sum(rate(ticdc_event_store_write_queue_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p99",
            format="heatmap",
        )
        .add_auto_query(
            'sum(rate(ticdc_event_store_write_queue_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_event_store_write_queue_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
    )

    write_prepare_duration = (
        graph(
            "Write Prepare Duration",
            description="",
            unit="s",
            min="0",
        )
        .add_range_query(
            'histogram_quantile(0.99, sum(rate(ticdc_event_store_write_prepare_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p99",
            format="heatmap",
        )
        .add_auto_query(
            'sum(rate(ticdc_event_store_write_prepare_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_event_store_write_prepare_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            legend="{{instance}}-avg",
        )
    )

    write_batch_size = heatmap(
        "Write Batch Size",
        unit="bytes",
    ).add_query(
        'sum(increase(ticdc_event_store_write_batch_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
    )

    write_batch_event_count = heatmap(
        "Write Batch Event Count",
    ).add_query(
        'sum(increase(ticdc_event_store_write_batch_events_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
    )

    data_size_on_disk = graph(
        "Data Size On Disk",
        description="The amount of pending data stored on-disk for event store",
        unit="bytes",
    ).add_auto_query(
        'sum(ticdc_event_store_on_disk_data_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance)',
        legend="{{instance}}",
    )

    data_size_in_memory = graph(
        "Data Size In Memory",
        description="The amount of pending data stored in-memory for event store",
        unit="bytes",
    ).add_auto_query(
        'sum(ticdc_event_store_in_memory_data_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance)',
        legend="{{instance}}",
    )

    scan_requests_s = graph(
        "Scan Requests / s",
        description="The number of scan requests received by event store",
        unit="ops",
        min="0",
    ).add_auto_range_query(
        'sum(rate(ticdc_event_store_scan_requests_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    scan_bytes_s = graph(
        "Scan Bytes / s",
        description="The number of bytes scanned by event store.",
        unit="binBps",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_scan_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    subscription_num = graph(
        "Subscription Num",
        description="The number of subscriptions created by event store.",
        min="0",
        decimals=0,
    ).add_auto_query(
        'ticdc_event_store_subscription_num{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}",
    )

    scan_operation_duration = graph(
        "Scan Operation Duration ",
        description="The time of event store iterator scan operation duration",
        unit="s",
        min="0",
    ).add_range_query(
        'histogram_quantile(0.99, sum(rate(ticdc_event_store_read_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance, type))',
        legend="{{instance}}-{{type}}-p99",
    )

    pebble_block_cache_access_s = graph(
        "pebble block cache access /s",
        description="The number of scan requests received by event store",
        unit="ops",
        min="0",
    ).add_auto_range_query(
        'sum(rate(ticdc_event_store_pebble_block_cache_access_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    pebble_block_cache_hit_ratio = graph(
        "pebble block cache hit ratio",
        description="",
        unit="percent",
        min="0",
    ).add_range_query(
        'sum(rate(ticdc_event_store_pebble_block_cache_access_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", type="hit"}[1m])) by (instance) / sum(rate(ticdc_event_store_pebble_block_cache_access_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", type=~"hit|miss"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    pebble_compaction_duration_seconds = graph(
        "pebble compaction duration seconds",
        description="",
        unit="s",
        min="0",
    ).add_range_query(
        'histogram_quantile(0.99, sum(rate(ticdc_event_store_pebble_compaction_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance, id))',
        legend="{{instance}}-{{type}}-p99",
    )

    pebble_flush_duration_primary = graph(
        "pebble flush duration seconds",
        description="",
        unit="s",
        min="0",
        key="pebble_flush_duration_primary",
    ).add_range_query(
        'histogram_quantile(0.99, sum(rate(ticdc_event_store_pebble_flush_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance, id))',
        legend="{{instance}}-{{type}}-p99",
    )

    pebble_compaction_bytes = (
        graph(
            "pebble compaction bytes",
            description="",
            unit="bytes",
        )
        .add_auto_query(
            'max(ticdc_event_store_pebble_compaction_debt_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance)',
            legend="debt-{{instance}}",
        )
        .add_auto_query(
            'max(ticdc_event_store_pebble_compaction_in_progress{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance)',
            legend="inprogress-{{instance}}",
        )
    )

    pebble_flush_duration_secondary = graph(
        "pebble flush duration seconds",
        description="",
        unit="s",
        min="0",
        key="pebble_flush_duration_secondary",
    ).add_range_query(
        'histogram_quantile(0.99, sum(rate(ticdc_event_store_pebble_flush_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance, id))',
        legend="{{instance}}-{{type}}-p99",
    )

    pebble_write_stall_s = graph(
        "pebble write stall / s",
        description="The number of scan requests received by event store",
        unit="ops",
        min="0",
    ).add_auto_range_query(
        'sum(rate(ticdc_event_store_pebble_write_stall_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, id, reason)',
        legend="{{instance}}",
    )

    pebble_level_files = graph(
        "pebble level files",
        description="The number of subscriptions created by event store.",
        min="0",
        decimals=0,
    ).add_auto_query(
        'max(ticdc_event_store_pebble_level_files{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance, level)',
        legend="{{instance}}-{{level}}",
    )

    row_builder.add_panels(
        resolved_ts_lag,
        register_dispatcher_startts_lag,
    )

    row_builder.add_panels(
        subscriptions_resolved_ts_lag,
        subscriptions_data_gc_lag,
    )

    row_builder.add_panels(
        input_event_count_s,
        input_bytes_s,
    )

    row_builder.add_panels(
        write_requests_s,
        write_worker_busy_ratio,
    )

    row_builder.add_panels(
        compressed_rows_s,
        write_duration,
    )

    row_builder.add_panels(
        write_queue_duration,
        write_prepare_duration,
    )

    row_builder.add_panels(
        write_batch_size,
        write_batch_event_count,
    )

    row_builder.add_panels(
        data_size_on_disk,
        data_size_in_memory,
    )

    row_builder.add_panels(
        scan_requests_s,
        scan_bytes_s,
    )

    row_builder.add_panels(
        subscription_num,
        scan_operation_duration,
    )

    row_builder.add_panels(
        pebble_block_cache_access_s,
        pebble_block_cache_hit_ratio,
    )

    row_builder.add_panels(
        pebble_compaction_duration_seconds,
        pebble_flush_duration_primary,
    )

    row_builder.add_panels(
        pebble_compaction_bytes,
        pebble_flush_duration_secondary,
    )

    row_builder.add_panels(
        pebble_write_stall_s,
        pebble_level_files,
    )

    return row_builder.build()
